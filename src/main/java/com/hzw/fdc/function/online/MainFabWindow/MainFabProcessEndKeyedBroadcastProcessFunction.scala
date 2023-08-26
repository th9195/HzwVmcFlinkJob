package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.engine.api.{ApiControlWindow, EngineFunction, IControlWindow}
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{ContextDataType, IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}

/**
 * @author gdj
 * @create 2021-04-21-18:34
 *
 */
class MainFabProcessEndKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,
  (String, JsonNode, JsonNode), JsonNode, fdcWindowData] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabProcessEndKeyedBroadcastProcessFunction])


  lazy val dataMissingRatioOutput = new OutputTag[FdcData[IndicatorResult]]("dataMissingRatio")
  lazy val debugOutput = new OutputTag[String]("debugTest")

  // 侧道输出日志信息
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  val job_fun_DebugCode:String = "023002"
  val jobName:String = "MainFabProcessEndWindowService"
  val optionName : String = "MainFabProcessEndKeyedBroadcastProcessFunction"

  //tool chamber recipe product stage --> context---> ContextConfig
  var contextMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, ContextConfigData]]()

  //contextId|sensorPartitionID--->controlWindowId--->IControlWindow
//  private val containerWindowMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, IControlWindow]]()

  //contextId|sensorPartitionID--->controlWindowId--->WindowConfigData
  private val processEndWindowConfigMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, WindowConfigData]]()

  //contextId->IndicatorConfig
  var indicatorConfigByDataMissing = new concurrent.TrieMap[Long, IndicatorConfig]()

  //controlWindowId->IndicatorConfig
  var indicatorConfigByCycleCount = new concurrent.TrieMap[Long, IndicatorConfig]()

  // 判断dataMissing和cycleCount indicator是否已经产生
  // {"tool|chamber": {"runId": [indicatorType|contextKey]}}
  var indicatorRecordMap = new concurrent.TrieMap[String, concurrent.TrieMap[String,  mutable.Set[String]]]()

  private var processEndEventStartState:ValueState[RunEventData] = _

  //[stepId, timestamp, [1: sensorAlias, 2: sensorValue, 3: unit] ]
  private var rawDataState: MapState[Long, windowRawData] = _

  //=========用于debug调试================
  // controlWindowId -> function
  var debugConfig = new concurrent.TrieMap[Long, mutable.Set[String]]()

  /** The state that is maintained by this process function */
  private var runIdState: ValueState[taskIdTimestamp] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 初始化ContextConfig
    //val contextConfigList = InitFlinkFullConfigHbase.ContextConfigList
    val contextConfigList = readHbaseAllConfig[List[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE, ContextDataType)
    contextConfigList.foreach(addContextMapToContextMap)


    //val indicatorConfigList = InitFlinkFullConfigHbase.IndicatorConfigList
    val indicatorConfigList = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

    // 初始化 dataMissingRatio 和 cycleCountIndicator
    for (one <- indicatorConfigList) {
      if (one.datas.algoClass == MainFabConstants.dataMissingRatio) {
        addDataMissingIndicatorToIndicatorConfigMap(one)
      } else if (one.datas.algoClass == MainFabConstants.cycleCountIndicator) {
        addCycleCountIndicatorToIndicatorConfigMap(one)
      } else {

      }
    }

//    // 26小时过期
//    val ttlConfig:StateTtlConfig = StateTtlConfig
//      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
//      .useProcessingTime()
//      .updateTtlOnCreateAndWrite()
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//      // compact 过程中清理过期的状态数据
//      .cleanupInRocksdbCompactFilter(5000)
//      .build()

//    // 26小时过期
//    val ttlConfig:StateTtlConfig = StateTtlConfig
//      .newBuilder(Time.seconds(1*60*60*ProjectConfig.RUN_MAX_LENGTH))
//      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//      .build()

    val processEndEventStartStateDescription = new
        ValueStateDescriptor[RunEventData]("processEndEventStartState", TypeInformation.of(classOf[RunEventData]))

    // 设置过期时间
//    processEndEventStartStateDescription.enableTimeToLive(ttlConfig)
    processEndEventStartState = getRuntimeContext.getState(processEndEventStartStateDescription)

    val rawDataStateDescription: MapStateDescriptor[Long, windowRawData] = new
        MapStateDescriptor[Long, windowRawData]("ProcessEndRawTraceState", TypeInformation.of(classOf[Long]),
          TypeInformation.of(classOf[windowRawData]))

    // 设置过期时间
//    rawDataStateDescription.enableTimeToLive(ttlConfig)
    rawDataState = getRuntimeContext.getMapState(rawDataStateDescription)

    val runIdStateDescription = new ValueStateDescriptor[taskIdTimestamp]("processIndicatorOutValueState",
      TypeInformation.of(classOf[taskIdTimestamp]))
    // 设置过期时间
//    runIdStateDescription.enableTimeToLive(ttlConfig)
    runIdState = getRuntimeContext.getState(runIdStateDescription)

  }

//  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode),
//    JsonNode, fdcWindowData]#OnTimerContext, out: Collector[fdcWindowData]): Unit = {
//    try {
//      runIdState.value() match {
//        case taskIdTimestamp(cacheKey, lastModified)
//          if (System.currentTimeMillis() >= lastModified + 60000L) =>
////          logger.warn(ErrorCode("002009d011D", System.currentTimeMillis(),
////            Map("msg" -> s"====processEnd ${cacheKey} math is time out 60s"), "processEnd 切窗口计算超时").toString)
////          // 清除状态
//          rawDataState.clear()
//          processEndEventStartState.clear()
////          containerWindowMap.clear()
//          processEndWindowConfigMap.clear()
//          runIdState.clear()
//          close()
//        case _ =>
//      }
//    }catch {
//      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
//        Map("onTimer" -> "结果匹配失败!!"), exception.toString).toString)
//    }
//  }

  /**
   *
   * eventStart/eventEnd
   *    $traceId|$toolName|$chamberName|$calcTrigger|$sensorPartitionID , in1 , List[WindowConfigData]
   * rawData
   *    $traceId|$toolName|$chamberName|$calcTrigger|$sensorPartitionID , MainFabRawDataTuple(保留了配置中的sensor,并且)
   *    case class MainFabRawDataTuple(dataType: String, timestamp: Long,stepId: Long,data: List[(String, Double, String)]) // 1: sensorAlias, 2: sensorValue, 3: unit
   *
   * 数据流处理
   */
  override def processElement(value: (String, JsonNode, JsonNode), ctx: KeyedBroadcastProcessFunction[String,
    (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext, out: Collector[fdcWindowData]): Unit = {

    try {
      val ptData = value._2
      val recordKey = value._1
      val sensorPartitionID = recordKey.split("\\|").last
      val dataType = ptData.get(MainFabConstants.dataType).asText()

      if(dataType == MainFabConstants.rawData){
        val RawData = toBean[MainFabRawDataTuple](ptData)
        rawDataState.put(RawData.timestamp, windowRawData(RawData.stepId, RawData.timestamp, RawData.data))


      }else if(dataType == MainFabConstants.eventStart){

        val RunEventStart = toBean[RunEventData](ptData)
        processEndEventStartState.update(RunEventStart)

      } else if (dataType == MainFabConstants.eventEnd) {

        try {
          val windowConfig = toBean[ConfigData[List[WindowConfigData]]](value._3)
          addWindowConfigToWindowConfigMap(windowConfig, sensorPartitionID,ctx)
        } catch {
          case e: Exception => logger.error(s"windowConfig json error  data:$value._3  " +
            s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

//        try {
//          // write the state back
//          runIdState.update(taskIdTimestamp(recordKey, System.currentTimeMillis()))
//
//          // schedule the next timer 60 seconds from the current event time
//          ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 60000L)
//        }catch {
//          case ex: Exception => logger.error("processEnd registerProcessingTimeTimer error: " + ex.toString)
//        }

        try {
          val runEventStart = processEndEventStartState.value()
          val runEventEnd = toBean[RunEventData](ptData)

          //有没有event start
          if (runEventStart != null) {
            //判断是否超过20小时
            val gap = runEventEnd.runEndTime - runEventStart.runStartTime
            if (gap >= 60 * 60 * ProjectConfig.RUN_MAX_LENGTH * 1000) {
              processEndEventStartState.clear()
              rawDataState.clear()
              val traceId = ptData.get(MainFabConstants.traceId).asText()

//              logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(), Map("runTimeTotal" -> gap.toString,
//                "RunEventStart" -> runEventStart, "traceId" -> traceId), "run超过20小时，清除数据").toJson)

              ctx.output(mainFabLogInfoOutput,
                generateMainFabLogInfo("0001B",
                  "processElement",
                  "超时的Run ,清除数据",
                  Map[String,Any]("runTimeTotal" -> gap.toString,
                    "RunEventStart" -> runEventStart,
                    "traceId" -> traceId,
                    "ptData" -> ptData.toString,
                    "windowConfig" -> value._3.toString
                  )))

            }else if(runEventEnd.errorCode.getOrElse(0) == 0 || runEventEnd.errorCode.isEmpty){

              processEndWindowFunction((runEventStart, runEventEnd), sensorPartitionID, ctx, out)
            }else{
              try {
                // 特殊的errorCode 不切窗口
                val specialErrorCode: String = runEventEnd.errorCode.get.toString
                val specialErrorCodeList: List[String] = ProjectConfig.SPECIAL_ERROR_CODE.split(",").toList
                if(!specialErrorCodeList.contains(specialErrorCode)){
                  processEndWindowFunction((runEventStart, runEventEnd), sensorPartitionID, ctx, out)
                }
              }catch {
                case exception: Exception =>logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(),
                  Map("func" -> "SPECIAL_ERROR_CODE有异常","RunEventEnd" -> runEventEnd),
                  ExceptionInfo.getExceptionInfo(exception)).toJson)
              }
              logger.warn(s"167,数据异常,errorCode : ${runEventEnd.errorCode}")
            }
          } else {
            logger.warn(ErrorCode("002001b001C", System.currentTimeMillis(), Map("RunEventEnd" -> runEventEnd,
              "MainFabRawDataListSize" -> rawDataState.keys().size), "没有RunEventStart").toJson)
          }

        } catch {
          case exception: Exception =>logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(),
            Map("MainFabRawDataListSize" -> "","RunEventEnd" -> "RunEventEnd"), ExceptionInfo.getExceptionInfo(exception)).toJson)
        }finally {
          // 清除状态
          close()
        }
      }
    }catch {
      case exception: Exception =>logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(),
        Map("MainFabRawDataListSize" -> "","RunEventEnd" -> ""), exception.toString).toJson)
    }
  }

  /**
   * 广播方法，加载实时增量配置
   *
   */
  override def processBroadcastElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String,
    (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#Context, out: Collector[fdcWindowData]): Unit = {
    try {
      val dataType = value.get(MainFabConstants.dataType).asText()

      if (dataType == MainFabConstants.context) {
        try {
          val contextConfig = toBean[ConfigData[List[ContextConfigData]]](value)
          addContextMapToContextMap(contextConfig)

        } catch {
          case e: Exception => logger.warn(s"contextConfig json error data:$value  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      } else if (dataType == MainFabConstants.indicatorConfig ){
        val indicatorConfig = toBean[ConfigData[IndicatorConfig]](value)
        if(indicatorConfig.datas.algoClass == MainFabConstants.dataMissingRatio) addDataMissingIndicatorToIndicatorConfigMap(indicatorConfig)
        if(indicatorConfig.datas.algoClass == MainFabConstants.cycleCountIndicator) addCycleCountIndicatorToIndicatorConfigMap(indicatorConfig)
      }else if (dataType == "debugTest") {
        // 用于window切窗口debug调试
        try {
          val debugScala = toBean[DebugScala](value)
          logger.warn(s"debugScala: $debugScala")
          val function = debugScala.function
          if (function == "setDebugStateForWindow" || function == "getWindowSnapshotInfo") {
            val args = debugScala.args
            if (args.keySet.contains("controlWindowId")) {
              val controlWindowId = args("controlWindowId").toLong
              if (debugScala.status) {
                if (debugConfig.contains(controlWindowId)) {
                  val functionSet = debugConfig(controlWindowId)
                  functionSet.add(function)
                  debugConfig.put(controlWindowId, functionSet)
                } else {
                  debugConfig.put(controlWindowId, mutable.Set(function))
                }
                logger.warn(s"debugConfig: $debugConfig")
              } else {
                debugConfig.remove(controlWindowId)
              }
            }
          }
        } catch {
          case e: Exception => logger.warn(s"debugScala json error data:$value  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }
      }
    }catch {
      case e: Exception => logger.warn(s"windowJob processEndKeyBroadcastElement error data:$value  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
}

  override def close(): Unit = {
//    containerWindowMap.clear()
    processEndWindowConfigMap.clear()
    rawDataState.clear()
    processEndEventStartState.clear()

    runIdState.clear()
    super.close()
//    rawDataState.clear()
//    processEndEventStartState.clear()
  }

  /**
   *  eventEnd到来之后, 开始处理processEnd窗口
   */
  def processEndWindowFunction(value: (RunEventData, RunEventData), sensorPartitionID: String,ctx: KeyedBroadcastProcessFunction[String,
    (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext, out: Collector[fdcWindowData]): Unit = {
    try {
      val runStartEvent = value._1
      val runEndEvent = value._2
      val runId = s"${runEndEvent.toolName}--${runEndEvent.chamberName}--${runStartEvent.runStartTime}"

      val contextKey = s"${runEndEvent.toolName}|${runEndEvent.chamberName}|${runEndEvent.recipeName}"
      var MatchStatus:Boolean = false
      val contextKeySet: mutable.Set[String] = mutable.Set()

      /**
       *  By toolName chamberName recipeName product stage
       */
      val productStageList = runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") + "|" + x.get.stage.getOrElse("") else "|").distinct
      for (productStage <- productStageList) {

        val productStageKey = s"$contextKey|$productStage"

        // 如果返回的是true, 代表有开始计算了
        if (this.contextMap.contains(productStageKey)) {
          contextKeySet.add(productStageKey)
          MatchStatus = true

          if(calculateWindows(contextKey = productStageKey,
            runStartEvent = runStartEvent,
            runEndEvent = runEndEvent,
            runId = runId,
            sensorPartitionID = sensorPartitionID,
            ctx = ctx,
            out = out)){
          }
        }
      }

      /**
       *  By toolName chamberName recipeName   stage
       */
      val stageList = runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "").distinct
      for (stage <- stageList) {
        val stageKey = s"$contextKey||$stage"

        if (this.contextMap.contains(stageKey) && !contextKeySet.contains(stageKey)) {
          contextKeySet.contains(stageKey)
          MatchStatus = true

          if(calculateWindows(contextKey = stageKey,
            runStartEvent = runStartEvent,
            runEndEvent = runEndEvent,
            runId = runId,
            sensorPartitionID = sensorPartitionID,
            ctx = ctx,
            out = out)){
          }
        }
      }

      /**
       *  By toolName chamberName recipeName product
       */
      val productList = runEndEvent.lotMESInfo.map(x => {
        if (x.nonEmpty) {
          val product = x.get.product
          if(product.nonEmpty){
            product.get
          }else{
            ""
          }
        } else ""
      }).distinct

      // By toolName chamberName recipeName product
      for (product <- productList) {
        val productKey = s"$contextKey|${product}|"

        if (this.contextMap.contains(productKey) && !contextKeySet.contains(productKey)) {
          contextKeySet.add(productKey)
          MatchStatus = true

          if (calculateWindows(contextKey = productKey,
            runStartEvent = runStartEvent,
            runEndEvent = runEndEvent,
            runId = runId,
            sensorPartitionID = sensorPartitionID,
            ctx = ctx,
            out = out)) {
          }
        }
      }


      /**
       *  By toolName chamberName recipeName
       */
      // 上面的筛选条件都没有计算过
      if(!MatchStatus){
        // By toolName chamberName recipeName
        val recipeKey = s"${contextKey}||"
        if (this.contextMap.contains(recipeKey)) {
          calculateWindows(contextKey = recipeKey,
            runStartEvent = runStartEvent,
            runEndEvent = runEndEvent,
            runId = runId,
            sensorPartitionID = sensorPartitionID,
            ctx = ctx,
            out = out)
        }else{
//          logger.warn(s"processEnd 匹配context的tool、chamber、recipe ${runId} 失败: " + recipeKey)
          ctx.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0008B",
              "processEndWindowFunction",
              s"processEnd 匹配context的tool、chamber、recipe ${runId} 失败",
              Map[String,Any]("runId" -> runId,
                "recipeKey" -> recipeKey)
            ))
        }
      }
    } catch {
      case e: Exception => logger.error(ErrorCode("0230020003C", System.currentTimeMillis(),
        Map(), ExceptionInfo.getExceptionInfo(e)).toJson)
    }
  }

  /**
   * 添加context config
   *
   * @param configList
   */
  def addContextMapToContextMap(configList: ConfigData[List[ContextConfigData]]): Unit = {
    try {
      for (elem <- configList.datas) {

        if (configList.status) {

          //key=tool chamber recipe product stage
          val product = if (elem.productName == null) "" else elem.productName
          val stage = if (elem.stage == null) "" else elem.stage
          val key = s"${elem.toolName}|${elem.chamberName}|${elem.recipeName}|$product|$stage"
          try {

            if (!contextMap.contains(key)) {
              val newMap = new TrieMap[Long, ContextConfigData]()
              newMap.put(elem.contextId, elem)
              println(s"addContext To ContextMap: key: $key contextId ${elem.contextId} config :$elem")
              contextMap.put(key, newMap)
            } else {
              val newMap = contextMap.getOrElse(key, new TrieMap[Long, ContextConfigData]())
              if (!newMap.contains(elem.contextId)) {
                println(s"addContext To ContextMap: key: $key contextId ${elem.contextId} config :$elem")
              } else {
                println(s"updata Context ToContextMap: Key: $key ContextId ${elem.contextId} oldConfig :${newMap.getOrElse(elem.contextId, "error")} newConfig:$elem")
              }
              newMap.put(elem.contextId, elem)
              contextMap.put(key, newMap)
            }
          } catch {
            case e: Exception =>
              logger.warn(ErrorCode("002003b009C", System.currentTimeMillis(), Map("contextConfig" -> elem), ExceptionInfo.getExceptionInfo(e)).toJson)
          }

        } else {


          try {
            //key=tool chamber recipe product stage
            val product = if (elem.productName == null) "" else elem.productName
            val stage = if (elem.stage == null) "" else elem.stage
            val key = s"${elem.toolName}|${elem.chamberName}|${elem.recipeName}|$product|$stage"
            if (!contextMap.contains(key)) {
              //没有为什么还要删？
              logger.warn(ErrorCode("002003b011C", System.currentTimeMillis(), Map("key" -> key, "contextConfig" -> elem), "").toJson)

          } else {
            val newMap = contextMap.get(key).get

              newMap.remove(elem.contextId)
              println(s"delete Context key: $key contextId ${elem.contextId} config :$elem")
              contextMap.put(key, newMap)
            }

          } catch {
            case e: Exception =>
              logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("contextConfig" -> elem), ExceptionInfo.getExceptionInfo(e)).toJson)

          }


        }
      }
    }catch {
      case e: Exception =>
        logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("contextConfig" -> configList), ExceptionInfo.getExceptionInfo(e)).toJson)

    }


  }


  /**
   * 添加window config
   *
   * @param configList
   */
  def addWindowConfigToWindowConfigMap(configList: ConfigData[List[WindowConfigData]],
                                       sensorPartitionID: String,
                                       ctx: KeyedBroadcastProcessFunction[String,
                                         (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext): Unit = {

    if (configList.status) {
      //加载配置
      for (elem <- configList.datas) {
        //筛选ProcessEnd的window
        if (elem.calcTrigger == MainFabConstants.ProcessEnd) {
          try {
            val cacheKey = s"${elem.contextId}|$sensorPartitionID"

            if (!processEndWindowConfigMap.contains(cacheKey)) {

              val newMap = new concurrent.TrieMap[Long, WindowConfigData]()
              newMap.put(elem.controlWindowId, elem)
              processEndWindowConfigMap.put(cacheKey, newMap)
            } else {
              val configMap = processEndWindowConfigMap(cacheKey)

              if(!configMap.contains(elem.controlWindowId)){
                //新window
                configMap.put(elem.controlWindowId, elem)
                processEndWindowConfigMap.put(cacheKey, configMap)
              }else{
                //已有，看版本是不是比当前大，如果大才更新
                val oldVersion = configMap(elem.controlWindowId).controlPlanVersion

                if(oldVersion <= elem.controlPlanVersion){
                  configMap.put(elem.controlWindowId, elem)
                  processEndWindowConfigMap.put(cacheKey, configMap)
                }else{
//                  logger.warn(s"window 配置 后台推送了一个小版本，没有更新 当前生效老版本：$oldVersion 推送的新版本：${elem.controlPlanVersion} data:${elem.toJson}")
                  ctx.output(mainFabLogInfoOutput,
                    generateMainFabLogInfo("0002B",
                      "addWindowConfigToWindowConfigMap",
                      "window 配置 后台推送了一个小版本，没有更新 当前生效老版本",
                      Map[String,Any]("oldVersion" -> oldVersion.toString,
                        "newVersion" -> elem.controlPlanVersion.toString,
                        "WindowConfigData" -> elem.toJson
                      )))
                }
              }
            }
          } catch {
            case e: Exception =>
              logger.error(s"add WindowConfig Exception:${ExceptionInfo.getExceptionInfo(e)} data:${elem.toJson}")
          }
        }
      }
      //构建ContainerWindow对象 树形结构
      for (elem <- configList.datas) {
        try {
          if (elem.calcTrigger == MainFabConstants.ProcessEnd) {
            val cacheKey = s"${elem.contextId}|$sensorPartitionID"

            if (!processEndWindowConfigMap.contains(cacheKey)) {
//              logger.warn(s"add ContainerWindow error : $elem")
              ctx.output(mainFabLogInfoOutput,
                generateMainFabLogInfo("0003B",
                  "addWindowConfigToWindowConfigMap",
                  "add ContainerWindow error",
                  Map[String,Any]("WindowConfigData" -> elem.toJson
                  )))
            } else {
              if (elem.isConfiguredIndicator) {

                //同一个context的所有window
//                val windowConfigList = processEndWindowConfigMap(cacheKey)

//                try {
//                  val tuple = try {
//                    //构建
//                    createContainerWindow(elem, windowConfigList)
//                  }catch {
//                    case e: Exception =>
//                      logger.error(ErrorCode("0230020001C", System.currentTimeMillis(),
//                        Map("WindowId" -> elem.controlWindowId,
//                          "parentWindowId" -> elem.parentWindowId,
//                          "context" -> elem.contextId, "windowStart" -> elem.windowStart,
//                          "windowEnd" -> elem.windowEnd, "createContainerWindow" -> ""), ExceptionInfo.getExceptionInfo(e)).toJson)
//                      (null, null)
//                  }
//
//                  val key = elem.contextId + "|" + sensorPartitionID
//                  val controlWindowId: Long = elem.controlWindowId
//
//                  if (tuple._2 != null) {
//                    if (!containerWindowMap.contains(key)) {
//
//                      val newMap = new concurrent.TrieMap[Long, IControlWindow]()
//                      newMap.put(controlWindowId, tuple._2)
//
//                      containerWindowMap.put(key, newMap)
//                    } else {
//                      val configMap = containerWindowMap(key)
//
//                      configMap.put(controlWindowId, tuple._2)
//                      containerWindowMap.put(key, configMap)
//                    }
//
//                  } else {
//                    //孤岛window
////                    logger.warn(s"createContainerWindow error :$windowConfigList")
//                    ctx.output(mainFabLogInfoOutput,
//                      generateMainFabLogInfo("0004B",
//                        "addWindowConfigToWindowConfigMap",
//                        "孤岛window createContainerWindow error",
//                        Map[String,Any]("WindowConfigData" -> elem.toJson,
//                        "windowConfigList" -> windowConfigList.toJson)
//                      ))
//                  }

//                } catch {
//                  case e: Exception =>
//                    logger.error(ErrorCode("0230020002C", System.currentTimeMillis(),
//                      Map("WindowId" -> elem.controlWindowId,
//                        "parentWindowId" -> elem.parentWindowId,
//                        "context" -> elem.contextId, "windowStart" -> elem.windowStart,
//                        "windowEnd" -> elem.windowEnd), ExceptionInfo.getExceptionInfo(e)).toJson)
//                }
              }
            }
          }
        } catch {
          case e: Exception =>
            logger.error(s"add  WindowConfig error Exception:${ExceptionInfo.getExceptionInfo(e)}")
        }
      }


    } else {

      //Todo 删除时应该校验版本
      for (elem <- configList.datas) {
//        logger.warn(s"MainFabLog: Delete WindowConfig: contextId : ${elem.contextId}  config :$elem")

        ctx.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0005B",
            "addWindowConfigToWindowConfigMap",
            "MainFabLog: Delete WindowConfig",
            Map[String,Any]("contextId" -> elem.contextId,
              "WindowConfigData" -> elem.toJson)
          ))

        val cacheKey = s"${elem.contextId}|$sensorPartitionID"
        if (!processEndWindowConfigMap.contains(cacheKey)) {
//          logger.warn(s"MainFabLog: Delete windowConfigMap error")
          ctx.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0006B",
              "addWindowConfigToWindowConfigMap",
              "MainFabLog: Delete windowConfigMap error",
              Map[String,Any]("contextId" -> elem.contextId,
                "WindowConfigData" -> elem.toJson)
            ))

        } else {
          val configMap = processEndWindowConfigMap(cacheKey)
          configMap.remove(elem.controlWindowId)
          processEndWindowConfigMap.put(cacheKey, configMap)
        }

//        val key =  elem.contextId + "|" + sensorPartitionID

//        if (!containerWindowMap.contains(key)) {
////          logger.warn(s"MainFabLog: Delete containerWindowMap error")
//          ctx.output(mainFabLogInfoOutput,
//            generateMainFabLogInfo("0007B",
//              "addWindowConfigToWindowConfigMap",
//              "MainFabLog: Delete containerWindowMap error",
//              Map[String,Any]("contextId" -> elem.contextId,
//                "WindowConfigData" -> elem.toJson)
//            ))
//        } else {
//          val configMap = containerWindowMap(key)
//          configMap.remove(elem.controlWindowId)
//          containerWindowMap.put(key, configMap)
//        }
      }
    }

  }

  /**
   * 添加DataMissingIndicator
   *
   * @param config
   */
  def addDataMissingIndicatorToIndicatorConfigMap(config: ConfigData[IndicatorConfig]): Unit = {

    if (config.status) {
      val one = config.datas
      try {
        println(s"MainFabLog: addDataMissingIndicator: contextId : ${one.contextId}  config :$one")

        indicatorConfigByDataMissing.put(one.contextId, one)
      } catch {
        case e: Exception =>
          logger.warn(s"add DataMissingIndicator Exception: $e")
      }
    } else {
      val one = config.datas
      println(s"MainFabLog: Delete DataMissingIndicator: contextId : ${one.contextId}  config :$one")
      indicatorConfigByDataMissing.remove(one.contextId)

    }
  }

  /**
   * 添加indicatorConfigByCycleCount
   *
   * @param config
   */
  def addCycleCountIndicatorToIndicatorConfigMap(config: ConfigData[IndicatorConfig]): Unit = {

    if (config.status) {
      val one = config.datas
      try {
        println(s"MainFabLog: addCycleCountIndicator: controlWindowId : ${one.controlWindowId}  config :$one")

        indicatorConfigByCycleCount.put(one.controlWindowId, one)
      } catch {
        case e: Exception =>
          logger.warn(s"add CycleCountIndicator Exception: $e")
      }
    } else {
      val one = config.datas
      println(s"MainFabLog: Delete CycleCountIndicator: controlWindowId : ${one.controlWindowId}  config :$one")
      indicatorConfigByCycleCount.remove(one.controlWindowId)

    }
  }


  /**
   * 构建windows树
   *
   * @param config
   * @param map
   * @return
   */
  def createContainerWindow(config: WindowConfigData, map: concurrent.TrieMap[Long, WindowConfigData]): (WindowConfigData, IControlWindow) = {
    var window: IControlWindow = null


    window = ApiControlWindow.parse(config.controlWindowType, config.windowStart, config.windowEnd)

    val aliases = config.sensorAlias.filter(_.sensorAliasName != null)

//    if (aliases.nonEmpty) {

      window.setWindowId(config.controlWindowId)
      for (sensorAlias <- aliases) {

        for (elem <- sensorAlias.indicatorId) {
          window.addIndicatorConfig(elem, sensorAlias.sensorAliasId, sensorAlias.sensorAliasName)
        }

      }
      //是否是最上层的window
      if (config.isTopWindows) {
        //递归结束
        (config, window)
      } else {
        //获取父window的配置
        if (map.contains(config.parentWindowId)) {
          val parentWindowConfig = map.get(config.parentWindowId).get
          //递归,返回父window
          val parentWindow = createContainerWindow(parentWindowConfig, map)._2
          //添加子window
          parentWindow.addSubWindow(window)
          //返回子window的配置和构建好子window 的父window 对象
          (config, parentWindow)
        } else {
          logger.warn(ErrorCode("002003b005C", System.currentTimeMillis(), Map("WindowId" -> config.controlWindowId, "parentWindowId" -> config.parentWindowId, "context" -> config.contextId, "contextAllWindow" -> map.map(w => w._2.controlWindowId)), " window 配置错误,找不到父window，构建window结构失败").toJson)
          (config, null)
        }

      }
//    } else {
//      logger.warn(ErrorCode("002003b005C", System.currentTimeMillis(), Map("WindowId" -> config.controlWindowId, "parentWindowId" -> config.parentWindowId, "context" -> config.contextId, "contextAllWindow" -> map.map(w => w._2.controlWindowId)), " window 配置错误,找不到父window，构建window结构失败").toJson)
//      (config, null)
//    }


  }

  /**
   *
   * @param rawdataList
   * @return
   */
  def buildDataFlowData(rawdataList: List[MainFabRawData]): IDataFlow = {

    val flow = ApiControlWindow.buildDataFlow()
    val timeStampSet: mutable.Set[Long] = mutable.Set()

    for (one <- rawdataList) {
      val timestamp = one.timestamp
      if (!timeStampSet.contains(timestamp)) {
        // timestamp去重
        timeStampSet.add(timestamp)

        val dp = ApiControlWindow.buildDataPacket()
        val stepId = one.stepId.toInt
        dp.setTimestamp(one.timestamp)
        dp.setStepId(stepId)

        for (sensor <- one.data) {
          val s1 = ApiControlWindow.buildSensorData()
          s1.setSensorName(sensor.sensorName)
          s1.setStepId(stepId)
          s1.setSensorAlias(sensor.sensorAlias)
          s1.setValue(sensor.sensorValue)
          s1.setTimestamp(one.timestamp)
          s1.setUnit(sensor.unit)
          dp.getSensorDataMap.put(s1.getSensorAlias, s1)
        }
        flow.getDataList.add(dp)
      }
    }
    flow
  }


  /**
   *  判断是否已经生成过indicator
   */
  def isIndicatorRecordFunction(indicatorType: String, runId: String, contextKey: String, recordKey: String): Boolean = {
    try{
      val element = s"$indicatorType|$contextKey"

      if(indicatorRecordMap.contains(recordKey)){
        val runIdMap = indicatorRecordMap(recordKey)

        if(runIdMap.contains(runId)){
          val contextKeySet = runIdMap(runId)
          if(contextKeySet.contains(element)){
            return false
          }
          contextKeySet.add(element)
          runIdMap.put(runId, contextKeySet)
        }else{
          // 不包含indicatorType
          runIdMap.put(runId, mutable.Set(element))
        }

        // 过滤过期的RUN_ID, 只保留10个
        if(runIdMap.keys.size >= 20){
          val runIdSet = runIdMap.keys.toList
          for (runId <- runIdSet.sorted.take(10)){
            runIdMap.remove(runId)
            logger.info(s"filterRunIdTime_delete RUN_ID: " + runId)
          }
        }

        indicatorRecordMap.put(recordKey, runIdMap)
      }else{

        // 不包含runId
        val TypeMapNew = concurrent.TrieMap[String,  mutable.Set[String]](runId -> mutable.Set(element))
        indicatorRecordMap.put(recordKey, TypeMapNew)
      }
    }catch {
      case ex: Exception => logger.error("recordFunction error: " + ex.toString)
    }

    true
  }

  /**
   * 计算window
   */
  def calculateWindows(contextKey: String,
                       runStartEvent: RunEventData,
                       runEndEvent: RunEventData,
                       runId: String,
                       sensorPartitionID: String,
                       ctx: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext,
                       out: Collector[fdcWindowData]): Boolean = {
    var resStatus = false
    try {
      val contextList = contextMap(contextKey)

      for ((contextId, contextOne) <- contextList) {

        //      if(runStartEvent.dataVersion == ProjectConfig.JOB_VERSION) {
        //匹配是否配置DataMissingIndicator
        if (this.indicatorConfigByDataMissing.contains(contextId)) {

          val dataMissingConfig = indicatorConfigByDataMissing(contextId)

          // 过滤是否生成过indicator
          val indicatorType = s"dataMissing|${dataMissingConfig.indicatorId}"
          val recordKey = s"${runStartEvent.toolName}|${runStartEvent.chamberName}"
          if (isIndicatorRecordFunction(indicatorType, runId, contextKey, recordKey)) {

            calcDataMissingIndicator(runStartEvent, runEndEvent, runId, ctx, contextOne, dataMissingConfig)
          }
        }
        //      }

        val key = s"$contextId|$sensorPartitionID"

        //匹配window
        if (!this.processEndWindowConfigMap.contains(key)) {
          ctx.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0009B",
              "calculateWindows",
              s"这个context和sensorPartitionID 没有对应的 processEndWindowConfig 配置",
              Map[String,Any]("contextId" -> contextId,
                "toolName" -> contextOne.toolName,
                "chamberName" -> contextOne.chamberName,
                "key" -> key,
                "recipeName" -> contextOne.recipeName,
                "productName" -> contextOne.productName,
                "stage" -> contextOne.stage)
            ))

        } else {

          //获取context 下所有window 的配置 list
          val windowConfigList = processEndWindowConfigMap(key)

          //构建dataflow
          val dataflowTmp = EngineFunction.buildDataFlowData(rawDataState.values().toList)

          val dataflow = dataflowTmp._1
          //设定run的开始时间结束时间
          dataflow.setStartTime(runStartEvent.runStartTime)
          dataflow.setStopTime(runEndEvent.runEndTime)

          val SensorAliasSet = dataflowTmp._2

          // 过滤掉不存在于SensorAliasSet中的相关window配置
          for((controlWindowId, windowConfig) <- windowConfigList) {
            if (windowConfig.isConfiguredIndicator) {
              val sensorAlias = windowConfig.sensorAlias.filter(x => SensorAliasSet.contains(x.sensorAliasName))
              if (sensorAlias.nonEmpty) {
                val elem = windowConfig.copy(sensorAlias = sensorAlias)

                val tuple = try {
                  //构建
                  createContainerWindow(elem, windowConfigList)
                } catch {
                  case e: Exception =>
                    logger.error(ErrorCode("0230020001C", System.currentTimeMillis(),
                      Map("WindowId" -> elem.controlWindowId,
                        "parentWindowId" -> elem.parentWindowId,
                        "context" -> elem.contextId, "windowStart" -> elem.windowStart,
                        "windowEnd" -> elem.windowEnd, "createContainerWindow" -> ""), ExceptionInfo.getExceptionInfo(e)).toJson)
                    (null, null)
                }

                if (tuple._2 != null) {
                  val window = tuple._2
                  try {

                    window.attachDataFlow(dataflow)

                    resStatus = true

                    try {
                      if (debugConfig.contains(controlWindowId)) {
                        //                    ApiControlWindow.setDebugStateForWindow(window,  true, "debugWindow")
                        val re = ApiControlWindow.getWindowSnapshotInfo(window, "debugWindow")
                        ctx.output(debugOutput, s"[processEnd] ==> input controlWindowId: $controlWindowId runId: $runId contextKey: $contextKey key:$key\t setStartTime:" +
                          s" ${runStartEvent.runStartTime} setStopTime: ${runEndEvent.runEndTime} rawDataState：${rawDataState.values().toList}")
                        ctx.output(debugOutput, s"[processEnd] ==> input controlWindowId: $controlWindowId runId: $runId windowSnapshotInfo：${re}")
                      }
                    } catch {
                      case ex: Exception =>
                        ctx.output(debugOutput, ErrorCode("007006b005C", System.currentTimeMillis(), Map("debugWindow" ->
                          "", "controlWindowId" -> controlWindowId, "runId" -> runId, "triggerType" -> "processEnd"), ExceptionInfo.getExceptionInfo(ex)).toJson)
                    }

                    //计算window
                    val results: List[WindowData] = ApiControlWindow.calculate(window).toList


                    if (debugConfig.contains(controlWindowId)) {
                      ctx.output(debugOutput, s"[processEnd] ==> input controlWindowId: $controlWindowId runId: $runId results：${results}")
                    }

                    //获取该window的配置
                    val windowConfig = windowConfigList(controlWindowId)

                    val windowDataListResult: List[WindowData] = results.filter(elem => {
                      elem.windowId == controlWindowId
                    })

                    // 特殊处理cycle window
                    if (windowDataListResult.nonEmpty) {
                      var windowStartTime = windowDataListResult.head.startTime
                      var windowEndTime = windowDataListResult.head.stopTime

                      val windowData = generateFdcWindowData(runStartEvent, runEndEvent, contextId, contextOne, windowDataListResult, windowConfig, windowStartTime, windowEndTime)
                      out.collect(windowData)
                    }

                    calcCycleCount(contextKey, runStartEvent, runEndEvent, runId, ctx, contextOne, controlWindowId, windowDataListResult, windowConfig)
                  } catch {
                    case exception: Exception =>
                      logger.warn(ErrorCode("0230020005C", System.currentTimeMillis(), Map("controlWindowId" -> controlWindowId, "runId" -> runId), ExceptionInfo.getExceptionInfo(exception)).toJson)
                      if (debugConfig.contains(controlWindowId)) {
                        ctx.output(debugOutput, ErrorCode("0230020005C", System.currentTimeMillis(), Map("controlWindowId" -> controlWindowId, "runId" -> runId), ExceptionInfo.getExceptionInfo(exception)).toJson)
                      }

                  }
                }
              }
            }
          }
        }
      }
    }catch {
      case ex: Exception =>logger.error(ErrorCode("0230020007C", System.currentTimeMillis(),
        Map("runId" -> runId), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }

    resStatus
  }

  /**
   * 生成 fdcWindowData 结果数据
   * @param runStartEvent
   * @param runEndEvent
   * @param contextId
   * @param contextOne
   * @param results
   * @param windowConfig
   * @param windowStartTime
   * @param windowEndTime
   * @return
   */
  def generateFdcWindowData(runStartEvent: RunEventData,
                            runEndEvent: RunEventData,
                            contextId: Long,
                            contextOne: ContextConfigData,
                            results: List[WindowData],
                            windowConfig: WindowConfigData,
                            windowStartTime: Long,
                            windowEndTime: Long) = {
    fdcWindowData("fdcWindowDatas",
      windowListData(
        runEndEvent.toolName,
        contextOne.toolId,
        runEndEvent.chamberName,
        contextOne.chamberId,
        runEndEvent.recipeName,
        contextOne.recipeId,
        s"${runEndEvent.toolName}--${runEndEvent.chamberName}--${runStartEvent.runStartTime}",
        runEndEvent.dataMissingRatio,
        contextId,
        runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else ""),
        runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else ""),
        windowConfig.controlWindowId,
        windowConfig.controlPlanId,
        windowConfig.controlPlanVersion,
        runEndEvent.dataMissingRatio,
        windowConfig.windowStart,
        windowStartTime,
        windowConfig.windowEnd,
        windowEndTime,
        windowEndTime - windowStartTime,
        runStartEvent.runStartTime,
        runEndEvent.runEndTime,
        System.currentTimeMillis(),
        windowType = "processEnd",
        runEndEvent.DCType,
        contextOne.locationId,
        contextOne.locationName,
        contextOne.moduleId,
        contextOne.moduleName,
        contextOne.toolGroupId,
        contextOne.toolGroupName,
        contextOne.chamberGroupId,
        contextOne.chamberGroupName,
        contextOne.recipeGroupId,
        contextOne.recipeGroupName,
        contextOne.limitStatus,
        runEndEvent.materialName,
        pmStatus = runStartEvent.pmStatus,
        pmTimestamp = runStartEvent.pmTimestamp,
        area = contextOne.area,
        section = contextOne.section,
        mesChamberName = contextOne.mesChamberName,
        lotMESInfo = runStartEvent.lotMESInfo,
        windowDatasList = results,
        dataVersion = runStartEvent.dataVersion
      ))
  }

  /**
   * 计算cycleCount Indicator
 *
   * @param contextKey
   * @param runStartEvent
   * @param runEndEvent
   * @param runId
   * @param ctx
   * @param contextOne
   * @param controlWindowId
   * @param results
   * @param windowConfig
   */
  def calcCycleCount(contextKey: String,
                     runStartEvent: RunEventData,
                     runEndEvent: RunEventData,
                     runId: String,
                     ctx: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext,
                     contextOne: ContextConfigData,
                     controlWindowId: Long,
                     results: List[WindowData],
                     windowConfig: WindowConfigData) = {
    if (results.nonEmpty) {
      // 过滤是否生成过indicator

      val recordKey = s"${runStartEvent.toolName}|${runStartEvent.chamberName}"

      //cycleWindow count indicator
      if (windowConfig.controlWindowType == MainFabConstants.CycleWindowMaxType ||
        windowConfig.controlWindowType == MainFabConstants.CycleWindowMinType) {


        val cycleCount: Int = results.head.cycleUnitCount

        //                      if (runStartEvent.dataVersion == ProjectConfig.JOB_VERSION) {
        if (!indicatorConfigByCycleCount.contains(controlWindowId)) {
          logger.warn(ErrorCode("002003b012C", System.currentTimeMillis(), Map("controlWindowId" -> controlWindowId, "runId" -> runId), "没有找到CycleCount indicator 配置").toJson)
        } else {
          val cycleCountIndicatorConfig = indicatorConfigByCycleCount(controlWindowId)

          val indicatorType = s"cycleCount|${cycleCountIndicatorConfig.indicatorId}"
          if (isIndicatorRecordFunction(indicatorType, runId, contextKey, recordKey)) {

            val productList = runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "")
            val stageList = runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "")


            val cycleCountIndicatorResult = IndicatorResult(
              controlPlanId = cycleCountIndicatorConfig.controlPlanId,
              controlPlanName = cycleCountIndicatorConfig.controlPlanName,
              controlPlanVersion = cycleCountIndicatorConfig.controlPlanVersion,
              locationId = contextOne.locationId,
              locationName = contextOne.locationName,
              moduleId = contextOne.moduleId,
              moduleName = contextOne.moduleName,
              toolGroupId = contextOne.toolGroupId,
              toolGroupName = contextOne.toolGroupName,
              chamberGroupId = contextOne.chamberGroupId,
              chamberGroupName = contextOne.chamberGroupName,
              recipeGroupName = contextOne.recipeGroupName,
              runId = runId,
              toolName = runStartEvent.toolName,
              toolId = contextOne.toolId,
              chamberName = runStartEvent.chamberName,
              chamberId = contextOne.chamberId,
              indicatorValue = cycleCount.toString,
              indicatorId = cycleCountIndicatorConfig.indicatorId,
              indicatorName = cycleCountIndicatorConfig.indicatorName,
              algoClass = cycleCountIndicatorConfig.algoClass,
              indicatorCreateTime = System.currentTimeMillis(),
              missingRatio = runEndEvent.dataMissingRatio,
              configMissingRatio = cycleCountIndicatorConfig.missingRatio,
              runStartTime = runStartEvent.runStartTime,
              runEndTime = runEndEvent.runEndTime,
              windowStartTime = runStartEvent.runStartTime,
              windowEndTime = runEndEvent.runEndTime,
              windowDataCreateTime = System.currentTimeMillis(),
              limitStatus = contextOne.limitStatus,
              materialName = runStartEvent.materialName,
              recipeName = runStartEvent.recipeName,
              recipeId = contextOne.recipeId,
              product = productList,
              stage = stageList,
              bypassCondition = cycleCountIndicatorConfig.bypassCondition,
              pmStatus = runStartEvent.pmStatus,
              pmTimestamp = runStartEvent.pmTimestamp,
              area = contextOne.area,
              section = contextOne.section,
              mesChamberName = contextOne.mesChamberName,
              lotMESInfo = runStartEvent.lotMESInfo,
              unit = "",
              dataVersion = runStartEvent.dataVersion,
              cycleIndex = "0"
            )


            val data = FdcData[IndicatorResult](
              "indicator",
              cycleCountIndicatorResult
            )

            ctx.output(dataMissingRatioOutput, data)
          }
        }
        //                      }
      }
    } else {
      //logger.error(ErrorCode("0230020004C", System.currentTimeMillis(), Map("controlWindowId" -> controlWindowId, "runId" -> runId), "window切分结果是空").toJson)
      ctx.output(mainFabLogInfoOutput,
        generateMainFabLogInfo("0004C",
          "calcCycleCount",
          s"window切分结果是空",
          Map[String,Any]("contextId" -> contextOne.contextId,
            "toolName" -> contextOne.toolName,
            "chamberName" -> contextOne.chamberName,
            "key" -> runId,
            "recipeName" -> contextOne.recipeName,
            "productName" -> contextOne.productName,
            "stage" -> contextOne.stage)
        ))
    }
  }

  /**
   * 计算 DataMissingIndicator
 *
   * @param runStartEvent
   * @param runEndEvent
   * @param runId
   * @param ctx
   * @param contextOne
   * @param dataMissingConfig
   */
  def calcDataMissingIndicator(runStartEvent: RunEventData, runEndEvent: RunEventData, runId: String, ctx: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext, contextOne: ContextConfigData, dataMissingConfig: IndicatorConfig) = {
    val productList = runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "")
    val stageList = runEndEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "")


    val dataMissingIndicatorResult = IndicatorResult(controlPlanId = dataMissingConfig.controlPlanId,
      controlPlanName = dataMissingConfig.controlPlanName,
      controlPlanVersion = dataMissingConfig.controlPlanVersion,
      locationId = contextOne.locationId,
      locationName = contextOne.locationName,
      moduleId = contextOne.moduleId,
      moduleName = contextOne.moduleName,
      toolGroupId = contextOne.toolGroupId,
      toolGroupName = contextOne.toolGroupName,
      chamberGroupId = contextOne.chamberGroupId,
      chamberGroupName = contextOne.chamberGroupName,
      recipeGroupName = contextOne.recipeGroupName,
      runId = runId,
      toolName = runStartEvent.toolName,
      toolId = contextOne.toolId,
      chamberName = runStartEvent.chamberName,
      chamberId = contextOne.chamberId,
      indicatorValue = runEndEvent.dataMissingRatio.toString,
      indicatorId = dataMissingConfig.indicatorId,
      indicatorName = dataMissingConfig.indicatorName,
      algoClass = dataMissingConfig.algoClass,
      indicatorCreateTime = System.currentTimeMillis(),
      missingRatio = runEndEvent.dataMissingRatio,
      configMissingRatio = dataMissingConfig.missingRatio,
      runStartTime = runStartEvent.runStartTime,
      runEndTime = runEndEvent.runEndTime,
      windowStartTime = runStartEvent.runStartTime,
      windowEndTime = runEndEvent.runEndTime,
      windowDataCreateTime = System.currentTimeMillis(),
      limitStatus = contextOne.limitStatus,
      recipeName = runEndEvent.recipeName,
      recipeId = contextOne.recipeId,
      product = productList,
      stage = stageList,
      materialName = runEndEvent.materialName,
      bypassCondition = dataMissingConfig.bypassCondition,
      pmStatus = runStartEvent.pmStatus,
      pmTimestamp = runStartEvent.pmTimestamp,
      area = contextOne.area,
      section = contextOne.section,
      mesChamberName = contextOne.mesChamberName,
      lotMESInfo = runEndEvent.lotMESInfo,
      unit = "",
      dataVersion = runStartEvent.dataVersion,
      cycleIndex = "0"
    )


    val data = FdcData[IndicatorResult](
      "indicator",
      dataMissingIndicatorResult
    )
    ctx.output(dataMissingRatioOutput, data)
  }

  /**
   * 生成日志信息
 *
   * @param debugCode
   * @param message
   * @param paramsInfo
   * @param dataInfo
   * @param exception
   * @return
   */
  def generateMainFabLogInfo(debugCode:String,funName:String,message:String,paramsInfo:Map[String,Any]=null,dataInfo:String="",exception:String = ""): MainFabLogInfo = {
    MainFabLogInfo(mainFabDebugCode = job_fun_DebugCode + debugCode,
      jobName = jobName,
      optionName = optionName,
      functionName = funName,
      logTimestamp = DateTimeUtil.getCurrentTimestamp,
      logTime = DateTimeUtil.getCurrentTime(),
      message = message,
      paramsInfo = paramsInfo,
      dataInfo = dataInfo,
      exception = exception)
  }

}
