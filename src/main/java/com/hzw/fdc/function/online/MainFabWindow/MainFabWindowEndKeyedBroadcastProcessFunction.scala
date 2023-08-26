package com.hzw.fdc.function.online.MainFabWindow


import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.engine.api.{ApiControlWindow, IControlWindow, IRealTimeSupport}
import com.hzw.fdc.engine.controlwindow.data.defs.{IDataFlow, IDataPacket}
import com.hzw.fdc.engine.controlwindow.data.impl.{DataPacket, WindowClipResult}
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{WindowConfigData, _}
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util
import java.util.Date

import com.hzw.fdc.util.InitFlinkFullConfigHbase.{ContextDataType, IndicatorDataType, readHbaseAllConfig}
import org.apache.flink.api.common.time.Time

import scala.collection.JavaConverters._
import scala.collection.{concurrent, mutable}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{ListBuffer, Set}
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConverters._


/**
 *    author：yuxiang
 * *  time： 2023.03.14
 * *  功能：将数据流和window配置组合，针对window切窗口， 输出windowEnd切出的结果
 */
class MainFabWindowEndKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode, fdcWindowData] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowEndKeyedBroadcastProcessFunction])

  lazy val WindowEndRunDataOutput = new OutputTag[RunEventData]("WindowEndRunData")
  lazy val cycleCountDataOutput = new OutputTag[FdcData[IndicatorResult]]("cycleCountData")
  lazy val debugOutput = new OutputTag[String]("debugTest")

  // 侧道输出日志信息
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  val job_fun_DebugCode:String = "024002"
  val jobName:String = "MainFabWindowEndWindowService"
  val optionName : String = "MainFabWindowEndKeyedBroadcastProcessFunction"


  //tool chamber recipe product stage --> context--->ContextConfig
  var contextMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, ContextConfigData]]()

  //sensorPartitionID|contextId -->controlWindowId--->WindowConfigData
  private val windowEndConfigMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, WindowConfigData]]()

  //{"controlWindowId":"IndicatorConfig"}   cucleCount indicator
  var indicatorConfigByCycleCount = new concurrent.TrieMap[Long, IndicatorConfig]()

  // ============================== 缓存的数据 ======================================

  //contextId|sensorPartitionID|traceId ---> controlWindowId--->IControlWindow
  private val containerWindowMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, IControlWindow]]()

  //{timestamp:  [stepId, timestamp, [1: sensorAlias, 2: sensorValue, 3: unit] ]}
  private var rawDataState: MapState[Long, windowRawData] = _

  // 判断哪些window已经切完窗口了
  private var traceIdWindowState: ListState[Long] = _

  // {"traceId": "RunStart"}
  private var traceIdRecipeStateMap: MapState[String, RunEventData] = _


  //{"traceId": stepId}  ====> 保存traceId当前stepId
  private var traceIdStepIdMap: MapState[String, String] = _


  //=========用于debug调试================
  // controlWindowId -> function
  var debugConfig = new concurrent.TrieMap[Long, mutable.Set[String]]()

  /** The state that is maintained by this process function */
  private var runIdState: ValueState[windowEndTimeOutTimestamp] = _

  // 试切窗口时间
  private var scanWindowTimeState:ValueState[scanWindowTime] = _

  override def open(parameters: Configuration): Unit = {

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 初始化ContextConfig
    //val contextConfigList = InitFlinkFullConfigHbase.ContextConfigList
    val contextConfigList = readHbaseAllConfig[List[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE, ContextDataType)
    contextConfigList.foreach(addContextMapToContextMap)

//    // 26小时过期
//    val ttlConfig:StateTtlConfig = StateTtlConfig
//      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
//      .useProcessingTime()
//      .updateTtlOnCreateAndWrite()
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//      // compact 过程中清理过期的状态数据
//      .cleanupInRocksdbCompactFilter(5000)
//      .build()
//
//    // 30分组过期
//    val min30TtlConfig:StateTtlConfig = StateTtlConfig
//      .newBuilder(Time.minutes(30L))
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


    //val indicatorConfigList = InitFlinkFullConfigHbase.IndicatorConfigList
    val indicatorConfigList = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)
    // 初始化 dataMissingRatio 和 cycleCountIndicator
    for (one <- indicatorConfigList) {
      if (one.datas.algoClass == MainFabConstants.cycleCountIndicator) {
        addCycleCountIndicatorToIndicatorConfigMap(one)
      }
    }

    val traceIdRecipeStateDescription: MapStateDescriptor[String,RunEventData] = new
        MapStateDescriptor[String,RunEventData]("traceIdRecipeStateDescription",
          TypeInformation.of(classOf[String]),TypeInformation.of(classOf[RunEventData]))
    // 设置过期时间
//    traceIdRecipeStateDescription.enableTimeToLive(ttlConfig)
    traceIdRecipeStateMap = getRuntimeContext.getMapState(traceIdRecipeStateDescription)

    val runIdStateWindowEndDescription = new ValueStateDescriptor[windowEndTimeOutTimestamp]("windowEndIndicatorOutValueState",
      TypeInformation.of(classOf[windowEndTimeOutTimestamp]))
    // 设置过期时间
//    runIdStateWindowEndDescription.enableTimeToLive(ttlConfig)
    runIdState = getRuntimeContext.getState(runIdStateWindowEndDescription)

    val scanWindowTimeStateDescription = new ValueStateDescriptor[scanWindowTime]("scanWindowTimeStateDescriptionValueState",
      TypeInformation.of(classOf[scanWindowTime]))
    // 设置过期时间
//    scanWindowTimeStateDescription.enableTimeToLive(ttlConfig)
    scanWindowTimeState = getRuntimeContext.getState(scanWindowTimeStateDescription)
//    scanWindowTimeState.update((System.currentTimeMillis(), false))


    val traceIdStepIdStateDescription: MapStateDescriptor[String,String] = new
        MapStateDescriptor[String,String]("traceIdStepIdStateDescription",
          TypeInformation.of(classOf[String]),TypeInformation.of(classOf[String]))
    // 设置过期时间
//    traceIdRecipeStateDescription.enableTimeToLive(ttlConfig)
    traceIdStepIdMap = getRuntimeContext.getMapState(traceIdStepIdStateDescription)

    val rawDataStateDescription: MapStateDescriptor[Long, windowRawData] = new
        MapStateDescriptor[Long, windowRawData]("windowEndRawTraceState",TypeInformation.of(classOf[Long]),
          TypeInformation.of(classOf[windowRawData]))
    // 设置过期时间
//    rawDataStateDescription.enableTimeToLive(min30TtlConfig)
    rawDataState = getRuntimeContext.getMapState(rawDataStateDescription)

    val traceIdWindowStateDescription: ListStateDescriptor[Long] = new
        ListStateDescriptor[Long]("windowEndTraceIdWindowState", TypeInformation.of(classOf[Long]))
    // 设置过期时间
//    rawDataStateDescription.enableTimeToLive(ttlConfig)
    traceIdWindowState = getRuntimeContext.getListState(traceIdWindowStateDescription)
  }

  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode),
    JsonNode, fdcWindowData]#OnTimerContext, out: Collector[fdcWindowData]): Unit = {
    try {
      runIdState.value() match {
        case windowEndTimeOutTimestamp(key, traceKey, lastModified)
          if (System.currentTimeMillis() >= lastModified + 60000L) =>
           // 清除状态
          traceIdStepIdMap.clear()
          scanWindowTimeState.clear()
          close()
        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("windowEnd onTimer" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }


  override def processElement(value: (String, JsonNode, JsonNode), readOnlyContext: KeyedBroadcastProcessFunction[String,
    (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext, collector: Collector[fdcWindowData]): Unit = {

    try {
      val in1 = value._2
      val recordKey = value._1
      val sensorPartitionID = recordKey.split("\\|").last

      val dataType = in1.get(MainFabConstants.dataType).asText()
      val traceId = recordKey.split("\\|").head


      if (dataType == MainFabConstants.rawData) {

        /**
         * 缓存rawData数据,存在rocksdb, 解决之前放在内存导致Flink job内存撑爆的问题
         */
        val one = toBean[MainFabRawData](in1)
        val stepId = one.stepId
        val timestamp = one.timestamp
        val resData: mutable.Set[(String, Double, String)] = mutable.Set()
        for (elem <- one.data) {
          resData.add((elem.sensorAlias, elem.sensorValue, elem.unit))
        }
        rawDataState.put(timestamp, windowRawData(stepId, timestamp, resData.toList))

        if (traceIdRecipeStateMap.contains(traceId)) {
          val stepId = in1.findPath("stepId").asText()
          val startMathStatus = if (traceIdStepIdMap.contains(traceId)) {
            if (traceIdStepIdMap.get(traceId) != stepId) {
              traceIdStepIdMap.put(traceId, stepId)
              true
            } else {
              false
            }
          } else {
            traceIdStepIdMap.put(traceId, stepId)
            true
          }
          /**
           * F1P2-17  Window end优化成stepId 改变的时候才会去尝试切窗口
           */
          var scanWindowTuple = scanWindowTimeState.value()
          if(scanWindowTuple == null){
            scanWindowTuple = scanWindowTime(System.currentTimeMillis(), status = false)

            scanWindowTimeState.update(scanWindowTuple)
          }
          val scanWindowTime1 = scanWindowTuple.time
          val scanWindowStatus = scanWindowTuple.status

          if (startMathStatus || scanWindowStatus) {
            //两种方案
            // 方案 1
            //            // 开始计算
            //            val runStartEvent = traceIdRecipeStateMap.get(traceId)
            //            scanWindow(runStartEvent, in1, sensorPartitionID, readOnlyContext, collector)

            // 方案 2  stepId改变 同时要满足和上次尝试切窗口的时间差至少要1.5min以上
            val nowTime = System.currentTimeMillis()
            if((nowTime-scanWindowTime1) > 300000) {
              scanWindowTimeState.update(scanWindowTime(nowTime, status = false))
              // 开始计算
              val runStartEvent = traceIdRecipeStateMap.get(traceId)
              scanWindow(runStartEvent, in1, sensorPartitionID, readOnlyContext, collector)
            }else{
              scanWindowTimeState.update(scanWindowTime(scanWindowTime1, status = true))
            }
          }
        } else {
          //logger.warn(s"rawData not found eventStart traceId :$traceId ")
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo(
              "b002A",
              "processElement",
              "rawData not found eventStart traceId :",
              Map[String,Any]("traceId" -> traceId, "recordKey" -> recordKey)
            ))
        }
      } else if (dataType == MainFabConstants.eventStart) {
        try {
          val windowConfig = toBean[ConfigData[List[WindowConfigData]]](value._3)
          addWindowConfigToWindowConfigMap(sensorPartitionID, windowConfig)
        } catch {
          case e: Exception => logger.warn(s"windowConfig json error  data:$value._3  " +
            s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

        val runStartEvent = toBean[RunEventData](in1)
        traceIdRecipeStateMap.put(runStartEvent.traceId, runStartEvent)

        scanWindow(runStartEvent, in1, sensorPartitionID, readOnlyContext, collector)

      } else if (dataType == MainFabConstants.eventEnd) {
        try{
          if (traceIdRecipeStateMap.contains(traceId)) {
            val runStartEvent = traceIdRecipeStateMap.get(traceId)

            scanWindow(runStartEvent, in1, sensorPartitionID, readOnlyContext, collector)
            traceIdRecipeStateMap.remove(traceId)
          }else{
            //logger.warn("windowEnd no exist traceId: " + traceId)
            readOnlyContext.output(mainFabLogInfoOutput,
              generateMainFabLogInfo(
                "b002A",
                "processElement",
                "windowEnd no exist traceId: ",
                Map[String,Any]("traceId" -> traceId)
              ))
          }
        }catch {
          case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
            Map("msg" -> "windEnd划窗口失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
        }finally {
          rawDataState.clear()
          traceIdWindowState.clear()
          traceIdRecipeStateMap.clear()
          traceIdStepIdMap.clear()
          runIdState.clear()
          scanWindowTimeState.clear()
          close()
        }
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("msg" -> "windEnd处理数据流失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }

  /**
   *  广播流处理
   */
  override def processBroadcastElement(in2: JsonNode, context: KeyedBroadcastProcessFunction[String,
    (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#Context, collector: Collector[fdcWindowData]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()

      if (dataType == MainFabConstants.context) {
        try {
          val contextConfig = toBean[ConfigData[List[ContextConfigData]]](in2)
          addContextMapToContextMap(contextConfig)
        } catch {
          case e: Exception => logger.warn(s"contextConfig json error data:$in2  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }
      }else if (dataType == "debugTest") {
        // 用于window切窗口debug调试
        addDebugTest(in2)
      }else if(dataType == MainFabConstants.indicatorConfig){
        val indicatorConfig = toBean[ConfigData[IndicatorConfig]](in2)
        if(indicatorConfig.datas.algoClass == MainFabConstants.cycleCountIndicator) addCycleCountIndicatorToIndicatorConfigMap(indicatorConfig)
      }
    } catch {
      case e: Exception => logger.warn(s"windowJob processBroadcastElement error data:$in2  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }

  override def close(): Unit = {
    super.close()
    //    windowConfigStateMap.clear()

  }

  /**
   *   增加对切窗口debug调试
   */
  def addDebugTest(in2: JsonNode): Unit = {
    try {
      val debugScala = toBean[DebugScala](in2)
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
      case e: Exception => logger.warn(s"debugScala json error data:$in2  " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }


  /**
   * 添加context config
   *
   */
  def addContextMapToContextMap(configList: ConfigData[List[ContextConfigData]]): Unit = {
    try {
      for (elem <- configList.datas) {
        println(s"${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)}  contextConfig: $elem")

        if (configList.status) {
          //key=tool chamber recipe product stage
          val product = if (elem.productName == null) "" else elem.productName
          val stage = if (elem.stage == null) "" else elem.stage
          val key = s"${elem.toolName}${elem.chamberName}${elem.recipeName}$product$stage"
          try {

            if (!contextMap.contains(key)) {
              val newMap = new TrieMap[Long, ContextConfigData]()
              newMap.put(elem.contextId, elem)
              contextMap.put(key, newMap)
            } else {
              val newMap = contextMap.getOrElse(key, new TrieMap[Long, ContextConfigData]())
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
            val key = s"${elem.toolName}${elem.chamberName}${elem.recipeName}$product$stage"
            if (contextMap.contains(key)) {
              val newMap = contextMap(key)
              newMap.remove(elem.contextId)
              println(s"delete Context key: $key contextId ${elem.contextId} config :$elem")
              contextMap.put(key, newMap)
            }
          } catch {
            case e: Exception =>
              logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("contextConfig" -> configList),
                ExceptionInfo.getExceptionInfo(e)).toJson)
          }
        }
      }
    }catch {
      case e: Exception =>
        logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("contextConfig" -> configList), ExceptionInfo.getExceptionInfo(e)).toJson)

    }

  }

  /**
   * 添加indicatorConfigByCycleCount
   *
   */
  def addCycleCountIndicatorToIndicatorConfigMap(config: ConfigData[IndicatorConfig]): Unit = {
    try {
      if (config.status) {
        val one = config.datas
        try {
          indicatorConfigByCycleCount.put(one.controlWindowId, one)
        } catch {
          case e: Exception =>
            logger.warn(s"add CycleCountIndicator Exception: $e")
        }
      } else {
        val one = config.datas
        indicatorConfigByCycleCount.remove(one.controlWindowId)
      }
    }catch {
    case e: Exception =>
      logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("function" ->
        "addCycleCountIndicatorToIndicatorConfigMap"), e.toString).toJson)
    }
  }


  /**
   * 添加window config
   *
   */
  def addWindowConfigToWindowConfigMap(sensorPartitionID: String, configList: ConfigData[List[WindowConfigData]]): Unit = {
    if (configList.status) {
      //加载配置
      for (elem <- configList.datas) {
        //筛选ProcessEnd的window
        //加载所有window
        try {
          val key = s"${sensorPartitionID}|${elem.contextId}"
          if (!windowEndConfigMap.contains(key)) {

            val newMap = concurrent.TrieMap[Long, WindowConfigData](elem.controlWindowId -> elem)
            windowEndConfigMap.put(key, newMap)
          } else {
            val configMap = windowEndConfigMap(key)

            if(!configMap.contains(elem.controlWindowId)){
              //新window
              configMap.put(elem.controlWindowId, elem)
              windowEndConfigMap.put(key, configMap)
            }else{
              //已有，看版本是不是比当前大，如果大才更新
              val oldVersion = configMap(elem.controlWindowId).controlPlanVersion

              if(oldVersion <= elem.controlPlanVersion){
                configMap.put(elem.controlWindowId, elem)
                windowEndConfigMap.put(key, configMap)
              }else{
                logger.warn(s"window 配置 后台推送了一个小版本，没有更新 当前生效老版本：$oldVersion " +
                  s"推送的新版本：${elem.controlPlanVersion} data:${elem.toJson}")
              }
            }
          }
        } catch {
          case e: Exception =>
            logger.warn(s"add WindowConfig Exception:${ExceptionInfo.getExceptionInfo(e)} data:${elem.toJson}")
        }
      }
    } else {

      //删除时应该校验版本
      for (elem <- configList.datas) {
        val key = s"${sensorPartitionID}|${elem.contextId}"

        if (windowEndConfigMap.contains(key)) {
          val configMap = windowEndConfigMap(key)
          configMap.remove(elem.controlWindowId)
          windowEndConfigMap.put(key, configMap)
        }
      }
    }
  }


  /**
   * 创建 window end 模式的window 对象 ICW
   *
   */
  def createRealTimeWindow(traceId: String, contextId: Long, sensorPartitionID: String,
                           readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode,
    fdcWindowData]#ReadOnlyContext): Unit = {

    val key = s"${contextId}|${sensorPartitionID}|${traceId}"

    val windowKey =   s"${sensorPartitionID}|${contextId}"
    if(windowEndConfigMap.contains(windowKey)) {

      val configList = windowEndConfigMap(windowKey)
      //构建ContainerWindow对象 树形结构
      for ((controlWindowId, elem) <- configList) {
        try {

          if (elem.calcTrigger == MainFabConstants.WindowEnd) {
            if (!windowEndConfigMap.contains(windowKey)) {
              logger.warn(s"add ContainerWindowEnd error : $elem")
            } else {
              //同一个context的所有window

              val windowConfigList = windowEndConfigMap(windowKey)

              if (elem.isConfiguredIndicator) {
                try {

                  val tuple = createContainerWindow(elem, windowConfigList, readOnlyContext)
                  val window = tuple._2
                  if (containerWindowMap.contains(key)) {
                    val configMap = containerWindowMap(key)

                    configMap.put(controlWindowId, window)
                    containerWindowMap.put(key, configMap)
                  } else {
                    val newMap = concurrent.TrieMap[Long, IControlWindow](controlWindowId -> window)
                    containerWindowMap.put(key, newMap)
                  }

                } catch {
                  case e: Exception =>
                    logger.warn(ErrorCode("002003b014C", System.currentTimeMillis(),
                      Map("WindowId" -> elem.controlWindowId,
                        "parentWindowId" -> elem.parentWindowId,
                        "context" -> elem.contextId, "windowStart" -> elem.windowStart,
                        "windowEnd" -> elem.windowEnd), ExceptionInfo.getExceptionInfo(e)).toJson)
                }
              }
            }
          }
        } catch {
          case e: Exception =>
            logger.warn(s"add  WindowEndConfig Exception:$e")
        }
      }
    }
  }


  /**
   * 构建windows树
   */
  def createContainerWindow(config: WindowConfigData, map: concurrent.TrieMap[Long, WindowConfigData],
                            readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode,
    fdcWindowData]#ReadOnlyContext): (WindowConfigData, IControlWindow) = {
    val window: IControlWindow = ApiControlWindow.parse(config.controlWindowType, config.windowStart, config.windowEnd)

    window.setWindowId(config.controlWindowId)
    val aliases = config.sensorAlias.filter(_.sensorAliasName != null)

    //    if (aliases.nonEmpty) {
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
        val parentWindow = createContainerWindow(parentWindowConfig, map, readOnlyContext)._2
        //添加子window
        parentWindow.addSubWindow(window)
        //返回子window的配置和构建好子window 的父window 对象
        (config, parentWindow)
      } else {
        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo(
            "002003b005C",
            "createContainerWindow",
            s"window 配置错误,找不到父window，构建window结构失败",
            Map[String,Any]("WindowId" -> config.controlWindowId,
              "parentWindowId" -> config.parentWindowId, "context" -> config.contextId,
              "contextAllWindow" -> map.map(w => w._2.controlWindowId))
          ))
        (config, null)
      }
    }
  }


  /**
   * 扫描window
   */
  def scanWindow(runStartData: RunEventData,
                 data: JsonNode,
                 sensorPartitionID: String,
                 readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode, fdcWindowData]#ReadOnlyContext,
                 out: Collector[fdcWindowData]): Unit = {

    val toolName = runStartData.toolName
    val chamberName = runStartData.chamberName
    val recipeName = runStartData.recipeName
    val contextKey = s"$toolName$chamberName$recipeName"
    var MatchStatus:Boolean = false
    val contextKeySet: mutable.Set[String] = mutable.Set()

    /**
     *  By toolName chamberName recipeName product stage
      */
    val productStageList = runStartData.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") + x.get.stage.getOrElse("") else "").distinct
    for (productStage <- productStageList) {
      val productStageKey = s"$contextKey$productStage"

      if (this.contextMap.contains(productStageKey)) {
        contextKeySet.add(productStageKey)
        MatchStatus = true

        if(calculateWindows(contextKey = productStageKey,
          data = data,
          readOnlyContext = readOnlyContext,
          sensorPartitionID = sensorPartitionID,
          out = out)){
        }
      }
    }

    /**
     *  By toolName chamberName recipeName    stage
     */
    val stageList = runStartData.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "").distinct
    for (stage <- stageList) {
      val stageKey = s"$contextKey$stage"

      if (this.contextMap.contains(stageKey) && !contextKeySet.contains(stageKey)) {
        contextKeySet.add(stageKey)
        MatchStatus = true

        if(calculateWindows(contextKey = stageKey,
          data = data,
          readOnlyContext = readOnlyContext,
          sensorPartitionID = sensorPartitionID,
          out = out)){
        }
      }
    }

    /**
     *  By toolName chamberName recipeName  product
     */
    val productList = runStartData.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "").distinct
    for (product <- productList) {
      val productKey = s"$contextKey$product"

      if (this.contextMap.contains(productKey) && !contextKeySet.contains(productKey)) {
        contextKeySet.add(productKey)
        MatchStatus = true

        if (calculateWindows(contextKey = productKey,
          data = data,
          readOnlyContext = readOnlyContext,
          sensorPartitionID = sensorPartitionID,
          out = out)) {
        }
      }
    }


    /**
     *  By toolName chamberName recipeName
     */
    // 上面的筛选条件都没有计算过
    if(!MatchStatus) {
      // By toolName chamberName recipeName
      if (this.contextMap.contains(contextKey)) {
        calculateWindows(contextKey = s"$contextKey",
          data = data,
          readOnlyContext = readOnlyContext,
          sensorPartitionID = sensorPartitionID,
          out = out)
      }else{
        logger.warn(s"windowEnd 匹配context的tool、chamber、recipe ${runStartData.runId} 失败: " + contextKey)
      }
    }
  }


  /**
   * 计算window
   */
  def calculateWindows(contextKey: String,
                       data: JsonNode,
                       sensorPartitionID: String,
                       readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode,
                         fdcWindowData]#ReadOnlyContext,
                       out: Collector[fdcWindowData]): Boolean = {
    var status = false
    try {

      val contextList = contextMap(contextKey)

      for ((contextId, contextOne) <- contextList) {
        if(windowEndConfigMap.contains(s"${sensorPartitionID}|${contextId}")) {
          status = true

          try {
            val dataType = data.get(MainFabConstants.dataType).asText()
            if (dataType == MainFabConstants.rawData) {
              val addStepRawData = toBean[MainFabRawData](data)
              val key = s"$contextId|$sensorPartitionID|${addStepRawData.traceId}"

              rawDataCalculatedWindowFunction(contextId, contextOne, key, dataType, contextKey,
                sensorPartitionID, data, readOnlyContext, out)

            } else if (dataType == MainFabConstants.eventStart) {
              val RunEventStart = toBean[RunEventData](data)
              //event start 创建 ICW
              createRealTimeWindow(RunEventStart.traceId, contextId, sensorPartitionID, readOnlyContext)

              readOnlyContext.output(WindowEndRunDataOutput, RunEventStart)

            } else if (dataType == MainFabConstants.eventEnd) {

              val RunEventEnd = toBean[RunEventData](data)
              val runStartEvent = traceIdRecipeStateMap.get(RunEventEnd.traceId)

              val key = s"$contextId|$sensorPartitionID|${RunEventEnd.traceId}"

              runEndCalculatedWindowFunction(contextId, runStartEvent.runStartTime, contextOne, key, dataType,
                contextKey, sensorPartitionID, data, readOnlyContext, out)

              containerWindowMap.remove(key)
            }
          } catch {
            case exception: Exception =>
              logger.warn(ErrorCode("002002b001C", System.currentTimeMillis(),
                Map("runId" -> data.get(MainFabConstants.traceId).asText()), ExceptionInfo.getExceptionInfo(exception)).toJson)
          }
        } else{
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo(
              "002003b001D",
              "calculateWindows",
              s"windowEndConfigMap no exit ${sensorPartitionID} \t contextKey:$contextKey",
              Map[String,Any]("contextId" -> contextId)
            ))
        }
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("002002b001d", System.currentTimeMillis(),
        Map("runId" -> data.get(MainFabConstants.traceId).asText()), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
    status
  }


  /**
   *  在runEnd结束的时候开始切窗口
   */
  def runEndCalculatedWindowFunction(contextId: Long,
                                     runStartTime: Long,
                                     contextOne: ContextConfigData,
                                     key: String,
                                     dataType: String,
                                     contextKey: String,
                                     sensorPartitionID: String,
                                     data: JsonNode,
                                     readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode,
                                       fdcWindowData]#ReadOnlyContext,
                                     out: Collector[fdcWindowData]): Unit = {
    try{
      val RunEventEnd = toBean[RunEventData](data)
      val key = s"$contextId|${sensorPartitionID}|${RunEventEnd.traceId}"

      // 如果重启可以不丢
      if(!containerWindowMap.contains(key)){
        createRealTimeWindow(RunEventEnd.traceId, contextId, sensorPartitionID, readOnlyContext)
      }

      if (containerWindowMap.contains(key)) {

        val realTimeWindowList = containerWindowMap(key)
        val rawDataList = rawDataState.values().toList


        val finishWindowIdList = traceIdWindowState.get.toList

        val runStartEvent = traceIdRecipeStateMap.get(RunEventEnd.traceId)
        val runId = s"${runStartEvent.toolName}--${runStartEvent.chamberName}--${runStartEvent.runStartTime}"


        var logStatus = true

        for ((windowId, realTimeWindow) <- realTimeWindowList) {
          // 判断是否已经计算完成
          if(!finishWindowIdList.contains(windowId) && realTimeWindow != null) {
            try {
              if (debugConfig.contains(windowId)) {
                //                    ApiControlWindow.setDebugStateForWindow(window,  true, "debugWindow")
                val re = ApiControlWindow.getWindowSnapshotInfo(realTimeWindow, "debugWindow")
                readOnlyContext.output(debugOutput, s"[processEnd] ==> input controlWindowId: $windowId " +
                  s"runId: $runId contextKey: $contextKey key:$key\t setStartTime: ${runStartEvent.runStartTime} " +
                  s"setStopTime: ${RunEventEnd.runEndTime} rawDataState：${rawDataState.values().toList}")
                readOnlyContext.output(debugOutput, s"[processEnd] ==> input controlWindowId: $windowId" +
                  s" runId: $runId windowSnapshotInfo：${re}")
              }
            } catch {
              case ex: Exception =>
                readOnlyContext.output(debugOutput, ErrorCode("007006b005C", System.currentTimeMillis(), Map("debugWindow" ->
                  "", "controlWindowId" -> windowId, "runId" -> runId,"triggerType"->"processEnd"), ExceptionInfo.getExceptionInfo(ex)).toJson)
            }

            try {
              if(logStatus){
                readOnlyContext.output(mainFabLogInfoOutput,
                  generateMainFabLogInfo(
                    "002003b001D",
                    "windowEnd rawDataList",
                    s"$runId end step1",
                    Map[String,Any]("rawDataList" -> rawDataList,
                      "timeStamp" -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
                  ))
                logStatus = false
              }
              val startTime = System.currentTimeMillis()

              val windowEndConfig = windowEndConfigMap(s"${sensorPartitionID}|${contextId.toString}")(windowId)

              //构建dataflow
              val dataflow: IDataFlow = buildSensorDataFlowData(rawDataList)

              //设定run的开始时间结束时间
              dataflow.setStartTime(runStartTime)
              dataflow.setStopTime(RunEventEnd.runEndTime)

              realTimeWindow.attachDataFlow(dataflow)

              //计算window
              val result: List[WindowData] = ApiControlWindow.calculate(realTimeWindow, true).toList

              if (debugConfig.contains(windowId)) {
                readOnlyContext.output(debugOutput, s"[processEnd] ==> input controlWindowId: $windowId " +
                  s"runId: $runId results：${result}")
              }
              //切分出来就输出
              if (result != null) {
                val endTime = System.currentTimeMillis()
                if ((endTime - startTime) > 5000) {
                  logger.warn(s"window end切窗口event end耗时 状态：成功 耗时: ${endTime - startTime} " +
                    s"traceId:${RunEventEnd.traceId} windowId：$windowId contextKey: $contextKey")
                }

                val resultWindowDataList: ListBuffer[WindowData] = result.filter(elem => {
                  elem.windowId == windowId
                }).to[ListBuffer]

                if (resultWindowDataList.nonEmpty) {
                  traceIdWindowState.add(windowId)
                  val windowStartTime = resultWindowDataList.head.startTime
                  val windowEndTime = resultWindowDataList.head.stopTime

                  // 组装window返回格式
                  val windowData = getFdcWindowData(runStartEvent, contextId.toLong, windowEndConfig, contextOne,
                    runId, windowStartTime, windowEndTime, resultWindowDataList.toList)
                  out.collect(windowData)

                  // cycle window 处理
                  calcCycleCountIndicator(contextKey,
                    readOnlyContext,
                    contextOne,
                    windowId,
                    windowEndConfig,
                    runStartEvent,
                    runId,
                    resultWindowDataList,
                    windowStartTime,
                    windowEndTime)
                } else {
                  readOnlyContext.output(mainFabLogInfoOutput,
                    generateMainFabLogInfo(
                      "002003b001D",
                      "windowEnd resultWindowDataList.nonEmpty",
                      s"controlWindowId $windowId 划窗口结果中没有当前windowId == $windowId 的数据",
                      Map[String,Any]("runId End:" -> runId,
                        "timeStamp" -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
                    ))
                }
              }
            } catch {
              case exception: Exception =>
                logger.error(ErrorCode("002003b015C", System.currentTimeMillis(), Map("runId" ->
                  data.get(MainFabConstants.traceId).asText(), "windowId" -> windowId,
                  "rawData" -> data, "realTimeWindow" -> realTimeWindow), ExceptionInfo.getExceptionInfo(exception)).toJson)
            }
          }
        }
      }
    }catch {
      case exception: Exception =>
        logger.error(ErrorCode("002003b015C", System.currentTimeMillis(), Map("runId" ->
          data.get(MainFabConstants.traceId).asText(), "function" -> "runEndCalculatedWindowFunction"),
          ExceptionInfo.getExceptionInfo(exception)).toJson)
    }
  }




  /**
   *   功能: rawData试切窗口
   */
  def rawDataCalculatedWindowFunction(contextId: Long,
                                      contextOne: ContextConfigData,
                                      key: String,
                                      dataType: String,
                                      contextKey: String,
                                      sensorPartitionID: String,
                                      data: JsonNode,
                                      readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode), JsonNode,
                                        fdcWindowData]#ReadOnlyContext,
                                      out: Collector[fdcWindowData]): Unit = {
    val addStepRawData = toBean[MainFabRawData](data)

    val rawData = addStepRawData
    // 如果重启可以不丢
    if(!containerWindowMap.contains(key)){
      createRealTimeWindow(addStepRawData.traceId, contextId, sensorPartitionID, readOnlyContext)
    }

    if (containerWindowMap.contains(key)) {

      val realTimeWindowList = containerWindowMap(key)
      val rawDataList = rawDataState.values().toList
      if(rawDataList.nonEmpty) {

        val finishWindowIdList = traceIdWindowState.get.toList
        for ((windowId, realTimeWindow) <- realTimeWindowList) {
          // 判断是否已经计算完成
          if (!finishWindowIdList.contains(windowId) && realTimeWindow != null) {
            try {
              val startTime = System.currentTimeMillis()

              val windowEndConfig = windowEndConfigMap(s"${sensorPartitionID}|${contextId.toString}")(windowId)

              //构建dataflow
              val dataflow: IDataFlow = buildSensorDataFlowData(rawDataList)

              realTimeWindow.attachDataFlow(dataflow)

              //计算window
              val result: List[WindowData] = ApiControlWindow.calculate(realTimeWindow, false).toList


              //切分出来就输出
              if (result != null) {
                val endTime = System.currentTimeMillis()
                if ((endTime - startTime) > 5000) {
                  logger.warn(s"window end切窗口event end耗时 状态：成功 耗时: ${endTime - startTime} traceId:${rawData.traceId}" +
                    s" windowId：$windowId contextKey: $contextKey")
                }

                val runStartEvent = traceIdRecipeStateMap.get(rawData.traceId)

                val runId = s"${runStartEvent.toolName}--${runStartEvent.chamberName}--${runStartEvent.runStartTime}"

                val resultWindowDataList: ListBuffer[WindowData] = result.filter(elem => {
                  elem.windowId == windowId
                }).to[ListBuffer]

                if (resultWindowDataList.nonEmpty) {
                  traceIdWindowState.add(windowId)
                  val windowStartTime = resultWindowDataList.head.startTime
                  val windowEndTime = resultWindowDataList.head.stopTime

                  // 组装window返回格式
                  val windowData = getFdcWindowData(runStartEvent, contextId.toLong, windowEndConfig, contextOne,
                    runId, windowStartTime, windowEndTime, resultWindowDataList.toList)
                  out.collect(windowData)

                  //todo 新增cycle window需求
                  calcCycleCountIndicator(contextKey,
                    readOnlyContext,
                    contextOne,
                    windowId,
                    windowEndConfig,
                    runStartEvent,
                    runId,
                    resultWindowDataList,
                    windowStartTime,
                    windowEndTime)
                }
              }
            } catch {
              case exception: Exception =>
                logger.error(ErrorCode("002003b015C", System.currentTimeMillis(), Map("runId" ->
                  data.get(MainFabConstants.traceId).asText(), "windowId" -> windowId), ExceptionInfo.getExceptionInfo(exception)).toJson)
            }
          }
        }
      }
    }
  }


  /**
   * 计算 cycleCount
   */
  def calcCycleCountIndicator(contextKey: String,
                              readOnlyContext: KeyedBroadcastProcessFunction[String, (String, JsonNode, JsonNode),
                                JsonNode, fdcWindowData]#ReadOnlyContext,
                              contextOne: ContextConfigData,
                              windowId: Long,
                              windowConfig: WindowConfigData,
                              runStartEvent: RunEventData,
                              runId: String,
                              resultWindowDataList: ListBuffer[WindowData],
                              windowStartTime: Long,
                              windowEndTime: Long) = {

    if (windowConfig.controlWindowType == MainFabConstants.CycleWindowMaxType ||
      windowConfig.controlWindowType == MainFabConstants.CycleWindowMinType) {
      val cycleCount: Integer = resultWindowDataList.head.cycleUnitCount

      if (!indicatorConfigByCycleCount.contains(windowId)) {
        logger.warn(ErrorCode("002003b013C", System.currentTimeMillis(), Map("controlWindowId" -> windowId, "runId" -> runId), "WindowEnd中没有找到CycleCount indicator 配置").toJson)
      } else {
        val cycleCountIndicatorConfig: IndicatorConfig = indicatorConfigByCycleCount(windowId)

        if (true) {
          val productList = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "")
          val stageList = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "")

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
            missingRatio = runStartEvent.dataMissingRatio,
            configMissingRatio = cycleCountIndicatorConfig.missingRatio,
            runStartTime = runStartEvent.runStartTime,
            runEndTime = runStartEvent.runEndTime,
            windowStartTime = windowStartTime,
            windowEndTime = windowEndTime,
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

          val data: FdcData[IndicatorResult] = FdcData[IndicatorResult](
            "indicator",
            cycleCountIndicatorResult
          )

          readOnlyContext.output(cycleCountDataOutput, data)

        }
      }
    }
  }


  /**
   *  组装返回fdcWindowData
   */
  def getFdcWindowData(runStartEvent: RunEventData, contextId: Long, windowConfig: WindowConfigData,
                       contextOne: ContextConfigData, runId: String, windowStartTime: Long,
                       windowEndTime: Long, windowDataList: List[WindowData]): fdcWindowData = {
    val windowData = fdcWindowData("fdcWindowDatas",
      windowListData(toolName = runStartEvent.toolName,
        toolId = contextOne.toolId,
        chamberName = runStartEvent.chamberName,
        chamberId = contextOne.chamberId,
        recipeName = runStartEvent.recipeName,
        recipeId = contextOne.recipeId,
        runId = runId,
        dataMissingRatio = 0,
        contextId = contextId,
        productName = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else ""),
        stage = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else ""),
        controlWindowId = windowConfig.controlWindowId,
        controlPlanId = windowConfig.controlPlanId,
        controlPlanVersion = windowConfig.controlPlanVersion,
        missingRatio = 0,
        windowStart = windowConfig.windowStart,
        windowStartTime = windowStartTime,
        windowEnd = windowConfig.windowEnd,
        windowEndTime = windowEndTime,
        windowTimeRange = windowEndTime - windowStartTime,
        runStartTime = runStartEvent.runStartTime,
        runEndTime = 0,
        windowEndDataCreateTime = System.currentTimeMillis(),
        windowType= "windowEnd",
        DCType = runStartEvent.DCType,
        locationId = contextOne.locationId,
        locationName = contextOne.locationName,
        moduleId = contextOne.moduleId,
        moduleName = contextOne.moduleName,
        toolGroupId = contextOne.toolGroupId,
        toolGroupName = contextOne.toolGroupName,
        chamberGroupId = contextOne.chamberGroupId,
        chamberGroupName = contextOne.chamberGroupName,
        recipeGroupId = contextOne.recipeGroupId,
        recipeGroupName = contextOne.recipeGroupName,
        limitStatus = contextOne.limitStatus,
        materialName = runStartEvent.materialName,
        pmStatus = runStartEvent.pmStatus,
        pmTimestamp = runStartEvent.pmTimestamp,
        area = contextOne.area,
        section = contextOne.section,
        mesChamberName = contextOne.mesChamberName,
        lotMESInfo = runStartEvent.lotMESInfo,
        windowDatasList = windowDataList,
        dataVersion = runStartEvent.dataVersion)
    )
    windowData
  }

  /**
   *  组装rawData数据到切窗口
   */
  def buildSensorDataFlowData(rawdataList: List[windowRawData]): IDataFlow = {

    val flow = ApiControlWindow.buildDataFlow()

    try {
      if (rawdataList.nonEmpty) {
        for (one <- rawdataList) {
          try {
            val stepId = one.stepId.toInt
            val timestamp = one.timestamp

              val dp = ApiControlWindow.buildDataPacket()
              dp.setTimestamp(timestamp)
              dp.setStepId(stepId)

              for (sensor <- one.rawList) {
                  val s1 = ApiControlWindow.buildSensorData()
                  s1.setSensorName("")
                  s1.setStepId(stepId)
                  s1.setSensorAlias(sensor._1)
                  s1.setValue(sensor._2)
                  s1.setTimestamp(timestamp)
                  s1.setUnit(sensor._3)
                  dp.getSensorDataMap.put(s1.getSensorAlias, s1)
              }
              flow.getDataList.add(dp)
          } catch {
            case exception: Exception =>
              logger.error(ErrorCode("002003b015C", System.currentTimeMillis(), Map("one" -> one,
                "function" -> "buildSensorDataFlowData"), ExceptionInfo.getExceptionInfo(exception)).toJson)
          }
        }
      }
    }catch {
      case exception: Exception =>
        logger.error(ErrorCode("002003b015C", System.currentTimeMillis(), Map("rawdataList" -> rawdataList,
          "function" -> "buildSensorDataFlowData"), ExceptionInfo.getExceptionInfo(exception)).toJson)
    }
    flow
  }

  /**
   * 生成日志信息
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
