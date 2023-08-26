package com.hzw.fdc.function.online.MainFabWindow2

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.engine.api.{ApiControlWindow, EngineFunction, IControlWindow}
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResponse
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.{Marshallable, Unmarshallable}
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.MainFabConstants.IS_DEBUG
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisSentinelPool}

import java.util
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
import scala.collection.JavaConversions._

class MainFabProcessEndCalcWindowBroadcastProcessFunction() extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,  JsonNode] {


  /**
   * 025:MainFabProcessEndWindow2Service
   * 002:切窗口
   */
  val job_fun_DebugCode:String = "025002"
  val jobName:String = "MainFabProcessEndWindow2Service"
  val optionName : String = "MainFabProcessEndCalcWindowBroadcastProcessFunction"

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabProcessEndCalcWindowBroadcastProcessFunction])

  // 划窗口时的丢弃的数据 侧道输出
  lazy val calcWindowGiveUpRunStreamOutput = new OutputTag[JsonNode]("calcWindowGiveUpRunStream")

  // 特殊Indicator 的结果数据侧道输出
  lazy val specialIndicatorOutput = new OutputTag[FdcData[IndicatorResult]]("specialIndicatorOutput")

  // debug调试 的打印数据侧道输出
  lazy val debugOutput = new OutputTag[String]("debugTest")

  // 侧道输出日志信息
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")

  lazy val mainFabDebugInfoOutput = new OutputTag[MainFabDebugInfo]("mainFabDebugInfo")

  /**
   * 缓存windowConfig配置信息
   * 保留2个版本
   * key1:controlPlanId
   * key2:controlVersion
   * key3:windowId
   * value:Window2Data
   */
  val windowConfigMap = new TrieMap[Long,TrieMap[Long,TrieMap[Long,Window2Data]]]()

  /**
   * 缓存 controlPlan的最大版本号
   * key: controlPlanId
   * value controlPlanVersion
   */
  private val controlPlanMaxVersionMap = new TrieMap[Long,Long]()

  /**
   * 缓存CycleCount的配置
   * key: controlWindowId
   * value: IndicatorConfig
   */
  var indicatorConfigByCycleCount = new concurrent.TrieMap[Long, IndicatorConfig]()


  /**
   * 注意:
   *  1- DataMissingRatio Indicator 一个controlPlan只计算一次
   *  2- DataMissingRatio Indicator 属于allTime的window;
   * 缓存DataMissing 的配置
   * key: controlPlanId | windowId
   * value: IndicatorConfig
   */
  var indicatorConfigByDataMissing = new concurrent.TrieMap[String, IndicatorConfig]()


  /**
   * debug 指定的 windowId 切窗口时的详细信息
   * 当策略信息中 status == true 添加 windowId
   * 当策略信息中 status == false 删除 windowId
   */
  var debugCalcWindowList = new ListBuffer[Long]()

  /**
   * value: EventDataMatchedWindow
   */
  var processEndEventStartState:ValueState[EventDataMatchedWindow] = _

  /**
   * value: ListBuffer[stepId,timestamp,seq[(sensorAlias,sensorValue,unit)]]
   */
//  var rawDataListState: ListState[(Long, Long, Seq[(String, Double, String)])] = _
  // 必须使用 MapState key 使用的timestamp 存入数据的时候只需要 Put (timestamp,value)

  var rawDataMapState: MapState[Long,Window2RawData] = _

  /**
   * 如果配置了哪些errorCode , 这些errorCode 是不需要切窗口的
   */
  var passErrorCodeList = new ListBuffer[String]()

  //=========用于debug调试================
  // controlWindowId -> function
  var debugConfig = new concurrent.TrieMap[Long, mutable.Set[String]]()

  var jedisSentinelPool : JedisSentinelPool= _
//  var jedis :Jedis = _

  // 自定义metrics
  @transient private var calcWindowCostTime = 0l

  var subTaskId = -1

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 初始化 passErrorCodeList
    passErrorCodeList = ProjectConfig.MAINFAB_PASS_ERROR_CODES.split(",").to[ListBuffer]

    if(!IS_DEBUG){
      val startTime = System.currentTimeMillis()
      logger.warn(s"--- parse Hbase Config start time = ${startTime}" )

      // 初始化 Hbase中的 Window2Config信息
      val window2ConfigList: ListBuffer[ConfigData[Window2Config]] = if(ProjectConfig.MAINFAB_READ_HBASE_CONFIG_ONCE){
        InitFlinkFullConfigHbase.Window2ConfigList
      }else{
        readHbaseAllConfig[Window2Config](ProjectConfig.HBASE_SYNC_WINDOW2_TABLE, MainFabConstants.window2Config)
      }
      window2ConfigList.foreach(parseWindow2Config(_))

      // 初始化 Hbase中的indicatorConfig信息
      val indicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] = if(ProjectConfig.MAINFAB_READ_HBASE_CONFIG_ONCE){
        InitFlinkFullConfigHbase.IndicatorConfigList
      }else{
        readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)
      }

      // 初始化 dataMissingRatio 和 cycleCountIndicator
      for (indicatorConfig <- indicatorConfigList) {
        if (indicatorConfig.datas.algoClass == MainFabConstants.dataMissingRatio) {
          addDataMissingIndicatorToIndicatorConfigMap(indicatorConfig)
        } else if (indicatorConfig.datas.algoClass == MainFabConstants.cycleCountIndicator) {
          addCycleCountIndicatorToIndicatorConfigMap(indicatorConfig)
        }
      }

      val endTIme = System.currentTimeMillis()
      logger.warn(s"--- parse Hbase Config Total Time : ${endTIme} - ${startTime} = ${endTIme - startTime}")
    }

    // 26小时过期
    val hour26TTLConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    // 30分组过期
//    val min30TTLConfig:StateTtlConfig = StateTtlConfig
//      .newBuilder(Time.minutes(30L))
//      .useProcessingTime()
//      .updateTtlOnCreateAndWrite()
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//      // compact 过程中清理过期的状态数据
//      .cleanupInRocksdbCompactFilter(5000)
//      .build()

    // 初始化状态变量 processEndEventStartState
    val processEndEventStartStateDescription = new
        ValueStateDescriptor[EventDataMatchedWindow]("processEndEventStartState", TypeInformation.of(classOf[EventDataMatchedWindow]))
    // 设置过期时间
    processEndEventStartStateDescription.enableTimeToLive(hour26TTLConfig)
    processEndEventStartState = getRuntimeContext.getState(processEndEventStartStateDescription)

    // 初始化 rawDataMapState
    val rawDataMapStateDescription: MapStateDescriptor[Long,Window2RawData] = new
        MapStateDescriptor[Long,Window2RawData]("rawDataMapState", TypeInformation.of(classOf[Long]),TypeInformation.of(classOf[Window2RawData]))
    // 设置过期时间
    rawDataMapStateDescription.enableTimeToLive(hour26TTLConfig)
    rawDataMapState = getRuntimeContext.getMapState(rawDataMapStateDescription)


    // 获取Redis 的连接
    getRedisConnection()

    // 初始化自定义metrics
    getRuntimeContext()
      .getMetricGroup()
      .gauge[Long, ScalaGauge[Long]]("calaWindowCostTimeGauge", ScalaGauge[Long](() => calcWindowCostTime))

    // 获取 当前subTaskId
    subTaskId = getRuntimeContext.getIndexOfThisSubtask
  }

  override def processElement(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]): Unit = {
    try {
      val dataType = inputValue.get(MainFabConstants.dataType).asText()

      if(dataType == MainFabConstants.eventStart){

        // 处理 EventStart 数据
        processEventStart(inputValue, readOnlyContext, collector)

      } else if(dataType == MainFabConstants.eventEnd){

        // 处理 EventEnd 数据
        processEventEnd(inputValue, readOnlyContext, collector)

      }else if(dataType == MainFabConstants.rawData){

        // 处理 RawData 数据
        processRawData(inputValue, readOnlyContext, collector)
      }

    }catch {
      case ex: Exception => logger.error(ErrorCode("0250020004B",
        System.currentTimeMillis(),
        Map("msg" -> "处理业务数据失败"),
        ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 处理EventStart 数据
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processEventStart(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])= {
    try{

      val eventStartDataMatchedWindow = toBean[EventDataMatchedWindow](inputValue)
      processEndEventStartState.update(eventStartDataMatchedWindow)
    }catch {
      case ex: Exception => logger.error(ErrorCode("0250020005B",
        System.currentTimeMillis(),
        Map("msg" -> s"----处理 EventStart 数据 异常 inputValue == ${inputValue}"),
        ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 处理rawData 数据
   *
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processRawData(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])= {
    try{

      val rawDataMatchedWindow: RawDataMatchedWindow = toBean[RawDataMatchedWindow](inputValue)

//      val sensorInfoList: List[(String, Double, String)] = rawDataMatchedWindow.data // 1: sensorAlias, 2: sensorValue, 3: unit

      if(null != processEndEventStartState.value()){
        // 开始收集rawData 数据
        cacheRawData(rawDataMatchedWindow)

      }else{
        // eventStart 丢失 或者 rawData 早于 eventStart
        giveUpData(inputValue,readOnlyContext)
      }

    }catch {
      case ex: Exception => logger.error(ErrorCode("0250020006B",
        System.currentTimeMillis(),
        Map("msg" -> s"----处理 RawData 数据 异常 inputValue == ${inputValue}"),
        ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 缓存 所有的rawData 数据 (所有的sensor)
   * @param stateKey
   * @param stepId
   * @param timestamp
   * @param sensorDataList  // 1: sensorAlias, 2: sensorValue, 3: unit
   */
  def cacheRawData(rawDataMatchedWindow: RawDataMatchedWindow) = {
    val stepId = rawDataMatchedWindow.stepId
    val timestamp = rawDataMatchedWindow.timestamp
    val data: List[(String, Double, String)] = rawDataMatchedWindow.data

    val newWindowRawData = Window2RawData(stepId,timestamp,data)
    rawDataMapState.put(timestamp,newWindowRawData)

//    val newRawDataValue: (Long, Long, List[(String, Double, String)]) = (stepId,timestamp,data)
//    rawDataListState.add(newRawDataValue)
  }



  /**
   * 处理 EventEnd 数据
   *
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processEventEnd(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])= {

    try{
      val eventEndDataMatchedWindow = toBean[EventDataMatchedWindow](inputValue)
      val errorCode: String = eventEndDataMatchedWindow.errorCode.getOrElse(0).toString

      // 如果errorCode 属于 passErrorCodeList 就不需要切窗口
      // eventStart 正常才开始计算窗口
      if(null != processEndEventStartState.value() &&  !passErrorCodeList.contains(errorCode)){

        // eventEnd 数据来了再创建切window的对象（保证这个对象是线程安全的）
        val windowConfig_calcWidnowObject = createCalcWindowObject(readOnlyContext)
        val window2Data: Window2Data = windowConfig_calcWidnowObject._1
        val calcWindowObject: IControlWindow = windowConfig_calcWidnowObject._2

        if(null != window2Data && null != calcWindowObject){
          processEndCalcWindow(eventEndDataMatchedWindow,window2Data,calcWindowObject,readOnlyContext,collector)
        }

      }else{
        // eventStart 丢失 或者 eventEnd 早于 eventStart
        giveUpData(inputValue,readOnlyContext)
      }

    }catch {
      case ex: Exception => logger.error(ErrorCode("0250020007B",
        System.currentTimeMillis(),
        Map("msg" -> s"----处理 EventEnd 数据 异常 inputValue == ${inputValue}"),
        ExceptionInfo.getExceptionInfo(ex)).toJson)
    }finally {
      // 清空状态数据
      cleanAllState()
    }
  }

  /**
   *
   * @param matchedControlPlanId
   * @param controlPlanVersion
   */
  def createCalcWindowObject(readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    // 获取 eventStart
    val runEventStartDataMatchedWindow = processEndEventStartState.value()

    val matchedControlPlanId = runEventStartDataMatchedWindow.matchedControlPlanId
    val matchedWindowId = runEventStartDataMatchedWindow.matchedWindowId
    val controlPlanVersion = runEventStartDataMatchedWindow.controlPlanVersion
    val runId = s"${runEventStartDataMatchedWindow.toolName}--${runEventStartDataMatchedWindow.chamberName}--${runEventStartDataMatchedWindow.runStartTime}"

    if(windowConfigMap.contains(matchedControlPlanId)){
      val versionWindowConfigMap = windowConfigMap.getOrElse(matchedControlPlanId, new TrieMap[Long, TrieMap[Long, Window2Data]])

      // 当这里没有读到该版本的配置信息时,再从Redis获取
      val window2DataMap: TrieMap[Long, Window2Data] = versionWindowConfigMap.getOrElse(controlPlanVersion,getWindowConfigFromRedis(runId,matchedControlPlanId,controlPlanVersion,matchedWindowId,readOnlyContext))

      // 如果没有匹配到对应的版本， 就使用最新的版本
      val matchedWindowConfigMap = if(!window2DataMap.nonEmpty){
        versionWindowConfigMap.get(controlPlanMaxVersionMap.get(matchedControlPlanId).get).get
      }else{
        window2DataMap
      }

      val elemWindowConfig = matchedWindowConfigMap.getOrElse(matchedWindowId, null)

      if(null != elemWindowConfig){
        val containerIControlWindow = createContainerIControlWindow(elemWindowConfig,matchedWindowConfigMap.toMap)
        if(null != containerIControlWindow){
          (elemWindowConfig,containerIControlWindow)
        }else{
          logger.error(s"[processEnd] ==> 创建切窗口对象失败 -----0001C-----  \n " +
            s"controlPlanId == $matchedControlPlanId \n " +
            s"controlPlanVersion == ${controlPlanVersion} " +
            s"controlWindowId == $matchedWindowId ; \n" +
            s"runId == $runId ; ")

          // 该日志应该侧道输出到kafka ;
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0001C",
              "createCalcWindowObject",
              s"[processEnd] ==> 创建切窗口对象失败 -----0001C----- ",
              Map[String, Any]("controlPlanId" -> matchedControlPlanId,
                "controlPlanVersion" -> controlPlanVersion,
                "controlWindowId" -> matchedWindowId,
                "runId" -> runId,
                "elemWindowConfig" -> elemWindowConfig)
            ))

          (elemWindowConfig,null)
        }
      }else{
        logger.error(s"[processEnd] ==> 创建切窗口对象失败 -----0000C----- 没有该window的配置信息\n " +
          s"controlPlanId == $matchedControlPlanId \n " +
          s"controlPlanVersion == ${controlPlanVersion} " +
          s"controlWindowId == $matchedWindowId ; \n" +
          s"runId == $runId ; ")

        // 该日志应该侧道输出到kafka ;
        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0000C",
            "createCalcWindowObject",
            s"[processEnd] ==> 创建切窗口对象失败 -----0000C----- 没有该window的配置信息",
            Map[String, Any]("controlPlanId" -> matchedControlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "controlWindowId" -> matchedWindowId,
              "runId" -> runId,
              "elemWindowConfig" -> elemWindowConfig)
          ))
        (null,null)
      }
    }else {
      logger.error(s"[processEnd] ==> 创建切窗口对象失败 -----0000B----- 匹配到的controlPlanId 没有对应的window配置信息\n " +
        s"controlPlanId == $matchedControlPlanId \n " +
        s"controlPlanVersion == ${controlPlanVersion} " +
        s"controlWindowId == $matchedWindowId ; \n" +
        s"runId == $runId ; ")

      readOnlyContext.output(mainFabLogInfoOutput,
        generateMainFabLogInfo("0000B",
          "createCalcWindowObject",
          s"[processEnd] ==> 创建切窗口对象失败 -----0000B----- 匹配到的controlPlanId 没有对应的window配置信息",
          Map[String, Any]("controlPlanId" -> matchedControlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "controlWindowId" -> matchedWindowId,
            "runId" -> runId)))

      (null,null)
    }

  }

  /**
   * 长Run 划窗口
   *
   * @param eventEndDataMatchedWindow
   * @param readOnlyContext
   * @param collector
   */
  def processEndCalcWindow(eventEndDataMatchedWindow: EventDataMatchedWindow,
                           window2Data: Window2Data,
                           calcWindowObject: IControlWindow,
                           readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                           collector: Collector[JsonNode]) = {

    val matchedControlPlanId = eventEndDataMatchedWindow.matchedControlPlanId
    val matchedWindowId = eventEndDataMatchedWindow.matchedWindowId
    val windowPartitionId = eventEndDataMatchedWindow.windowPartitionId

    // 获取 eventStart  和  controlPlanConfig
    val eventStartDataMatchedWindow: EventDataMatchedWindow = processEndEventStartState.value()
    val runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}"

    // parseRawData
    val rawDataSensorInfoTuple = parseRawData(runId, window2Data,readOnlyContext)
    val clipWindowByAllSensorDataValueList = rawDataSensorInfoTuple._1
    val rawDataSensorAliasMap = rawDataSensorInfoTuple._2

    if (clipWindowByAllSensorDataValueList.nonEmpty && !rawDataSensorAliasMap.isEmpty) {
      // 构建dataflow
      val dataflow: IDataFlow = EngineFunction.buildWindow2DataFlowData(clipWindowByAllSensorDataValueList)
      dataflow.setStartTime(eventStartDataMatchedWindow.runStartTime)
      dataflow.setStopTime(eventEndDataMatchedWindow.runEndTime)
      calcWindowObject.attachDataFlow(dataflow)

      // 计算window
      val calcWindowStartTime: Long = DateTimeUtil.getCurrentTimestamp // 开始划window
      val apiControlWindow = new ApiControlWindow()
      val clipWindowResult: ClipWindowResult = apiControlWindow.split(calcWindowObject, true)
      val calcWindowEndTime: Long = DateTimeUtil.getCurrentTimestamp // 结束划window
      calcWindowCostTime = calcWindowEndTime - calcWindowStartTime

      // 侧道输出日志--切窗口花的时长
      readOnlyContext.output(mainFabLogInfoOutput,
        generateMainFabLogInfo("0001A",
          "calculateWindows",
          s"切窗口时长",
          Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
            "controlWindowId" -> matchedWindowId,
            "windowPartitionId" -> windowPartitionId,
            "windowType" -> MainFabConstants.windowTypeProcessEnd ,
            "runId" -> runId,
            "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
            "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
            "subTaskId" -> subTaskId,
            "clipWindowResult" -> clipWindowResult,
            "calcWindowCostTime" -> (calcWindowEndTime - calcWindowStartTime))
        ))

      // debug 调试信息
      if(debugCalcWindowList.contains(matchedWindowId)){
        readOnlyContext.output(mainFabDebugInfoOutput,
          generateMainFabDebugInfo(Map[String, Any]("matchedControlPlanId"->matchedControlPlanId,
            "matchedWindowId" -> matchedWindowId,
            "windowPartitionId" -> windowPartitionId,
            "windowType" -> MainFabConstants.windowTypeProcessEnd ,
            "runId" -> runId,
            "window2DataPartition" -> window2Data,
            "clipWindowByAllSensorDataValueList" -> clipWindowByAllSensorDataValueList,
            "rawDataSensorAliasMap" -> rawDataSensorAliasMap,
            "clipWindowResult" -> clipWindowResult))
        )
      }

      if (null != clipWindowResult &&
        WindowClipResponse.SUCCESS_CODE.equals(clipWindowResult.msgCode) &&
        null != clipWindowResult.windowTimeRangeList) {

        val windowTimeRangeList = clipWindowResult.windowTimeRangeList

        // 只要当前windowId 的结果数据 (用于父子窗口: 切子窗口时会把父窗口的结果一起返回)
        val windowTimeRangeCurrentWindowIdList: List[ClipWindowTimestampInfo] = windowTimeRangeList.filter(windowTimeRangeResult => {
          windowTimeRangeResult.windowId == matchedWindowId
        })

        if (windowTimeRangeCurrentWindowIdList.nonEmpty) {

          val collectDataStart = System.currentTimeMillis()
          // 收集processEnd  window 结果数据
          collectWindowResultData(eventStartDataMatchedWindow,
            eventEndDataMatchedWindow,
            window2Data,
            windowTimeRangeCurrentWindowIdList,
            rawDataSensorAliasMap,
            readOnlyContext,
            collector)
          val collectDataEnd = System.currentTimeMillis()

          // 侧道输出日志--切窗口花的时长
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0002A",
              "calculateWindows",
              s"卡数据时长",
              Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
                "controlWindowId" -> matchedWindowId,
                "windowPartitionId" -> windowPartitionId,
                "windowType" -> MainFabConstants.windowTypeProcessEnd ,
                "runId" -> runId,
                "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
                "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
                "subTaskId" -> subTaskId,
                "collectDataCostTime" -> (collectDataEnd-collectDataStart) )
            ))


          // 计算cycleCount
          calcCycleCount(eventEndDataMatchedWindow,
            window2Data,
            windowTimeRangeCurrentWindowIdList,
            readOnlyContext)
        } else {
          logger.error(s"[processEnd] ==> 划窗口异常 -----0005C----- 划窗口结果异常:当前 WindowId 没有窗口结果数据 \n " +
            s"matchedControlPlanId == ${matchedControlPlanId} \n" +
            s"controlWindowId == $matchedWindowId ; \n" +
            s"windowPartitionId == $windowPartitionId ; \n " +
            s"windowType == ${MainFabConstants.windowTypeProcessEnd} ; \n" +
            s"runId == $runId ; ")

          // 该日志应该侧道输出到kafka ;
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0005C",
              "calculateWindows",
              s"[processEnd] ==> 划窗口异常 -----0005C----- 划窗口结果异常:当前 WindowId 没有窗口结果数据 ",
              Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
                "controlWindowId" -> matchedWindowId,
                "windowPartitionId" -> windowPartitionId,
                "windowType" -> MainFabConstants.windowTypeProcessEnd ,
                "runId" -> runId,
                "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
                "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
                "clipWindowResult" -> clipWindowResult)
            ))
        }
      } else {
        logger.error(s"[processEnd] ==> 划窗口异常 -----0004C----- 划窗口结果异常，请查看mes信息 \n" +
          s"matchedControlPlanId == ${matchedControlPlanId} \n" +
          s"controlWindowId == $matchedWindowId ; \n" +
          s"windowPartitionId == $windowPartitionId ; \n" +
          s"windowType == ${MainFabConstants.windowTypeProcessEnd} ; \n" +
          s"runId == $runId ; ")

        // 该日志应该侧道输出到kafka ;
        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0004C",
            "calculateWindows",
            s"[processEnd] ==> 划窗口异常 -----0004C----- 划窗口结果异常，请查看mes信息 ",
            Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
              "controlWindowId" -> matchedWindowId,
              "windowPartitionId" -> windowPartitionId,
              "windowType" -> MainFabConstants.windowTypeProcessEnd ,
              "runId" -> runId,
              "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
              "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
              "clipWindowResult" -> clipWindowResult)
          ))
      }

    } else {
      logger.error(s"[processEnd] ==> 划窗口异常 -----0003C----- sensor信息有异常" +
        s"matchedControlPlanId == ${matchedControlPlanId} \n" +
        s"controlWindowId == $matchedWindowId ;\n " +
        s"windowPartitionId == $windowPartitionId ;\n " +
        s"windowType == ${MainFabConstants.windowTypeProcessEnd} ; \n" +
        s"runId == $runId ; ")

      // 该日志应该侧道输出到kafka ;
      readOnlyContext.output(mainFabLogInfoOutput,
        generateMainFabLogInfo("0003C",
          "calculateWindows",
          s"[processEnd] ==> 划窗口异常 -----0003C----- sensor信息有异常",
          Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
            "controlWindowId" -> matchedWindowId,
            "windowPartitionId" -> windowPartitionId,
            "windowType" -> MainFabConstants.windowTypeProcessEnd ,
            "runId" -> runId,
            "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
            "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
            "rawDataSensorAliasMap" -> rawDataSensorAliasMap)
        ))
    }

    // 计算DataMissingRatio
    calcDataMissingRatio(eventEndDataMatchedWindow, window2Data, readOnlyContext)

    // 清理内存
    clipWindowByAllSensorDataValueList.clear()
    rawDataSensorAliasMap.clear()
  }

  /**
   *
   * @param WindowData
   * @return
   */
  def parseRawData(runId:String,
                   windowConfigData: Window2Data,
                   readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val splitWindowByAllSensorList: List[String] = if (null != windowConfigData) {
      val extraSensorAliasNameList = windowConfigData.extraSensorAliasNameList
      val splitWindowBy = windowConfigData.splitWindowBy.split("####").toList
      extraSensorAliasNameList ++ splitWindowBy
    } else {
      List()
    }

//    logger.warn(s"splitWindowByAllSensorList == ${splitWindowByAllSensorList}")

    // 缓存 切window 所需要的sensor List
    val allRawDataSplitWindowBySensorDataValueList = new ListBuffer[(Long, Long, Seq[(String, Double, String)])]

    // 缓存 切window 所需要的备用sensor List
    val allRawDataSplitWindowByBackupSensorDataValueList = new ListBuffer[(Long, Long, Seq[(String, Double, String)])]
    // 切窗口时的备用sensor : 取第一个rawData数据中的第一个sensor 作为切窗口的备用sensor;
    var splitBackupSensor = ""

    // 以sensorName 为单位，缓存每个sensor 的集合
    val sensorAliasMap = new util.HashMap[String,ListBuffer[RawDataSensorData]]()
    val it = rawDataMapState.values().iterator()
    while(it.hasNext){
      val windowRawData = it.next()
      val stepId = windowRawData.stepId
      val timestamp = windowRawData.timestamp
      val sensorList = windowRawData.sensorList
      val splitSensorValueData = new ListBuffer[(String, Double, String)]

      // 收集切窗口时的备用sensor;
      val splitBackupSensorValueData = new ListBuffer[(String, Double, String)]

      sensorList.foreach(elemSensor =>{
        val sensorAliasName = elemSensor._1
        val sensorValue = elemSensor._2
        val unit = elemSensor._3
        if(splitBackupSensor.isEmpty){
          splitBackupSensor = sensorAliasName
        }

        // 收集切window 所需要的所有sensor;
        if(splitWindowByAllSensorList.contains(sensorAliasName)){
          splitSensorValueData.append(elemSensor)
        }

        if(splitBackupSensor == sensorAliasName){
          splitBackupSensorValueData.append(elemSensor)
        }

        // 收集每个sensor 的集合
        val newSensorData = RawDataSensorData(sensorValue, timestamp, stepId,unit)
        val cacheSensorDataList: ListBuffer[RawDataSensorData] = sensorAliasMap.getOrDefault(sensorAliasName, new ListBuffer[RawDataSensorData])
        cacheSensorDataList.append(newSensorData)
        sensorAliasMap.put(sensorAliasName,cacheSensorDataList)
      })

      if(splitSensorValueData.nonEmpty){
        val elemRawDataSplitWindowBySensorDataValue: (Long, Long, List[(String, Double, String)]) = (stepId, timestamp, splitSensorValueData.toList)
        allRawDataSplitWindowBySensorDataValueList.append(elemRawDataSplitWindowBySensorDataValue)
      }

      if(splitBackupSensorValueData.nonEmpty){
        val elemRawDataSplitWindowByBackupSensorDataValue: (Long, Long, List[(String, Double, String)]) = (stepId, timestamp, splitBackupSensorValueData.toList)
        allRawDataSplitWindowByBackupSensorDataValueList.append(elemRawDataSplitWindowByBackupSensorDataValue)
      }
    }

    if(allRawDataSplitWindowBySensorDataValueList.nonEmpty){
      (allRawDataSplitWindowBySensorDataValueList,sensorAliasMap)
    }else{

      logger.warn(s"当前Run没有StepId Sensor,使用备用sensor去切窗口;\n " +
        s"runId == ${runId} ; \n " +
        s"splitBackupSensor == ${splitBackupSensor}")

      // 侧道输出日志--切窗口花的时长
      readOnlyContext.output(mainFabLogInfoOutput,
        generateMainFabLogInfo("0007C",
          "parseRawData",
          s"当前Run没有StepId Sensor,使用备用sensor去切窗口",
          Map[String, Any]("windowType" -> MainFabConstants.windowTypeProcessEnd ,
            "runId" -> runId,
            "splitBackupSensor" -> splitBackupSensor,
            "windowConfigData" -> windowConfigData)))

      (allRawDataSplitWindowByBackupSensorDataValueList,sensorAliasMap)
    }

  }


  /**
   *   组装window结果数据 并输出
   *   以 sensorAlias为单位 输出结果
   * @param eventStartDataMatchedWindow
   * @param eventEndDataMatchedWindow
   * @param window2Data
   * @param windowTimeRangeResultList
   * @param rawDataSensorAliasMap
   * @param stateKey
   * @param readOnlyContext
   * @param collector
   */
  def collectWindowResultData(eventStartDataMatchedWindow: EventDataMatchedWindow,
                              eventEndDataMatchedWindow: EventDataMatchedWindow,
                              window2Data: Window2Data,
                              windowTimeRangeResultList: List[ClipWindowTimestampInfo],
                              rawDataSensorAliasMap: util.HashMap[String, ListBuffer[RawDataSensorData]],
                              readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                              collector: Collector[JsonNode]) = {

//    val startTime = System.currentTimeMillis()
    val runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}"

    val windowPartitionId = eventStartDataMatchedWindow.windowPartitionId
    val windowSensorInfoList: List[WindowSensorInfo] = window2Data.windowSensorInfoList
//    val windowPartitionAllSensorList = window2Data.sensorAliasNameListMap.getOrElse(windowPartitionId,List[String]()).distinct
    val windowAllSensorList = window2Data.sensorAliasNameList

    windowSensorInfoList.foreach(windowSensorInfo =>{
      val sensorAliasName = windowSensorInfo.sensorAliasName

      if(windowAllSensorList.contains(sensorAliasName)){
        val indicatorIdList = windowSensorInfo.indicatorIdList

        val cycleUnitCount:Int = windowTimeRangeResultList.size
        var cycleIndex:Int = 0

        val windowDataList : ListBuffer[WindowData] = new ListBuffer[WindowData]
        windowTimeRangeResultList.foreach(windowTimeRange => {
          val startTime = windowTimeRange.startTime
          val startInclude = windowTimeRange.startInclude
          val endTime = windowTimeRange.endTime
          val endInclude = windowTimeRange.endInclude
          val sensorDataList: ListBuffer[RawDataSensorData] = rawDataSensorAliasMap.getOrDefault(sensorAliasName, new ListBuffer[RawDataSensorData])

          // 根据切出来的窗口 起始和结束时间 过滤sensor的数据
          val rawDataSensorDataListResult = sensorDataList.filter(rawDataSensorData => {
            val timestamp = rawDataSensorData.timestamp

            // 根据 startInclude endInclude 判断是否包含起始时间或者结束时间
            if(startInclude && endInclude){
              startTime <= timestamp && timestamp <= endTime
            }else if(startInclude && !endInclude){
              startTime <= timestamp && timestamp < endTime
            }else if(!startInclude && endInclude){
              startTime < timestamp && timestamp <= endTime
            }else{
              startTime < timestamp && timestamp < endTime
            }
          })

          if(rawDataSensorDataListResult.nonEmpty){

            if(window2Data.controlWindowType == MainFabConstants.CycleWindowMaxType ||
              window2Data.controlWindowType == MainFabConstants.CycleWindowMinType){
              // 如果是cycle Window cycleIndix 从1 开始计数
              cycleIndex += 1
            }else{
              // 如果不是cycle Window cycleIndix = -1
              cycleIndex = -1
            }

            // 组装 WindowData 数据
            val windowData: WindowData = generateWindowData(window2Data,
              sensorAliasName,
              indicatorIdList,
              cycleUnitCount,
              cycleIndex,
              startTime,
              endTime,
              rawDataSensorDataListResult)
            windowDataList.append(windowData)
          }
        })

        // 组装结果数据
        if(windowDataList.nonEmpty){
          val windowData: fdcWindowData = generateFdcWindowData(eventStartDataMatchedWindow,
            eventEndDataMatchedWindow,
            window2Data,
            windowDataList.toList)

          collector.collect(beanToJsonNode[fdcWindowData](windowData))
        }else{

          // todo 目前下面的打印日志太多， 先去掉该日志输出看看日志量是否下降
//          logger.error(s"[processEnd] ==> windowDataList 为空 -----0006C----- ; \n " +
//            s"runId == ${runId} ; \n" +
//            s"matchedControlPlanId == ${eventStartDataMatchedWindow.matchedControlPlanId} ; \n" +
//            s"matchedWindowId == ${eventStartDataMatchedWindow.matchedWindowId} ; \n" +
//            s"windowPartitionId == ${windowPartitionId} ; \n" +
//            s"sensorAliasName == ${sensorAliasName}  " )
//
//          // 该日志应该侧道输出到kafka ;
//          readOnlyContext.output(mainFabLogInfoOutput,
//            generateMainFabLogInfo("0006C",
//              "calculateWindows",
//              s"[processEnd] ==> windowDataList 为空 -----0006C----- ;",
//              Map[String,Any]("runId" -> runId,
//                "matchedControlPlanId" -> eventStartDataMatchedWindow.matchedControlPlanId,
//                "matchedWindowId" -> eventStartDataMatchedWindow.matchedWindowId,
//                "windowPartitionId" -> windowPartitionId,
//                "windowType" -> MainFabConstants.windowTypeProcessEnd ,
//                "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
//                "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
//                "sensorAliasName" -> sensorAliasName ,
//                "windowDataList" -> windowDataList,
//                "eventEndDataMatchedWindow" -> eventEndDataMatchedWindow,
//                "window2Data" -> window2Data,
//                "windowTimeRangeResultList" -> windowTimeRangeResultList)
//            ))
        }
      }
    })

//    val endTime = System.currentTimeMillis()
//    val collectSensorDataCastTime = endTime - startTime
//    // 侧道输出日志--卡数据时长
//    readOnlyContext.output(mainFabLogInfoOutput,
//      generateMainFabLogInfo("0002A",
//        "calculateWindows",
//        s"该window下所有sensor卡数据时长",
//        Map[String, Any]("matchedControlPlanId" -> eventStartDataMatchedWindow.matchedControlPlanId,
//          "controlWindowId" -> eventStartDataMatchedWindow.matchedWindowId,
//          "windowPartitionId" -> windowPartitionId,
//          "windowType" -> MainFabConstants.windowTypeProcessEnd ,
//          "runId" -> runId,
//          "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
//          "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
//          "collectSensorDataCastTime" -> collectSensorDataCastTime)
//      ))
  }

  /**
   * 计算cycleCount
   * @param eventEndDataMatchedWindow
   * @param window2Data
   * @param windowTimeRangeCurrentWindowIdList
   * @param eventStartMapStateKey
   * @param readOnlyContext
   */
  def calcCycleCount(eventEndDataMatchedWindow: EventDataMatchedWindow,
                     window2Data: Window2Data,
                     windowTimeRangeCurrentWindowIdList: List[ClipWindowTimestampInfo],
                     readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val traceId = eventEndDataMatchedWindow.traceId
    val runId = eventEndDataMatchedWindow.runId
    val matchedWindowId = eventEndDataMatchedWindow.matchedWindowId
    val windowPartitionId = eventEndDataMatchedWindow.windowPartitionId

    // 注意: window根据sensor个数分区后,每个分区都会计算一次cycleCount。 为了不重复计算统一回去分区Id == partition_0 的值;
    if(windowTimeRangeCurrentWindowIdList.nonEmpty  &&
      "partition_0".equalsIgnoreCase(windowPartitionId) &&
      (window2Data.controlWindowType == MainFabConstants.CycleWindowMaxType ||
      window2Data.controlWindowType == MainFabConstants.CycleWindowMinType)){

      if (!indicatorConfigByCycleCount.contains(matchedWindowId)) {
        logger.warn(ErrorCode("0250020008B", System.currentTimeMillis(), Map("matchedWindowId" -> matchedWindowId, "traceId" -> traceId,"runId" -> runId), "没有找到CycleCount indicator 配置").toJson)
        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0008B",
            "calculateWindows",
            s"[processEnd] ==> -----0008B----- 没有找到CycleCount indicator 配置 ",
            Map[String, Any]("controlWindowId" -> matchedWindowId,
              "windowPartitionId" -> windowPartitionId,
              "windowType" -> MainFabConstants.windowTypeProcessEnd ,
              "runId" -> runId,
              "subTaskId" -> subTaskId ,
              "traceId" -> traceId)))
      }else{

        val cycleCount: Int = windowTimeRangeCurrentWindowIdList.size
        val eventStartDataMatchedWindow: EventDataMatchedWindow = processEndEventStartState.value()
        val matchedControlPlanConfig = eventStartDataMatchedWindow.matchedControlPlanConfig

        val runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}"

        val cycleCountIndicatorConfig = indicatorConfigByCycleCount(matchedWindowId)
        val productList = eventEndDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "")
        val stageList = eventEndDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "")

        val cycleCountIndicatorResult = IndicatorResult(
          controlPlanId = cycleCountIndicatorConfig.controlPlanId,
          controlPlanName = cycleCountIndicatorConfig.controlPlanName,
          controlPlanVersion = cycleCountIndicatorConfig.controlPlanVersion,
          locationId = matchedControlPlanConfig.locationId,
          locationName = matchedControlPlanConfig.locationName,
          moduleId = matchedControlPlanConfig.moduleId,
          moduleName = matchedControlPlanConfig.moduleName,
          toolGroupId = matchedControlPlanConfig.toolGroupId,
          toolGroupName = matchedControlPlanConfig.toolGroupName,
          chamberGroupId = matchedControlPlanConfig.chamberGroupId,
          chamberGroupName = matchedControlPlanConfig.chamberGroupName,
          recipeGroupName = matchedControlPlanConfig.recipeGroupName,
          runId = runId,
          toolName = eventStartDataMatchedWindow.toolName,
          toolId = matchedControlPlanConfig.toolId,
          chamberName = eventStartDataMatchedWindow.chamberName,
          chamberId = matchedControlPlanConfig.chamberId,
          indicatorValue = cycleCount.toString,
          indicatorId = cycleCountIndicatorConfig.indicatorId,
          indicatorName = cycleCountIndicatorConfig.indicatorName,
          algoClass = cycleCountIndicatorConfig.algoClass,
          indicatorCreateTime = System.currentTimeMillis(),
          missingRatio = eventEndDataMatchedWindow.dataMissingRatio,
          configMissingRatio = cycleCountIndicatorConfig.missingRatio,
          runStartTime = eventStartDataMatchedWindow.runStartTime,
          runEndTime = eventEndDataMatchedWindow.runEndTime,
          windowStartTime = eventStartDataMatchedWindow.runStartTime,
          windowEndTime = eventEndDataMatchedWindow.runEndTime,
          windowDataCreateTime = System.currentTimeMillis(),
          limitStatus = matchedControlPlanConfig.limitStatus,
          materialName = eventStartDataMatchedWindow.materialName,
          recipeName = eventStartDataMatchedWindow.recipeName,
          recipeId = matchedControlPlanConfig.recipeId,
          product = productList,
          stage = stageList,
          bypassCondition = cycleCountIndicatorConfig.bypassCondition,
          pmStatus = eventStartDataMatchedWindow.pmStatus,
          pmTimestamp = eventStartDataMatchedWindow.pmTimestamp,
          area = matchedControlPlanConfig.area,
          section = matchedControlPlanConfig.section,
          mesChamberName = matchedControlPlanConfig.mesChamberName,
          lotMESInfo = eventStartDataMatchedWindow.lotMESInfo,
          unit = "",
          dataVersion = eventStartDataMatchedWindow.dataVersion,
          cycleIndex = "0"
        )

        val data = FdcData[IndicatorResult](
          "indicator",
          cycleCountIndicatorResult
        )
        readOnlyContext.output(specialIndicatorOutput, data)
      }
    }
  }

  /**
   * 计算 DataMissingRatio Indicator
   * @param eventEndDataMatchedWindow
   * @param window2Data
   * @param eventStartMapStateKey
   * @param readOnlyContext
   */
  def calcDataMissingRatio(eventEndDataMatchedWindow: EventDataMatchedWindow,
                           window2Data: Window2Data,
                           readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {
    val eventStartDataMatchedWindow: EventDataMatchedWindow = processEndEventStartState.value()
    val matchedControlPlanConfig = eventStartDataMatchedWindow.matchedControlPlanConfig
    val controlPlanId = matchedControlPlanConfig.controlPlanId
    val windowPartitionId = eventEndDataMatchedWindow.windowPartitionId
    val windowId = window2Data.controlWindowId
    val dataMissingKey = controlPlanId.toString + "_" + windowId

    // 注意: window根据sensor个数分区后,每个分区都会计算一次DataMissing。 为了不重复计算统一回去分区Id == partition_0 的值;
    if(indicatorConfigByDataMissing.contains(dataMissingKey)){
      val dataMissingConfig = indicatorConfigByDataMissing.get(dataMissingKey).get

      val runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}"

      val productList = eventEndDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "")
      val stageList = eventEndDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "")

      val dataMissingIndicatorResult = IndicatorResult(controlPlanId = dataMissingConfig.controlPlanId,
        controlPlanName = dataMissingConfig.controlPlanName,
        controlPlanVersion = dataMissingConfig.controlPlanVersion,
        locationId = matchedControlPlanConfig.locationId,
        locationName = matchedControlPlanConfig.locationName,
        moduleId = matchedControlPlanConfig.moduleId,
        moduleName = matchedControlPlanConfig.moduleName,
        toolGroupId = matchedControlPlanConfig.toolGroupId,
        toolGroupName = matchedControlPlanConfig.toolGroupName,
        chamberGroupId = matchedControlPlanConfig.chamberGroupId,
        chamberGroupName = matchedControlPlanConfig.chamberGroupName,
        recipeGroupName = matchedControlPlanConfig.recipeGroupName,
        runId = runId,
        toolName = eventStartDataMatchedWindow.toolName,
        toolId = matchedControlPlanConfig.toolId,
        chamberName = eventStartDataMatchedWindow.chamberName,
        chamberId = matchedControlPlanConfig.chamberId,
        indicatorValue = eventEndDataMatchedWindow.dataMissingRatio.toString,
        indicatorId = dataMissingConfig.indicatorId,
        indicatorName = dataMissingConfig.indicatorName,
        algoClass = dataMissingConfig.algoClass,
        indicatorCreateTime = System.currentTimeMillis(),
        missingRatio = eventEndDataMatchedWindow.dataMissingRatio,
        configMissingRatio = dataMissingConfig.missingRatio,
        runStartTime = eventStartDataMatchedWindow.runStartTime,
        runEndTime = eventEndDataMatchedWindow.runEndTime,
        windowStartTime = eventStartDataMatchedWindow.runStartTime,
        windowEndTime = eventEndDataMatchedWindow.runEndTime,
        windowDataCreateTime = System.currentTimeMillis(),
        limitStatus = matchedControlPlanConfig.limitStatus,
        recipeName = eventEndDataMatchedWindow.recipeName,
        recipeId = matchedControlPlanConfig.recipeId,
        product = productList,
        stage = stageList,
        materialName = eventEndDataMatchedWindow.materialName,
        bypassCondition = dataMissingConfig.bypassCondition,
        pmStatus = eventStartDataMatchedWindow.pmStatus,
        pmTimestamp = eventStartDataMatchedWindow.pmTimestamp,
        area = matchedControlPlanConfig.area,
        section = matchedControlPlanConfig.section,
        mesChamberName = matchedControlPlanConfig.mesChamberName,
        lotMESInfo = eventEndDataMatchedWindow.lotMESInfo,
        unit = "",
        dataVersion = eventStartDataMatchedWindow.dataVersion,
        cycleIndex = "0"
      )


      val data = FdcData[IndicatorResult](
        "indicator",
        dataMissingIndicatorResult
      )
      readOnlyContext.output(specialIndicatorOutput, data)
    }

  }

  /**
   * 组装 WindowData结构数据
   * @param window2Data
   * @param sensorAliasName
   * @param unit
   * @param indicatorIdList
   * @param cycleUnitCount
   * @param cycleIndex
   * @param startTime
   * @param endTime
   * @param rawDataSensorDataListResult
   * @return
   */
  def generateWindowData(window2Data: Window2Data,
                         sensorAliasName: String,
                         indicatorIdList: List[Long],
                         cycleUnitCount: Int,
                         cycleIndex: Int,
                         startTime: Long,
                         endTime: Long,
                         rawDataSensorDataList: ListBuffer[RawDataSensorData]) = {
    // 数据结构转换成 sensorDataList
    val sensorDataLists = rawDataSensorDataList.map(sensorData => {
      sensorDataList(sensorValue = sensorData.sensorValue,
        timestamp = sensorData.timestamp,
        step = sensorData.step)
    }).toList

    WindowData(windowId = window2Data.controlWindowId,
      sensorName = null,
      sensorAlias = sensorAliasName,
      unit = rawDataSensorDataList.head.unit,
      indicatorId = indicatorIdList,
      cycleUnitCount = cycleUnitCount,
      cycleIndex = cycleIndex,
      startTime = startTime,
      stopTime = endTime,
      startTimeValue = None,
      stopTimeValue = None,
      sensorDataList = sensorDataLists)
  }

  /**
   * 生产window结果数据
   * @param ventEndDataMatchedWindow
   * @param windowConfig
   * @param calcWindowDataResult
   * @param key
   * @return
   */
  def generateFdcWindowData(eventStartDataMatchedWindow: EventDataMatchedWindow,
                            eventEndDataMatchedWindow: EventDataMatchedWindow,
                            windowConfig: Window2Data,
                            windowDataList: List[WindowData]) = {

    val matchedControlPlanConfig = eventStartDataMatchedWindow.matchedControlPlanConfig

    // 获取windowStart/EndTime
    val windowStartTime = windowDataList.head.startTime
    val windowEndTime = windowDataList.head.stopTime

    // 获取最大的controlPlanVersion
    val matchedControlPlanId = eventEndDataMatchedWindow.matchedControlPlanId
    val controlPlanVersion = if(controlPlanMaxVersionMap.contains(matchedControlPlanId)){controlPlanMaxVersionMap(matchedControlPlanId)}else{eventEndDataMatchedWindow.controlPlanVersion}

    val lotMESInfo: List[Option[Lot]] = if(null == eventEndDataMatchedWindow.lotMESInfo || !eventEndDataMatchedWindow.lotMESInfo.nonEmpty){
      if(null == eventStartDataMatchedWindow.lotMESInfo || !eventStartDataMatchedWindow.lotMESInfo.nonEmpty){
       List(None)
      }else{
        eventStartDataMatchedWindow.lotMESInfo
      }
    }else{
      eventEndDataMatchedWindow.lotMESInfo
    }

    val resultLotMesInfo: List[Option[Lot]] = if(lotMESInfo.nonEmpty){
      lotMESInfo.map((elem: Option[Lot]) => {
        if(elem.nonEmpty){
          if(null == elem.get.wafers || !elem.get.wafers.nonEmpty){
            None
          }else{
            elem
          }
        }else{
          None
        }
      })
    }else{
      List(None)
    }

    fdcWindowData("fdcWindowDatas",
      windowListData(
        toolName = eventEndDataMatchedWindow.toolName,
        toolId = matchedControlPlanConfig.toolId,
        chamberName = eventEndDataMatchedWindow.chamberName,
        chamberId = matchedControlPlanConfig.chamberId,
        recipeName = eventEndDataMatchedWindow.recipeName,
        recipeId = matchedControlPlanConfig.recipeId,
        runId = s"${eventEndDataMatchedWindow.toolName}--${eventEndDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}",
        dataMissingRatio = eventEndDataMatchedWindow.dataMissingRatio,
        contextId = 0,
        productName = eventEndDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else ""),
        stage = eventEndDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else ""),
        controlWindowId = windowConfig.controlWindowId,
        controlPlanId = eventEndDataMatchedWindow.matchedControlPlanId,
        controlPlanVersion = controlPlanVersion,
        missingRatio = eventEndDataMatchedWindow.dataMissingRatio,
        windowStart = windowConfig.windowStart,
        windowStartTime = windowStartTime,
        windowEnd = windowConfig.windowEnd,
        windowEndTime = windowEndTime,
        windowTimeRange = windowEndTime - windowStartTime,
        runStartTime = eventStartDataMatchedWindow.runStartTime,
        runEndTime = eventEndDataMatchedWindow.runEndTime,
        windowEndDataCreateTime = System.currentTimeMillis(),
        windowType = "processEnd2.0",
        DCType = eventEndDataMatchedWindow.DCType,
        locationId = matchedControlPlanConfig.locationId,
        locationName = matchedControlPlanConfig.locationName,
        moduleId = matchedControlPlanConfig.moduleId,
        moduleName = matchedControlPlanConfig.moduleName,
        toolGroupId = matchedControlPlanConfig.toolGroupId,
        toolGroupName = matchedControlPlanConfig.toolGroupName,
        chamberGroupId = matchedControlPlanConfig.chamberGroupId,
        chamberGroupName = matchedControlPlanConfig.chamberGroupName,
        recipeGroupId = matchedControlPlanConfig.recipeGroupId,
        recipeGroupName = matchedControlPlanConfig.recipeGroupName,
        limitStatus = matchedControlPlanConfig.limitStatus,
        materialName = eventEndDataMatchedWindow.materialName,
        pmStatus = eventEndDataMatchedWindow.pmStatus,
        pmTimestamp = eventEndDataMatchedWindow.pmTimestamp,
        area = matchedControlPlanConfig.area,
        section = matchedControlPlanConfig.section,
        mesChamberName = matchedControlPlanConfig.mesChamberName,
        lotMESInfo = resultLotMesInfo,
        windowDatasList = windowDataList,
        dataVersion = eventEndDataMatchedWindow.dataVersion
      ))
  }

  /**
   * 收集乱序数据: rawData 早于 eventStart的数据
   * 收集丢失eventStart 的数据: eventStart丢失，rawData 和 eventEnd 都收集起来
   * @param inputValue
   * @param readOnlyContext
   */
  def giveUpData(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    // 侧道输出到指定kafka
//    readOnlyContext.output(calcWindowGiveUpRunStreamOutput,inputValue)
  }

  /**
   * 清理状态数据
   * @param key
   */
  def cleanAllState() = {
    processEndEventStartState.clear()
    rawDataMapState.clear()
    close()
  }


  override def processBroadcastElement(inputDimValue: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {

    try{
      val dataType = inputDimValue.get(MainFabConstants.dataType).asText()

      if (MainFabConstants.window2Config == dataType ) {
        try {

          // 解析 Window2Config 信息
          val configData: ConfigData[Window2Config] = toBean[ConfigData[Window2Config]](inputDimValue)
          parseWindow2Config(configData)
        } catch {
          case e: Exception => {
            logger.error(s"parseWindow2Config error; " +
              s"inputDimValue == ${inputDimValue}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }

      }else if (dataType == MainFabConstants.indicatorConfig ){

        try{
          val indicatorConfig = toBean[ConfigData[IndicatorConfig]](inputDimValue)
          if(indicatorConfig.datas.algoClass == MainFabConstants.cycleCountIndicator) {
            addCycleCountIndicatorToIndicatorConfigMap(indicatorConfig)
          }

          if(indicatorConfig.datas.algoClass == MainFabConstants.dataMissingRatio) {
            addDataMissingIndicatorToIndicatorConfigMap(indicatorConfig)
          }
        }catch {
          case e:Exception => {
            logger.error(s"addCycleCountIndicatorToIndicatorConfigMap error; " +
              s"inputDimValue == ${inputDimValue}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }
      }else if (dataType == "debugTest") {
        // 用于window切窗口debug调试
        try {
          parseDebugConfig(inputDimValue)
        } catch {
          case e: Exception => {
            logger.warn(s"debugScala json error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }
      }else if (dataType == "debugWindowJobConfigInfo") {
        // 用于window切窗口debug调试
        try {
          val debugWindowJobConfig = toBean[DebugWindowJobConfig](inputDimValue)
          parseDebugWindowJobConfigInfo(debugWindowJobConfig)
        } catch {
          case e: Exception => {
            logger.warn(s"debugWindowJobConfig json error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }
      }else if (dataType == "debugCalcWindow") {
        // 用于window切窗口debug调试
        try {
          val debugCalcWindow = toBean[DebugCalcWindow](inputDimValue)
          parseDebugCalcWindow(debugCalcWindow)
        } catch {
          case e: Exception => {
            logger.warn(s"DebugCalcWindow json error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }
      }
    }catch{
      case e:Exception => {
        logger.warn(s"processBroadcastElement error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
      }
    }
  }


  /**
   *
   *  optionCode:
   *    0:修改window 配置;
   *      默认都为0，全删全建;
   *    1:删除controlPlan;
   *      当删除controlPlan时，需要讲该controlPlanId下的所有window配置都删除
   *
   * @param configData
   * @return
   */
  def parseWindow2Config(configData: ConfigData[Window2Config], isOnLine:Boolean = false) = {
    val window2Config = configData.datas
    val optionCode = window2Config.optionCode

    optionCode match {
      case 0 =>{
        updateWindowConfigMap(window2Config,isOnLine)
      }
      case 1 =>{
        deleteWindowConfigMap(window2Config)
      }
      case _ =>{
        logger.error(s"parseWindowConfig error : optionCode error == ${optionCode} " +
          s"configData == ${configData.toJson} ")
      }
    }
  }

  /**
   * 将kafka的window配置信息解析成分区模式
   * @param window2Data
   * @return
   */
  def parseWindow2DataPartition(window2Data: Window2Data) = {
    val sensorAliasNameList = window2Data.sensorAliasNameList
    val sensorNum = sensorAliasNameList.size

    var partitionNum = sensorNum/ProjectConfig.MAINFAB_WINDOW2_PARTITION_CAPACITY
    partitionNum = if(0 == partitionNum){
      partitionNum + 1
    }else{
      partitionNum
    }

    val partitionSensorListMap = new TrieMap[String,ListBuffer[String]]()

    sensorAliasNameList.foreach(sensorAliasName => {
      val partitionCode = sensorAliasName.hashCode.abs % partitionNum
      val partitionId = "partition_" + partitionCode
      val sensorPartitionList = partitionSensorListMap.getOrElse(partitionId,ListBuffer[String]())
      sensorPartitionList.append(sensorAliasName)
      partitionSensorListMap.put(partitionId,sensorPartitionList)
    })

    Window2DataPartition(controlWindowId = window2Data.controlWindowId,
      parentWindowId = window2Data.parentWindowId,
      isTopWindows = window2Data.isTopWindows,
      isConfiguredIndicator = window2Data.isConfiguredIndicator,
      windowStart = window2Data.windowStart,
      windowEnd = window2Data.windowEnd,
      splitWindowBy = window2Data.splitWindowBy,
      controlWindowType = window2Data.controlWindowType,
      calculationTrigger = window2Data.calculationTrigger,
      extraSensorAliasNameList = window2Data.extraSensorAliasNameList,
      sensorAliasNameListMap = partitionSensorListMap,
      windowSensorInfoList = window2Data.windowSensorInfoList)
  }

  /**
   * optionCode == 0 时 更新该controlPlanId下的配置信息
   * 保留两个版本
   *
   * @param window2Config
   * @return
   */
  def updateWindowConfigMap(window2Config: Window2Config, isOnLine:Boolean = false) = {
    val controlPlanId = window2Config.controlPlanId
    val controlPlanVersion: Long = window2Config.controlPlanVersion
    val window2DataList = window2Config.window2DataList

    // todo 判断版本号是否大于之前的版本号
    val currentMaxControlPlanVersion: Long = controlPlanMaxVersionMap.getOrElse(controlPlanId, 0)
    if(controlPlanVersion >= currentMaxControlPlanVersion){
      val window2DataMap = new TrieMap[Long,Window2Data]
      window2DataList.foreach(window2Data => {

        // 拆分成分区模式
        //      val window2DataPartition = parseWindow2DataPartition(window2Data)
        //      if(isOnLine){
        //        logger.error(s"window2DataPartition == ${window2DataPartition.toJson}")
        //      }
        window2DataMap.put(window2Data.controlWindowId,window2Data)
      })

      val versionWindowConfigMap = windowConfigMap.getOrElse(controlPlanId, new TrieMap[Long,TrieMap[Long,Window2Data]])
      versionWindowConfigMap.put(controlPlanVersion,window2DataMap)

      val saveVersionWindowConfigMap = if(versionWindowConfigMap.size > 2){
        val newVersionWindowConfigMap = deleteMinVersionWindowConfig(versionWindowConfigMap)
        newVersionWindowConfigMap
      }else{
        versionWindowConfigMap
      }

      // 缓存window 配置
      windowConfigMap.put(controlPlanId,saveVersionWindowConfigMap)

      // 缓存controlPlanMaxVersion 信息
      controlPlanMaxVersionMap.put(controlPlanId,controlPlanVersion)
    }else{
      logger.warn(s"新收到的策略信息不是最新版本!\n " +
        s"新策略 controlPlanId == ${window2Config.controlPlanId} \n " +
        s"新策略 controlPlanVersion == ${window2Config.controlPlanVersion} \n " +
        s"currentMaxControlPlanVersion == ${currentMaxControlPlanVersion} \n ")
    }


  }

  /**
   * 删除最小version号的配置
   * @param versionWindowConfigMap
   * @return
   */
  def deleteMinVersionWindowConfig(versionWindowConfigMap: TrieMap[Long, TrieMap[Long, Window2Data]]) = {
    val newVersionWindowConfigMap = versionWindowConfigMap.clone()

    val versionList: Iterable[Long] = newVersionWindowConfigMap.keys
    val minVersion = versionList.min
    newVersionWindowConfigMap.remove(minVersion)

    newVersionWindowConfigMap
  }

  /**
   * optionCode == 1 时 删除该controlPlanId 下的所有配置信息
   * @param window2Config
   * @return
   */
  def deleteWindowConfigMap(window2Config: Window2Config) = {
    val controlPlanId = window2Config.controlPlanId

    // 删除window配置
    if(windowConfigMap.contains(controlPlanId)){
      windowConfigMap.remove(controlPlanId)
    }

    // 删除controlPlanMaxVersion
    if(controlPlanMaxVersionMap.contains(controlPlanId)){
      controlPlanMaxVersionMap.remove(controlPlanId)
    }
  }

  /**
   * 构建windows树
   *
   * @param window2Data
   * @param window2DataMap
   * @return
   */
  def createContainerIControlWindow(window2Data: Window2Data, window2DataMap: Map[Long, Window2Data]):  IControlWindow = {

    try{
      val containerIControlWindow: IControlWindow = ApiControlWindow.parse(window2Data.controlWindowType,
        window2Data.windowStart,
        window2Data.windowEnd)

      containerIControlWindow.setWindowId(window2Data.controlWindowId)

//      val windowSensorInfoList = window2Data.windowSensorInfoList.filter(_.sensorAliasName != null)
//      for (sensorInfo <- windowSensorInfoList) {
//        for (indicatorId <- sensorInfo.indicatorIdList) {
//          containerIControlWindow.addIndicatorConfig(indicatorId, sensorInfo.sensorAliasId, sensorInfo.sensorAliasName)
//        }
//      }

      //是否是最上层的window
      if (window2Data.isTopWindows) {
        //递归结束
        containerIControlWindow
      } else {
        //获取父window的配置
        if (window2DataMap.contains(window2Data.parentWindowId)) {
          val parentWindowConfig: Window2Data = window2DataMap.get(window2Data.parentWindowId).get
          //递归,返回父window
          val parentContainerIControlWindow = createContainerIControlWindow(parentWindowConfig, window2DataMap)
          //添加子window
          parentContainerIControlWindow.addSubWindow(containerIControlWindow)
          //返回子window的配置和构建好子window 的父window 对象
          parentContainerIControlWindow
        } else {
          logger.error(ErrorCode("0250020001B",
            System.currentTimeMillis(),
            Map("parentWindowId" -> window2Data.parentWindowId,
              "controlWindowId" -> window2Data.controlWindowId,
              "contextAllWindowId" -> window2DataMap.map(w => w._2.controlWindowId)),
            " window 配置错误,找不到父window，构建window结构失败").toJson)
          null
        }
      }
    }catch {
      case e: Exception =>
        logger.error(ErrorCode("0250020002B",
          System.currentTimeMillis(),
          Map("WindowId" -> window2Data.controlWindowId,
            "parentWindowId" -> window2Data.parentWindowId,
            "windowStart" -> window2Data.windowStart,
            "windowEnd" -> window2Data.windowEnd,
            "updateContainerIControlWindowMap" -> ""), ExceptionInfo.getExceptionInfo(e)).toJson)
        null
    }
  }

  /**
   * 添加indicatorConfigByCycleCount
   *
   * @param config
   */
  def addCycleCountIndicatorToIndicatorConfigMap(config: ConfigData[IndicatorConfig]): Unit = {
    val indicatorConfig = config.datas
    if (config.status) {
      indicatorConfigByCycleCount.put(indicatorConfig.controlWindowId, indicatorConfig)
    } else {
      indicatorConfigByCycleCount.remove(indicatorConfig.controlWindowId)
    }
  }


  /**
   * 添加indicatorConfigByDataMissing
   * @param config
   */
  def addDataMissingIndicatorToIndicatorConfigMap(config: ConfigData[IndicatorConfig]): Unit = {

    val indicatorConfig = config.datas
    val controlPlanId = indicatorConfig.controlPlanId
    val controlWindowId = indicatorConfig.controlWindowId
    val key = controlPlanId.toString + "_" + controlWindowId
    if (config.status) {
      indicatorConfigByDataMissing.put(key, indicatorConfig)
    } else {
      indicatorConfigByDataMissing.remove(key)
    }
  }


  /**
   *
   * debug 指定的 windowId 切窗口时的详细信息
   * 当策略信息中 status == true 添加 windowId
   * 当策略信息中 status == false 删除 windowId
   * @param debugCalcWindow
   */
  def parseDebugCalcWindow(debugCalcWindow : DebugCalcWindow) = {
    val status = debugCalcWindow.status
    val windowId = debugCalcWindow.windowId
    if(status && !debugCalcWindowList.contains(windowId)){
      debugCalcWindowList += windowId
    }else{
      if(debugCalcWindowList.contains(windowId)){
        debugCalcWindowList -= windowId
      }
    }
  }

  /**
   * 解析debugTest 配置信息
   * @param inputDimValue
   * @return
   */
  def parseDebugConfig(inputDimValue: JsonNode) = {
    val debugScala = toBean[DebugScala](inputDimValue)
    logger.warn(s"debugScala: ${debugScala.toJson}")
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
  }


  /**
   *
   * @param debugWindowJobConfig
   */
  def parseDebugWindowJobConfigInfo(debugWindowJobConfig: DebugWindowJobConfig) = {
    val controlPlanIdList: List[DebugWindowJobInfo] = debugWindowJobConfig.controlPlanList
    if(controlPlanIdList.nonEmpty){
      controlPlanIdList.foreach(debugWindowJobInfo => {
        // 打印 windowConfigMap 信息
        logger.warn(s"-----------------------------------printWindowConfigMap------------------------------------")
        printWindowConfigMap(debugWindowJobInfo.controlPlanId,debugWindowJobInfo.windowIdList)
      })
    }
  }

  /**
   * 打印 windowConfigMap
   * @param controlPlanId
   * @param windowIdList
   */
  def printWindowConfigMap(controlPlanId: Long, windowIdList: List[Long]) = {
    if(windowConfigMap.contains(controlPlanId)){
      val versionWindowConfigMap = windowConfigMap.get(controlPlanId).get
      versionWindowConfigMap.foreach(version_windowIdWindowConfigMap => {
        val versionId = version_windowIdWindowConfigMap._1
        val windowIdWindowConfigMap = version_windowIdWindowConfigMap._2

        windowIdWindowConfigMap.foreach(windowId_window2Data => {
          val windowId = windowId_window2Data._1
          val window2Data = windowId_window2Data._2
          // 如果指定了windowId 就只打印指定的windowId信息
          if(windowIdList.nonEmpty){
            if(windowIdList.contains(windowId)){
              logger.warn(s"controlPlanId->versionId->windowId : ${controlPlanId}->${versionId}->${windowId}")
              logger.warn(s"window2Data == ${window2Data.toJson}")
            }
          }else{
            // 如果没有指定windowId 就打印该controlPlan下所有的windowId信息
            logger.warn(s"controlPlanId->versionId->windowId : ${controlPlanId}->${versionId}->${windowId}")
            logger.warn(s"window2Data == ${window2Data.toJson}")
          }
        })
      })
    }
  }


  /**
   * 生成日志信息
   *
   * @param logCode
   * @param message
   * @param paramsInfo
   * @param dataInfo
   * @param exception
   * @return
   */
  def generateMainFabLogInfo(logCode:String, funName:String, message:String, paramsInfo:Map[String,Any]=null, dataInfo:String="", exception:String = ""): MainFabLogInfo = {
    MainFabLogInfo(mainFabDebugCode = job_fun_DebugCode + logCode,
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

  /**
   * 生成调试日志
   * @param paramsInfo
   * @return
   */
  def generateMainFabDebugInfo(paramsInfo:Map[String,Any]=null) = {
    MainFabDebugInfo(
      logTime = DateTimeUtil.getCurrentTime(),
      paramsInfo = paramsInfo
    )
  }


  /**
   * 解析Redis 中的 window 配置信息
   * @param configData
   * @return
   */
  def parseWindow2ConfigByRedis(runId:String,controlPlanId:Long, controlPlanVersion:Long, configData:RedisWindowConfig,
                                readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val window2Config = configData.datas
    val optionCode = window2Config.optionCode
    val window2DataMap = new TrieMap[Long,Window2Data]
    optionCode match {
      case 0 =>{
        val redis_controlPlanId = window2Config.controlPlanId
        val redis_controlPlanVersion = window2Config.controlPlanVersion
        val window2DataList = window2Config.window2DataList
        if(redis_controlPlanId == controlPlanId && redis_controlPlanVersion == controlPlanVersion){
          window2DataList.foreach(window2Data => {

            // 拆分成分区模式
//            val window2DataPartition = parseWindow2DataPartition(window2Data)
//            window2DataPartitionMap.put(window2Data.controlWindowId,window2DataPartition)

            window2DataMap.put(window2Data.controlWindowId,window2Data)
          })
        }else{
          logger.error(ErrorCode("0250020010B", System.currentTimeMillis(),
            Map("msg" -> "读取 Redis 失败 -- 版本不匹配",
              "controlPlanId" -> controlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "redis_controlPlanId" -> redis_controlPlanId,
              "redis_controlPlanVersion" -> redis_controlPlanVersion,
              "runId" -> runId
            ),"").toString)

          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0010B",
              "getWindowConfigFromRedis",
              s"[processEnd] ==> 读取 Redis 失败 -- 版本不匹配 -----0010B----- ",
              Map[String, Any]("controlPlanId" -> controlPlanId,
                "controlPlanVersion" -> controlPlanVersion,
                "redis_controlPlanId" -> redis_controlPlanId,
                "redis_controlPlanVersion" -> redis_controlPlanVersion,
                "runId" -> runId)))

        }
      }
      case _ =>{
        logger.error(ErrorCode("0250020011B", System.currentTimeMillis(),
          Map("msg" -> "读取 Redis 失败 -- optionCode 错误",
            "controlPlanId" -> controlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "optionCode" -> optionCode,
            "runId" -> runId
          ),"").toString)
      }
    }

    window2DataMap
  }

  /**
   * 根据 controlPlanId verion windowId 读取redis 中的window配置信息
   * @param controlPlanId
   * @param controlPlanVersion
   * @param windowId
   */
  def getWindowConfigFromRedis(runId:String,controlPlanId:Long,controlPlanVersion:Long,windowId:Long,
                               readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {
    val jedis = getJedis()
    try {
      if(null != jedis){
        jedis.select(ProjectConfig.REDIS_CONFIG_CACHE_DATABASE)
        val key = s"windowconfig_${controlPlanId}_${controlPlanVersion}"
        val filed = s"${windowId}"
        val windowConfigDataStr = jedis.hget(key,filed)

        if(StringUtils.isNotEmpty(windowConfigDataStr)){
          val newStr = windowConfigDataStr.substring(1,windowConfigDataStr.length-1).replaceAll("\\\\","")
          val redisWindowConfig: RedisWindowConfig = newStr.fromJson[RedisWindowConfig]
          parseWindow2ConfigByRedis(runId,controlPlanId,controlPlanVersion,redisWindowConfig,readOnlyContext)
        }else{

          logger.error(ErrorCode("0250020009B", System.currentTimeMillis(),
            Map("msg" -> "读取Redis 失败 --- 读出来的配置信息为空",
              "controlPlanId" -> controlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "windowId" -> windowId,
              "runId"->runId,
              "windowConfigDataStr" -> windowConfigDataStr,
              "jedis" -> jedis.toString,
              "key" -> key,
              "filed" -> filed
            ),"").toString)

          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0009B",
              "getWindowConfigFromRedis",
              s"[processEnd] ==> 读取Redis 失败 --- 读出来的配置信息为空 -----0009B----- ",
              Map[String, Any]("controlPlanId" -> controlPlanId,
                "controlPlanVersion" -> controlPlanVersion,
                "windowId" -> windowId,
                "runId" -> runId,
                "jedis" -> jedis.toString,
                "key" -> key,
                "filed" -> filed)))

          new TrieMap[Long, Window2Data]
        }
      }else{
        logger.error(ErrorCode("0250020012B", System.currentTimeMillis(),
          Map("msg" -> "读取Redis 失败---jedis对象为空！",
            "controlPlanId" -> controlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "windowId" -> windowId,
            "runId" -> runId), null).toString)

        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0012B",
            "getWindowConfigFromRedis",
            s"[processEnd] ==> 读取Redis 失败 --- 读取Redis 失败---jedis对象为空！ -----0012B----- ",
            Map[String, Any]("controlPlanId" -> controlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "windowId" -> windowId,
              "runId" -> runId,
              "jedis" -> jedis.toString)))

        new TrieMap[Long, Window2Data]
      }

    } catch {
      case exception: Exception => {
        logger.error(ErrorCode("0250020003B", System.currentTimeMillis(),
          Map("msg" -> "读取Redis 失败",
            "controlPlanId" -> controlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "windowId" -> windowId,
            "runId" -> runId
          ), exception.toString).toString)

        // todo 尝试重新连接Redis;
        logger.warn(s"retry connect redis")
        getRedisConnection()
        new TrieMap[Long, Window2Data]
      }
    }finally {
      jedis.close()
    }
  }

  /**
   * 获取Redis连接
   */
  def getRedisConnection(): Unit = {
    val config =  new JedisPoolConfig
    config.setMaxTotal(ProjectConfig.REDIS_MAX_TOTAL)
    config.setMaxIdle(ProjectConfig.REDIS_MAX_IDLE)
    config.setMinIdle(ProjectConfig.REDIS_MIN_IDLE)

    //哨兵模式
    val redismaster = ProjectConfig.REDIS_MASTER
    val redis_sentinel_nodes = ProjectConfig.REDIS_SENTINEL_NODES
    val redisconnectiontimeout = ProjectConfig.REDIS_CONNECTION_TIMEOUT
    val sentinel_set = redis_sentinel_nodes.split(",").toSet

    //构建Redis线程池
    jedisSentinelPool = new JedisSentinelPool(redismaster,sentinel_set,config,redisconnectiontimeout)

//    try{
//      //从线程池中获取连接
//      jedis = jedisSentinelPool.getResource
//      jedis.select(ProjectConfig.REDIS_CONFIG_CACHE_DATABASE)
//
//    }catch {
//      case e:Exception => {
//        logger.error(s"--- 获取连接失败 -----")
//      }
//    }
  }

  // 获取单个连接
  def getJedis() = {
    var jedis :Jedis = null
    try {
      if (jedisSentinelPool == null) {
        getRedisConnection()
        jedis = jedisSentinelPool.getResource
      } else {
        //从线程池中获取连接
        jedis = jedisSentinelPool.getResource
      }
    }catch {
      case e:Exception=>{
        logger.error(s"")
        getRedisConnection()
      }
    }
    jedis
  }
}
