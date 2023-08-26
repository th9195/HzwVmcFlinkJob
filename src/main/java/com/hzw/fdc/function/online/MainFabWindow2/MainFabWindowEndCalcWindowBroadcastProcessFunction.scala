package com.hzw.fdc.function.online.MainFabWindow2

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
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
import org.apache.flink.api.common.state._
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
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

class MainFabWindowEndCalcWindowBroadcastProcessFunction() extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,  JsonNode] {

  /**
   * 025:MainFabWindowEndWindow2Service
   * 002:切窗口
   */
  private val job_fun_DebugCode:String = "026002"
  private val jobName:String = "MainFabWindowEndWindow2Service"
  private val optionName : String = "MainFabWindowEndCalcWindowBroadcastProcessFunction"

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowEndCalcWindowBroadcastProcessFunction])

  // 划窗口时的丢弃的数据 侧道输出
  lazy val calcWindowGiveUpRunStreamOutput = new OutputTag[JsonNode]("calcWindowGiveUpRunStream")

  // 特殊Indicator 的结果数据侧道输出
  lazy val specialIndicatorOutput = new OutputTag[FdcData[IndicatorResult]]("specialIndicatorOutput")

  // debug调试 的打印数据侧道输出
  lazy val debugOutput = new OutputTag[String]("debugTest")

  lazy val mainFabDebugInfoOutput = new OutputTag[MainFabDebugInfo]("mainFabDebugInfo")

  // 侧道输出日志信息
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")

  /**
   * 缓存windowConfig配置信息
   * 保留2个版本
   * key1:controlPlanId
   * key2:controlVersion
   * key3:windowId
   * value:Window2Data
   */
  private val windowConfigMap = new TrieMap[Long,TrieMap[Long,TrieMap[Long,Window2Data]]]()

  /**
   * 缓存 controlPlan的最大版本号
   */
  private val controlPlanMaxVersionMap = new TrieMap[Long,Long]()

  /**
   * 缓存切窗口时使用的window配置信息 和 切窗口的对象
   * 用于: RawData 数据频繁尝试切window的时候不需要每次都去创建切window的对象;
   * 注意: window 切出来后就需要是否; 并且eventEnd 后必须是否;
   * key1:traceId
   * value : 2元元组 (Window2Data,IControlWindow)
   */
  private val windowConfigAndCalcWindowObjMap = new TrieMap[String,(Window2Data,IControlWindow)]()

  /**
   * 缓存CycleCount的配置
   * key: controlWindowId
   * value: IndicatorConfig
   */
  private val indicatorConfigByCycleCount = new concurrent.TrieMap[Long, IndicatorConfig]()

  /**
   * debug 指定的 windowId 切窗口时的详细信息
   * 当策略信息中 status == true 添加 windowId
   * 当策略信息中 status == false 删除 windowId
   */
  var debugCalcWindowList = new ListBuffer[Long]()

  /**
   * value : EventDataMatchedWindow
   */
  private var windowEndEventStartState:ValueState[EventDataMatchedWindow] = _

  /**
   * keyby : timestamp
   * value: WindowRawData
   */
  var rawDataMapState: MapState[Long,Window2RawData] = _


  /**
   * keyby: traceId | controlPlanId | windowId
   * value: StepId
   *
   * 添加/更新 : 在RawData 数据中 StepId 更改 时更新
   * 删除 : 在划出窗口后删除; (特殊情况:cycleWindow ， 在 EventEnd 时删除)
   * 用于 : 在判断是否满足切window的时候
   */
//  private var runDataStepIdValueState: ValueState[Long] = _
  private var rawDataLastCalcWindowStepIdTimeValueState: ValueState[WindowEndLastCalcWindowInfo] = _

  //=========用于debug调试================
  // controlWindowId -> function
  private var debugConfig = new concurrent.TrieMap[Long, mutable.Set[String]]()

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

    if(!IS_DEBUG){
      val startTime = System.currentTimeMillis()

      logger.warn(s"--- parse Hbase Config start time = ${startTime}" )
      // 初始化 Hbase中的Window2Config信息
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
        if (indicatorConfig.datas.algoClass == MainFabConstants.cycleCountIndicator) {
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


    // 初始化 windowEndEventStartState
    val windowEndEventStartStateDescription = new
        ValueStateDescriptor[EventDataMatchedWindow]("windowEndEventStartState",TypeInformation.of(classOf[EventDataMatchedWindow]))
    windowEndEventStartStateDescription.enableTimeToLive(hour26TTLConfig)
    windowEndEventStartState = getRuntimeContext.getState(windowEndEventStartStateDescription)

    // 初始化 rawDataMapState
    val rawDataMapStateDescription: MapStateDescriptor[Long,Window2RawData] = new
        MapStateDescriptor[Long,Window2RawData]("rawDataMapState", TypeInformation.of(classOf[Long]),TypeInformation.of(classOf[Window2RawData]))
    // 设置过期时间
    rawDataMapStateDescription.enableTimeToLive(hour26TTLConfig)
    rawDataMapState = getRuntimeContext.getMapState(rawDataMapStateDescription)

    // 初始化 rawDataLastCalcWindowStepIdTimeValueState
    val rawDataLastCalcWindowStepIdTimeValueStateDescription = new
        ValueStateDescriptor[WindowEndLastCalcWindowInfo]("rawDataLastCalcWindowStepIdTimeValueState",TypeInformation.of(classOf[WindowEndLastCalcWindowInfo]))
    rawDataLastCalcWindowStepIdTimeValueStateDescription.enableTimeToLive(hour26TTLConfig)
    rawDataLastCalcWindowStepIdTimeValueState = getRuntimeContext.getState(rawDataLastCalcWindowStepIdTimeValueStateDescription)

    // 初始化自定义metrics
    getRuntimeContext()
      .getMetricGroup()
      .gauge[Long, ScalaGauge[Long]]("calcWindowCostTimeGauge", ScalaGauge[Long](() => calcWindowCostTime))


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
      case ex: Exception => logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> "处理业务数据失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
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
      windowEndEventStartState.update(eventStartDataMatchedWindow)

    }catch {
      case ex: Exception => logger.error(ErrorCode("026002d012C",
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
      val traceId = rawDataMatchedWindow.traceId
      val matchedControlPlanId = rawDataMatchedWindow.matchedControlPlanId
      val matchedWindowId = rawDataMatchedWindow.matchedWindowId

      if(null != windowEndEventStartState.value()){
        // 缓存 rawData 数据到MapState
        cacheRawDataToState(rawDataMatchedWindow)

        // 判断是否可以计算
        val isCalcWindowStatus = judgeIsSatisfiedCalcWindow(rawDataMatchedWindow)

        if(isCalcWindowStatus){
          // 尝试切window的时候才开始创建切window的对象并缓存到全局变量中,Map[traceId,(window配置,切window对象)]
          val windowConfig_calcWidnowObject = createCalcWindowObject(readOnlyContext)
          val windowConfigData: Window2Data = windowConfig_calcWidnowObject._1
          val calcWindowObject: IControlWindow = windowConfig_calcWidnowObject._2

          if(null != windowConfigData && null != calcWindowObject){
            // windowEnd 切窗口
            windowEndCalcWindow(rawDataMatchedWindow,
              null,
              false,
              windowConfigData,
              calcWindowObject,
              readOnlyContext,collector)
          }
        }
      }else{
        // eventStart 丢失 或者 rawData 早于 eventStart
        giveUpData(inputValue,readOnlyContext)
      }
    }catch {
      case ex: Exception => logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> s"----处理 RawData 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 缓存RawData数据
   *
   * @param rawDataMatchedWindow
   * @param key
   */
  def cacheRawDataToState(rawDataMatchedWindow: RawDataMatchedWindow) = {
    val stepId = rawDataMatchedWindow.stepId
    val timestamp = rawDataMatchedWindow.timestamp
    val data: List[(String, Double, String)] = rawDataMatchedWindow.data
//    val newRawData: (Long, Long, List[(String, Double, String)]) = (stepId, timestamp, data)
//    rawDataListState.add(newRawData)

    val newWindowRawData = Window2RawData(stepId,timestamp,data)
    rawDataMapState.put(timestamp,newWindowRawData)

  }

  /**
   *
   * 解决问题:
   *  1- 切window太频繁;
   *
   * 策略:
   *  1- 当stepId改变过, 并且距上次切窗口时间相差3分钟 , 才满足切窗口的条件;
   * 实现步骤:
   *  1- 如果是第一条rawData数据过来就缓存第一条rawData的stepId,true(表示stepId是否改变过),timeStamp
   *  2- 如果后面的数据时间戳于上次切窗口时的时间戳相差超过了3分钟 并且 stepId改变过 则满足切窗口条件,并切记录 (new_stepId , false , new_timeStamp)
   *  3- 如果面的数据 stepId 改变了 , 并且标记位位false 则记录 (new_stepId , true , old_timeStamp)
   *    注意: 这里必须记录上次满足切窗口时的时间戳;
   *
   * @param key
   * @param rawDataMatchedWindow
   * @return
   */
  def judgeIsSatisfiedCalcWindow(rawDataMatchedWindow: RawDataMatchedWindow) = {
    var isCalcWindow = false
    val new_stepId: Long = rawDataMatchedWindow.stepId
    val new_timestamp = rawDataMatchedWindow.timestamp
    val rawDataLastCalcWindowStepIdTime: WindowEndLastCalcWindowInfo = rawDataLastCalcWindowStepIdTimeValueState.value()

    if (null != rawDataLastCalcWindowStepIdTime) {
      val old_stepId = rawDataLastCalcWindowStepIdTime.stepId
      val stepIdIsChanged = rawDataLastCalcWindowStepIdTime.stepIdIsChanged
      val old_timestamp = rawDataLastCalcWindowStepIdTime.timestamp
      val diffTimestamp = new_timestamp - old_timestamp

      if(stepIdIsChanged && diffTimestamp >= ProjectConfig.CALC_WINDOW_INTERVAL){
        rawDataLastCalcWindowStepIdTimeValueState.update(WindowEndLastCalcWindowInfo(new_stepId,false,new_timestamp))
        isCalcWindow = true
      }else if(!stepIdIsChanged && new_stepId != old_stepId) {
        rawDataLastCalcWindowStepIdTimeValueState.update(WindowEndLastCalcWindowInfo(new_stepId,true,old_timestamp))
        isCalcWindow = false
      }
    } else {
      rawDataLastCalcWindowStepIdTimeValueState.update(WindowEndLastCalcWindowInfo(new_stepId,true,new_timestamp))
      isCalcWindow = false
    }
    isCalcWindow
  }


  /**
   *
   * @param window2Data
   * @return
   */
  def parseRawData(runId:String,
                   window2Data: Window2Data,
                   readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val splitWindowByAllSensorList: List[String] = if (null != window2Data) {
      val extraSensorAliasNameList = window2Data.extraSensorAliasNameList
      val splitWindowBy = window2Data.splitWindowBy.split("####").toList
      extraSensorAliasNameList ++ splitWindowBy
    } else {
      List()
    }
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

      // 切window 所需要的所有sensor;
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
          Map[String, Any]("windowType" -> MainFabConstants.windowTypeWindowEnd ,
            "runId" -> runId,
            "splitBackupSensor" -> splitBackupSensor,
            "windowConfigData" -> window2Data)))

      (allRawDataSplitWindowByBackupSensorDataValueList,sensorAliasMap)
    }
  }

  /**
   *
   * @param matchedControlPlanId
   * @param controlPlanVersion
   */
  def createCalcWindowObject(readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {
    // 获取 eventStart
    val runEventStartDataMatchedWindow = windowEndEventStartState.value()
    val traceId = runEventStartDataMatchedWindow.traceId
    val matchedControlPlanId = runEventStartDataMatchedWindow.matchedControlPlanId
    val matchedWindowId = runEventStartDataMatchedWindow.matchedWindowId
    val controlPlanVersion = runEventStartDataMatchedWindow.controlPlanVersion

    if(windowConfigAndCalcWindowObjMap.contains(traceId + matchedWindowId)){
      windowConfigAndCalcWindowObjMap.get(traceId + matchedWindowId).get
    }else{
      val runId = s"${runEventStartDataMatchedWindow.toolName}--${runEventStartDataMatchedWindow.chamberName}--${runEventStartDataMatchedWindow.runStartTime}"
      val versionWindowConfigMap = windowConfigMap.getOrElse(matchedControlPlanId, new TrieMap[Long, TrieMap[Long, Window2Data]])

      if(windowConfigMap.contains(matchedControlPlanId)){
        // 当这里没有读到该版本的配置信息时，再从Redis获取
        val window2DataMap: TrieMap[Long, Window2Data] = versionWindowConfigMap.getOrElse(controlPlanVersion,getWindowConfigFromRedis(runId,matchedControlPlanId,controlPlanVersion,matchedWindowId,readOnlyContext))

        // 如果没有匹配到对应的版本， 就使用最新的版本
        val matchedWindowConfigMap = if(window2DataMap.nonEmpty){
          window2DataMap
        }else{
          versionWindowConfigMap.get(controlPlanMaxVersionMap.get(matchedControlPlanId).get).get
        }

        val elemWindow2Data: Window2Data= matchedWindowConfigMap.getOrElse(matchedWindowId, null)

        if(null != elemWindow2Data){
          val containerIControlWindow = createContainerIControlWindow(elemWindow2Data,matchedWindowConfigMap.toMap)
          if(null != containerIControlWindow){

            // 缓存后面rawData需求时,可直接从缓存中获取
            windowConfigAndCalcWindowObjMap.put(traceId + matchedWindowId,(elemWindow2Data,containerIControlWindow))

            (elemWindow2Data,containerIControlWindow)
          }else{
            logger.error(s"[windowEnd] ==> 创建切窗口对象失败 -----0001C-----  \n " +
              s"controlPlanId == $matchedControlPlanId \n " +
              s"controlPlanVersion == ${controlPlanVersion} " +
              s"controlWindowId == $matchedWindowId ; \n" +
              s"runId == $runId ; ")

            // 该日志应该侧道输出到kafka ;
            readOnlyContext.output(mainFabLogInfoOutput,
              generateMainFabLogInfo("0001C",
                "createCalcWindowObject",
                s"[windowEnd] ==> 创建切窗口对象失败 -----0001C----- ",
                Map[String, Any]("controlPlanId" -> matchedControlPlanId,
                  "controlPlanVersion" -> controlPlanVersion,
                  "controlWindowId" -> matchedWindowId,
                  "runId" -> runId,
                  "elemWindow2Data" -> elemWindow2Data)
              ))

            (elemWindow2Data,null)
          }
        }else{
          logger.error(s"[windowEnd] ==> 创建切窗口对象失败 -----0000C----- 没有该window的配置信息\n " +
            s"controlPlanId == $matchedControlPlanId \n ; " +
            s"controlPlanVersion == ${controlPlanVersion} ; " +
            s"controlWindowId == $matchedWindowId ; \n" +
            s"runId == $runId ; ")

          // 该日志应该侧道输出到kafka ;
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0000C",
              "createCalcWindowObject",
              s"[windowEnd] ==> 创建切窗口对象失败 -----0000C----- ",
              Map[String, Any]("controlPlanId" -> matchedControlPlanId,
                "controlPlanVersion" -> controlPlanVersion,
                "controlWindowId" -> matchedWindowId,
                "runId" -> runId,
                "elemWindow2Data" -> elemWindow2Data)
            ))
          (null,null)
        }
      }else{
        logger.error(s"[windowEnd] ==> 创建切窗口对象失败 -----0000B----- 匹配到的controlPlanId 没有对应的window配置信息\n " +
          s"controlPlanId == $matchedControlPlanId \n " +
          s"controlPlanVersion == ${controlPlanVersion} " +
          s"controlWindowId == $matchedWindowId ; \n" +
          s"runId == $runId ; ")

        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0000B",
            "createCalcWindowObject",
            s"[windowEnd] ==> 创建切窗口对象失败 -----0000B----- 匹配到的controlPlanId 没有对应的window配置信息",
            Map[String, Any]("controlPlanId" -> matchedControlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "controlWindowId" -> matchedWindowId,
              "runId" -> runId)))
        (null,null)
      }
    }
  }

  /**
   * 计算窗口 : RawData  StepId 发生改变
   *
   * @param rawDataMatchedWindow
   * @param stateKey
   * @param readOnlyContext
   * @param collector
   */
  def windowEndCalcWindow(rawDataMatchedWindow: RawDataMatchedWindow,
                          runEventEndDataMatchedWindow: EventDataMatchedWindow,
                          isEventEnd:Boolean,
                          window2Data: Window2Data,
                          calcWindowObject: IControlWindow,
                          readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                          collector: Collector[JsonNode]) = {

    val calcWindowByRawDataOrEventEnd = if(isEventEnd){"eventEnd"}else{"rawData"}
    val matchedWindowId = if(isEventEnd){runEventEndDataMatchedWindow.matchedWindowId}else{rawDataMatchedWindow.matchedWindowId}
    val runEndTime = if(isEventEnd){runEventEndDataMatchedWindow.runEndTime}else{rawDataMatchedWindow.timestamp}
    val matchedControlPlanId = if(isEventEnd){runEventEndDataMatchedWindow.matchedControlPlanId}else{rawDataMatchedWindow.matchedControlPlanId}
    val windowPartitionId = if(isEventEnd){runEventEndDataMatchedWindow.windowPartitionId}else{rawDataMatchedWindow.windowPartitionId}

    // 获取eventStart  和  controlPlanConfig
    val eventStartDataMatchedWindow: EventDataMatchedWindow = windowEndEventStartState.value()
    val runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}"

    // parseRawData
    val rawDataSensorInfoTuple = parseRawData(runId, window2Data,readOnlyContext)
    val clipWindowByAllSensorDataValueList = rawDataSensorInfoTuple._1
    val rawDataSensorAliasMap: util.HashMap[String, ListBuffer[RawDataSensorData]] = rawDataSensorInfoTuple._2

    if (clipWindowByAllSensorDataValueList.nonEmpty && !rawDataSensorAliasMap.isEmpty) {
      // 构建dataflow
      val dataflow: IDataFlow = EngineFunction.buildWindow2DataFlowData(clipWindowByAllSensorDataValueList)
      dataflow.setStartTime(eventStartDataMatchedWindow.runStartTime)
      dataflow.setStopTime(runEndTime)
      calcWindowObject.attachDataFlow(dataflow)

      // 计算window
      val calcWindowStartTime: Long = DateTimeUtil.getCurrentTimestamp // 开始划window
      val apiControlWindow = new ApiControlWindow()
      val clipWindowResult: ClipWindowResult = apiControlWindow.split(calcWindowObject, isEventEnd)
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
            "windowType" -> MainFabConstants.windowTypeWindowEnd ,
            "runId" -> runId,
            "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
            "setStopTime" -> runEndTime,
            "subTaskId" -> subTaskId,
            "clipWindowResult" -> clipWindowResult,
            "calcWindowCostTime" -> (calcWindowEndTime - calcWindowStartTime))
        ))

      if(debugCalcWindowList.contains(matchedWindowId)){
        readOnlyContext.output(mainFabDebugInfoOutput,
          generateMainFabDebugInfo(Map[String, Any](
            "matchedControlPlanId" -> matchedControlPlanId,
            "matchedWindowId" -> matchedWindowId,
            "windowPartitionId" -> windowPartitionId,
            "windowType" -> MainFabConstants.windowTypeWindowEnd ,
            "runId" -> runId,
            "isEventEnd" -> isEventEnd,
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

        if (windowTimeRangeCurrentWindowIdList.nonEmpty) { // 说明window 正常切出来了

          val collectDataStart = System.currentTimeMillis()
          // 收集windowEnd  window 结果数据
          collectWindowResultData(eventStartDataMatchedWindow,
            runEventEndDataMatchedWindow,
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
                "windowType" -> MainFabConstants.windowTypeWindowEnd ,
                "runId" -> runId,
                "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
                "setStopTime" -> runEndTime,
                "subTaskId" -> subTaskId,
                "collectDataCostTime" -> (collectDataEnd - collectDataStart) )
            ))

          // 计算cycleCount
          calcCycleCount(eventStartDataMatchedWindow,
            window2Data,
            windowTimeRangeCurrentWindowIdList,
            readOnlyContext)

        } else {
          logger.error(s"[windowEnd-${calcWindowByRawDataOrEventEnd}] ==> 划窗口异常 -----0005C----- 划窗口结果异常:当前 WindowId 没有窗口结果数据 \n " +
            s"matchedControlPlanId == ${matchedControlPlanId} \n" +
            s"controlWindowId == $matchedWindowId ; \n" +
            s"windowPartitionId == $windowPartitionId ; \n" +
            s"windowType == ${MainFabConstants.windowTypeWindowEnd} ; \n" +
            s"runId == $runId ; ")

          // 该日志应该侧道输出到kafka ;
          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0005C",
              "calculateWindows",
              s"[windowEnd-${calcWindowByRawDataOrEventEnd}] ==> 划窗口异常 -----0005C----- 划窗口结果异常:当前 WindowId 没有窗口结果数据 ",
              Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
                "controlWindowId" -> matchedWindowId,
                "windowType" -> MainFabConstants.windowTypeWindowEnd ,
                "runId" -> runId,
                "clipWindowResult" -> clipWindowResult)
            ))
        }

        // window 正常切出来就清理所有的缓存数据
        cleanAllState()
        cleanWindowConfigAndCalcWindowObjMap(eventStartDataMatchedWindow.traceId + matchedWindowId)
      } else {
        logger.error(s"[windowEnd-${calcWindowByRawDataOrEventEnd}] ==> 划窗口异常 -----0004C----- 划窗口结果异常，请查看mes信息 \n" +
          s"matchedControlPlanId == ${matchedControlPlanId} \n" +
          s"controlWindowId == $matchedWindowId ; \n" +
          s"windowPartitionId == $windowPartitionId ; \n" +
          s"windowType == ${MainFabConstants.windowTypeWindowEnd} ; \n" +
          s"runId == $runId ; ")

        // 该日志应该侧道输出到kafka ;
        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0004C",
            "calculateWindows",
            s"[windowEnd-${calcWindowByRawDataOrEventEnd}] ==> 划窗口异常 -----0004C----- 划窗口结果异常，请查看mes信息 ",
            Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
              "controlWindowId" -> matchedWindowId,
              "windowType" -> MainFabConstants.windowTypeWindowEnd ,
              "runId" -> runId,
              "clipWindowResult" -> clipWindowResult)
          ))
      }

    } else {
      logger.error(s"[windowEnd-${calcWindowByRawDataOrEventEnd}] ==> 划窗口异常 -----0003C----- sensor信息有异常" +
        s"matchedControlPlanId == ${matchedControlPlanId} \n" +
        s"controlWindowId == $matchedWindowId ;\n " +
        s"windowPartitionId == $windowPartitionId ;\n " +
        s"windowType == ${MainFabConstants.windowTypeWindowEnd} ; \n" +
        s"runId == $runId ; ")

      // 该日志应该侧道输出到kafka ;
      readOnlyContext.output(mainFabLogInfoOutput,
        generateMainFabLogInfo("0003C",
          "calculateWindows",
          s"[windowEnd-${calcWindowByRawDataOrEventEnd}] ==> 划窗口异常 -----0003C----- sensor信息有异常",
          Map[String, Any]("matchedControlPlanId" -> matchedControlPlanId,
            "controlWindowId" -> matchedWindowId,
            "windowPartitionId" -> windowPartitionId,
            "windowType" -> MainFabConstants.windowTypeWindowEnd ,
            "runId" -> runId)
        ))
    }

    // 清理内存
    clipWindowByAllSensorDataValueList.clear()
    rawDataSensorAliasMap.clear()
  }


  /**
   * 处理 EventEnd 数据
   *
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processEventEnd(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])= {
    val eventEndDataMatchedWindow = toBean[EventDataMatchedWindow](inputValue)
    val traceId = eventEndDataMatchedWindow.traceId
    val matchedControlPlanId = eventEndDataMatchedWindow.matchedControlPlanId
    val matchedWindowId = eventEndDataMatchedWindow.matchedWindowId

    try{
      // 有eventStart 正常才开始计算窗口
      if(null != windowEndEventStartState.value()){

        val windowConfig_calcWidnowObject = createCalcWindowObject(readOnlyContext)
        val window2Data: Window2Data = windowConfig_calcWidnowObject._1
        val calcWindowObject: IControlWindow = windowConfig_calcWidnowObject._2

        if(null != window2Data && null != calcWindowObject){
          // windowEnd 切窗口
          windowEndCalcWindow(null,
            eventEndDataMatchedWindow,
            true,
            window2Data,
            calcWindowObject,
            readOnlyContext,collector)
        }
      }else{
        // eventStart 丢失 或者 eventEnd 早于 eventStart 或者windowEnd 已经划出了窗口
        giveUpData(inputValue,readOnlyContext)
      }
    }catch {
      case ex: Exception => logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> s"----处理 EventEnd 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }finally {
      cleanAllState()
      cleanWindowConfigAndCalcWindowObjMap(traceId + matchedWindowId)
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
   * 组装window结果数据 并输出
   * 以 sensorAlias为单位 输出结果
   *
   * @param eventStartDataMatchedWindow
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

    val windowSensorInfoList: List[WindowSensorInfo] = window2Data.windowSensorInfoList
    val windowPartitionId = eventStartDataMatchedWindow.windowPartitionId
//    val windowPartitionAllSensorList = window2Data.sensorAliasNameListMap.getOrElse(windowPartitionId,ListBuffer[String]()).distinct
    val sensorAliasNameList = window2Data.sensorAliasNameList.distinct

    windowSensorInfoList.foreach(windowSensorInfo =>{
      val sensorAliasName = windowSensorInfo.sensorAliasName
      if(sensorAliasNameList.contains(sensorAliasName)){
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
          val rawDataSensorDataListResult: ListBuffer[RawDataSensorData] = sensorDataList.filter(rawDataSensorData => {
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

//          // 需要侧道输出
//          logger.error(s"[WindowEnd] ==> windowDataList 为空 -----0006C----- ; \n " +
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
//              s"[WindowEnd] ==> windowDataList 为空 -----0006C----- ;",
//              Map[String,Any]("runId" -> runId,
//                "matchedControlPlanId" -> eventStartDataMatchedWindow.matchedControlPlanId,
//                "matchedWindowId" -> eventStartDataMatchedWindow.matchedWindowId,
//                "windowPartitionId" -> windowPartitionId,
//                "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
//                "setStopTime" -> eventStartDataMatchedWindow.runEndTime,
//                "sensorAliasName" -> sensorAliasName ,
//                "windowDataList" -> windowDataList.toJson,
//                "eventStartDataMatchedWindow" -> eventStartDataMatchedWindow,
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
//          "windowType" -> MainFabConstants.windowTypeWindowEnd ,
//          "runId" -> runId,
//          "setStartTime" -> eventStartDataMatchedWindow.runStartTime,
//          "setStopTime" -> eventEndDataMatchedWindow.runEndTime,
//          "collectSensorDataCastTime" -> collectSensorDataCastTime)
//      ))
  }

  /**
   * 计算cycleCount
   * @param eventStartDataMatchedWindow
   * @param window2Data
   * @param windowTimeRangeCurrentWindowIdList
   * @param eventStartMapStateKey
   * @param readOnlyContext
   */
  def calcCycleCount(eventStartDataMatchedWindow: EventDataMatchedWindow,
                     window2Data: Window2Data,
                     windowTimeRangeCurrentWindowIdList: List[ClipWindowTimestampInfo],
                     readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val traceId = eventStartDataMatchedWindow.traceId
    val runId = eventStartDataMatchedWindow.runId
    val matchedWindowId = eventStartDataMatchedWindow.matchedWindowId
    val windowPartitionId = eventStartDataMatchedWindow.windowPartitionId

    if(windowTimeRangeCurrentWindowIdList.nonEmpty  &&
      "partition_0".equalsIgnoreCase(windowPartitionId) &&
      (window2Data.controlWindowType == MainFabConstants.CycleWindowMaxType ||
        window2Data.controlWindowType == MainFabConstants.CycleWindowMinType)){

      if (!indicatorConfigByCycleCount.contains(matchedWindowId)) {
        logger.warn(ErrorCode("0260020008B", System.currentTimeMillis(), Map("matchedWindowId" -> matchedWindowId, "traceId" -> traceId,"runId" -> runId), "没有找到CycleCount indicator 配置").toJson)

        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0008B",
            "calculateWindows",
            s"[windowEnd] ==> -----0008B----- 没有找到CycleCount indicator 配置 ",
            Map[String, Any]("controlWindowId" -> matchedWindowId,
              "windowPartitionId" -> windowPartitionId,
              "windowType" -> MainFabConstants.windowTypeWindowEnd ,
              "runId" -> runId,
              "subTaskId" -> subTaskId ,
              "traceId" -> traceId)))
      }else{

        val cycleCount: Int = windowTimeRangeCurrentWindowIdList.size
        val matchedControlPlanConfig = eventStartDataMatchedWindow.matchedControlPlanConfig

        val runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}"

        val cycleCountIndicatorConfig = indicatorConfigByCycleCount(matchedWindowId)
        val productList = eventStartDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "")
        val stageList = eventStartDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "")

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
          missingRatio = eventStartDataMatchedWindow.dataMissingRatio,
          configMissingRatio = cycleCountIndicatorConfig.missingRatio,
          runStartTime = eventStartDataMatchedWindow.runStartTime,
          runEndTime = eventStartDataMatchedWindow.runEndTime,
          windowStartTime = eventStartDataMatchedWindow.runStartTime,
          windowEndTime = eventStartDataMatchedWindow.runEndTime,
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
   * 生产window结果数据
   * @param eventEndDataMatchedWindow
   * @param windowConfig
   * @param calcWindowDataResult
   * @param key
   * @return
   */
  def generateFdcWindowData(eventStartDataMatchedWindow: EventDataMatchedWindow,
                            eventEndDataMatchedWindow: EventDataMatchedWindow,
                            windowConfig: Window2Data,
                            windowDataList: List[WindowData]) = {

    val runEndTime = if(null == eventEndDataMatchedWindow){
      eventStartDataMatchedWindow.runEndTime
    }else{
      eventEndDataMatchedWindow.runEndTime
    }

    // 获取最大的controlPlanVersion
    val matchedControlPlanId = eventStartDataMatchedWindow.matchedControlPlanId
    val controlPlanVersion = if(controlPlanMaxVersionMap.contains(matchedControlPlanId)){controlPlanMaxVersionMap(matchedControlPlanId)}else{eventEndDataMatchedWindow.controlPlanVersion}

    val lotMESInfo = if(null == eventStartDataMatchedWindow.lotMESInfo || !eventStartDataMatchedWindow.lotMESInfo.nonEmpty){
        List(None)
      }else{
        eventStartDataMatchedWindow.lotMESInfo
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


    val matchedControlPlanConfig = eventStartDataMatchedWindow.matchedControlPlanConfig
    // 获取windowStart/EndTime
    val windowStartTime = windowDataList.head.startTime
    val windowEndTime = windowDataList.head.stopTime

    fdcWindowData("fdcWindowDatas",
      windowListData(
        toolName = eventStartDataMatchedWindow.toolName,
        toolId = matchedControlPlanConfig.toolId,
        chamberName = eventStartDataMatchedWindow.chamberName,
        chamberId = matchedControlPlanConfig.chamberId,
        recipeName = eventStartDataMatchedWindow.recipeName,
        recipeId = matchedControlPlanConfig.recipeId,
        runId = s"${eventStartDataMatchedWindow.toolName}--${eventStartDataMatchedWindow.chamberName}--${eventStartDataMatchedWindow.runStartTime}",
        dataMissingRatio = eventStartDataMatchedWindow.dataMissingRatio,
        contextId = 0,
        productName = eventStartDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else ""),
        stage = eventStartDataMatchedWindow.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else ""),
        controlWindowId = windowConfig.controlWindowId,
        controlPlanId = eventStartDataMatchedWindow.matchedControlPlanId,
        controlPlanVersion = controlPlanVersion,
        missingRatio = eventStartDataMatchedWindow.dataMissingRatio,
        windowStart = windowConfig.windowStart,
        windowStartTime = windowStartTime,
        windowEnd = windowConfig.windowEnd,
        windowEndTime = windowEndTime,
        windowTimeRange = windowEndTime - windowStartTime,
        runStartTime = eventStartDataMatchedWindow.runStartTime,
        runEndTime = runEndTime,
        windowEndDataCreateTime = System.currentTimeMillis(),
        windowType = "windowEnd2.0",
        DCType = eventStartDataMatchedWindow.DCType,
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
        materialName = eventStartDataMatchedWindow.materialName,
        pmStatus = eventStartDataMatchedWindow.pmStatus,
        pmTimestamp = eventStartDataMatchedWindow.pmTimestamp,
        area = matchedControlPlanConfig.area,
        section = matchedControlPlanConfig.section,
        mesChamberName = matchedControlPlanConfig.mesChamberName,
        lotMESInfo = resultLotMesInfo,
        windowDatasList = windowDataList,
        dataVersion = eventStartDataMatchedWindow.dataVersion
      ))
  }

  /**
   * debug 调试
   * @param eventEndDataMatchedWindow
   * @param readOnlyContext
   * @param matchedWindowId
   * @param eventStartDataMatchedWindow
   * @param runId
   * @param containerIControlWindow
   * @param rawDataList
   */
  def generaterDebugInfo(eventEndDataMatchedWindow: EventDataMatchedWindow,
                         readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                         matchedWindowId: Long,
                         eventStartDataMatchedWindow: EventDataMatchedWindow,
                         runId: String, containerIControlWindow: IControlWindow,
                         rawDataList: ListBuffer[(Long, Long, Seq[(String, Double, String)])]) = {
    if (debugConfig.contains(matchedWindowId)) {
      try {
        val debugResult = ApiControlWindow.getWindowSnapshotInfo(containerIControlWindow, "debugWindow")
        readOnlyContext.output(debugOutput, s"[windowEnd] ==> input controlWindowId: $matchedWindowId runId: $runId \t setStartTime:" +
          s" ${eventStartDataMatchedWindow.runStartTime} setStopTime: ${eventEndDataMatchedWindow.runEndTime} rawDataState：${rawDataList}")

        readOnlyContext.output(debugOutput, s"[windowEnd] ==> input controlWindowId: $matchedWindowId runId: $runId windowSnapshotInfo：${debugResult}")
      } catch {
        case ex: Exception =>
          readOnlyContext.output(debugOutput, ErrorCode("007006b005C", System.currentTimeMillis(), Map("debugWindow" ->
            "", "controlWindowId" -> matchedWindowId, "runId" -> runId, "triggerType" -> "windowEnd"), ExceptionInfo.getExceptionInfo(ex)).toJson)
      }
    }
  }

  /**
   * 收集乱序数据: rawData 早于 eventStart的数据
   * 收集丢失eventStart 的数据: eventStart丢失，rawData 和 eventEnd 都收集起来
   * @param inputValue
   * @param readOnlyContext
   */
  def giveUpData(inputValue: JsonNode,readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    // 侧道输出到指定kafka
//    readOnlyContext.output(calcWindowGiveUpRunStreamOutput,inputValue)
  }

  /**
   * 清空所有状态数据
   */
  def cleanAllState() = {
    cleanWindowEndEventStartState()
    cleanRawDataMapState()
    cleanRawDataLastCalcWindowStepIdTimeValueState()
    close()
  }

  /**
   * 清理 eventStartMapState 状态数据
   */
  def cleanWindowEndEventStartState() = {
    windowEndEventStartState.clear()
  }

  /**
   * 清理 rawDataMapState 状态数据
   */
  def cleanRawDataMapState() = {
    rawDataMapState.clear()
  }

  /**
   * 清理 runDataStepIdMapState 状态数据
   */
  def cleanRawDataLastCalcWindowStepIdTimeValueState() = {
    rawDataLastCalcWindowStepIdTimeValueState.clear()
  }

  /**
   * 清理 windowConfigAndCalcWindowObjMap
   * @param key
   * @return
   */
  def cleanWindowConfigAndCalcWindowObjMap(key:String) = {
    if(windowConfigAndCalcWindowObjMap.contains(key)){
      windowConfigAndCalcWindowObjMap.remove(key)
    }
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
   * @param window2Config
   * @return
   */
  def updateWindowConfigMap(window2Config: Window2Config, isOnLine:Boolean = false) = {
    val controlPlanId = window2Config.controlPlanId
    val controlPlanVersion = window2Config.controlPlanVersion
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
  def createContainerIControlWindow(window2Data: Window2Data,
                                    window2DataMap: Map[Long, Window2Data]):  IControlWindow = {

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
          logger.warn(ErrorCode("0260020001B",
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
        logger.error(ErrorCode("0260020002B",
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
  def parseWindow2ConfigByRedis(runId:String,controlPlanId:Long, controlPlanVersion:Long, configData: RedisWindowConfig,
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

//            // 拆分成分区模式
//            val window2DataPartition = parseWindow2DataPartition(window2Data)
            window2DataMap.put(window2Data.controlWindowId,window2Data)
          })
        }else{

          logger.error(ErrorCode("0260020010B", System.currentTimeMillis(),
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
              s"[windowEnd] ==> 读取 Redis 失败 -- 版本不匹配 -----0010B----- ",
              Map[String, Any]("controlPlanId" -> controlPlanId,
                "controlPlanVersion" -> controlPlanVersion,
                "redis_controlPlanId" -> redis_controlPlanId,
                "redis_controlPlanVersion" -> redis_controlPlanVersion,
                "runId" -> runId)))
        }
      }
      case _ =>{
        logger.error(ErrorCode("0260020011B", System.currentTimeMillis(),
          Map("msg" -> "读取 Redis 失败 -- optionCode 错误",
            "controlPlanId" -> controlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "optionCode" -> optionCode
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

          logger.error(ErrorCode("0260020009B", System.currentTimeMillis(),
            Map("msg" -> "读取Redis失败 --- 读出来的配置信息为空",
              "runId" -> runId,
              "controlPlanId" -> controlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "windowId" -> windowId,
              "windowConfigDataStr" -> windowConfigDataStr,
              "jedis" -> jedis.toString,
              "key" -> key,
              "filed" -> filed
            ),"").toString)

          readOnlyContext.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0009B",
              "getWindowConfigFromRedis",
              s"[windowEnd] ==> 读取Redis 失败 --- 读出来的配置信息为空 -----0009B----- ",
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
        logger.error(ErrorCode("0260020012B", System.currentTimeMillis(),
          Map("msg" -> "读取Redis 失败---jedis对象为空！",
            "runId" -> runId,
            "controlPlanId" -> controlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "windowId" -> windowId
          ), null).toString)

        readOnlyContext.output(mainFabLogInfoOutput,
          generateMainFabLogInfo("0012B",
            "getWindowConfigFromRedis",
            s"[windowEnd] ==> 读取Redis 失败 --- 读取Redis 失败---jedis对象为空！ -----0012B----- ",
            Map[String, Any]("controlPlanId" -> controlPlanId,
              "controlPlanVersion" -> controlPlanVersion,
              "windowId" -> windowId,
              "runId" -> runId,
              "jedis" -> jedis.toString)))

        new TrieMap[Long, Window2Data]
      }
    } catch {
      case exception: Exception => {
        logger.error(ErrorCode("0260020003B", System.currentTimeMillis(),
          Map("msg" -> "读取Redis 失败",
            "runId" -> runId,
            "controlPlanId" -> controlPlanId,
            "controlPlanVersion" -> controlPlanVersion,
            "windowId" -> windowId
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

    // 构建Redis线程池
    jedisSentinelPool = new JedisSentinelPool(redismaster,sentinel_set,config,redisconnectiontimeout)

//    try{
//      //从线程池中获取连接
//      jedis = jedisSentinelPool.getResource
//      jedis.select(ProjectConfig.REDIS_CONFIG_CACHE_DATABASE)
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
