package com.hzw.fdc.function.online.MainFabWindow2

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{controlPlanConfigList, readHbaseAllConfig}
import com.hzw.fdc.util.MainFabConstants.{IS_DEBUG, chamberName, eventEnd}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.util.Collections
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

class MainFabMatchControlPlanAndWindowsBroadcastProcessFunction(_windowType :String) extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,  JsonNode] {

  private var windowType:String = _

  /**
   * 025:MainFabProcessEndWindow2Service
   * 002:切窗口
   */
  val job_fun_DebugCode:String = "025001"
  val jobName:String = "MainFabProcessEndWindow2Service"
  val optionName : String = "MainFabMatchControlPlanAndWindowsBroadcastProcessFunction"

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabMatchControlPlanAndWindowsBroadcastProcessFunction])

  lazy val matchControlPlanAndWindowGiveUpRunStreamOutput = new OutputTag[JsonNode]("matchControlPlanAndWindowGiveUpRunStream")

  // 侧道输出日志信息
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")

  lazy val mainFabDebugInfoOutput = new OutputTag[String]("mainFabDebugInfo")

  /**
   * 缓存controlPlanConfig的配置
   * key1: toolName|chamberName|recipeName[|productName|stageName]
   * value: TrieMap[Long,ControlPlanConfig]
   *    key2: controlPlanId
   *    value : ControlPlanConfig
   * 为什么需要两层Map来缓存controlPlan的配置？
   *    1- key1 的设计师为了Run 匹配controlPlan方便;
   *    2- 由于controlPlan by product  或者 by stage 时可以交叉配置,所以相同的key1 可能有多个controlPlan
   */
  val controlPlanConfigMap = new TrieMap[String,TrieMap[Long,ControlPlanConfig]]()


  /**
   * controlPlanConfig 策略优化
   * 缓存controlPlanConfig的配置
   * key1: toolName|chamberName|recipeName
   * value: List[controlPlanId]
   */
  val matchControlPlanConfig2Map = new TrieMap[String,Set[Long]]()

  /**
   * controlPlanConfig 策略优化
   * 缓存controlPlanConfig的配置
   * key1: controlPlanId
   * value: ControlPlanConfigInfo
   */
  val controlPlanConfig2Map = new TrieMap[Long,ControlPlanConfig2]()


  /**
   * 缓存每个controlPlan 所有的windowId
   * key:controlPlanId
   * value List[MatchWindowPartitionConfig]
   */
//  private val windowPartitionConfigMap = new TrieMap[Long,List[MatchWindowPartitionConfig]]()

  /**
   * 缓存每个controlPlan 所有的windowId
   * key:controlPlanId
   * value List[MatchWindowConfig]
   */
  private val matchWindowConfigMap = new TrieMap[Long,List[MatchWindowConfig]]()

  /**
   *  缓存 当前Run匹配了哪些 controlPlan 以及controlPlan下有哪些windows;
   *  key : traceId  ; 在 eventEnd 时需要清除;
   *  value : List[(ControlPlanConfig,List[MatchWindowPartitionConfig])];
   *
   */
//  private val runMatchedWindowPartitionMap = new TrieMap[String,List[(ControlPlanConfig,List[MatchWindowPartitionConfig])]]
  private val runMatchedWindowMap = new TrieMap[String,List[(ControlPlanConfig,List[MatchWindowConfig])]]


  /**
   * debug 指定的 tool|chamber|recipe 匹配上的 controlPlan 和 window
   * 当策略信息中 status == true 添加 tool|chamber|recipe
   * 当策略信息中 status == false 删除 tool|chamber|recipe
   */
  private val debugMatchControlPlanList = new ListBuffer[String]()

  // 缓存eventStart :  用于 eventStart 正常匹配到了controlPlan 和 window
  private var eventStartState:ValueState[RunEventData] = _

  // 缓存eventEnd :  用于 eventEnd 比 eventStart 早到, 先缓存eventEnd
  private var eventEndState:ValueState[JsonNode] = _

  // 缓存eventStart :  用于 eventStart 没有匹配到 controlPlan 和 window  说明该Run的 rawData 和 eventEnd 可以直接丢弃
  private var giveUpEventStartState:ValueState[RunEventData] = _

  //缓存RawData数据 : 用于 rawData 比 eventStart 早到, 先缓存这部分的rawData
//  private var earlyRawDataListState :ListState[JsonNode]=_
  private var earlyRawDataMapState :MapState[Long,JsonNode]=_

  // 缓存eventStart :  用于 eventStart 所带的recipe错误时 eventStart的数据
  private var eventStartRecipeErrorState:ValueState[RunEventData] = _

  // 缓存RawData 数据 : 用于 eventStart 所带的recipe错误时 rawData 的数据
  private var recipeErrorRawDataMapState :MapState[Long,JsonNode]=_

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    windowType = _windowType

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    if(!IS_DEBUG){
      val startTime = System.currentTimeMillis()

      // 初始化ControlPlanConfig
//      val controlPlanConfigList: ListBuffer[ConfigData[List[ControlPlanConfig]]] = if(ProjectConfig.MAINFAB_READ_HBASE_CONFIG_ONCE){
//        InitFlinkFullConfigHbase.controlPlanConfigList
//      }else{
//        readHbaseAllConfig[List[ControlPlanConfig]](ProjectConfig.HBASE_SYNC_CONTROL_PLAN_TABLE, MainFabConstants.controlPlanConfig)
//      }
//      controlPlanConfigList.foreach(parseControlPlanConfig)


      val controlPlanConfigList: ListBuffer[ConfigData[ControlPlanConfig2]] = if(ProjectConfig.MAINFAB_READ_HBASE_CONFIG_ONCE){
        InitFlinkFullConfigHbase.controlPlanConfig2List
      }else{
        readHbaseAllConfig[ControlPlanConfig2](ProjectConfig.HBASE_SYNC_CONTROLPLAN2_TABLE, MainFabConstants.controlPlanConfig2)
      }
      controlPlanConfigList.foreach(parseControlPlanConfig2)

      // 初始化window2Config
      val window2ConfigList: ListBuffer[ConfigData[Window2Config]] = if(ProjectConfig.MAINFAB_READ_HBASE_CONFIG_ONCE){
        InitFlinkFullConfigHbase.Window2ConfigList
      }else{
        readHbaseAllConfig[Window2Config](ProjectConfig.HBASE_SYNC_WINDOW2_TABLE, MainFabConstants.window2Config)
      }
      window2ConfigList.foreach(parseWindowConfig(_))

      val endTIme = System.currentTimeMillis()
      logger.warn("--- parse Hbase Config Total Time: " + (endTIme - startTime))
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

    //   初始化状态变量 processEndEventStartState
    val processEndEventStartStateDescription = new
        ValueStateDescriptor[RunEventData]("processEndEventStartState", TypeInformation.of(classOf[RunEventData]))
    // 设置过期时间
    processEndEventStartStateDescription.enableTimeToLive(hour26TTLConfig)
    eventStartState = getRuntimeContext.getState(processEndEventStartStateDescription)

    //   初始化状态变量 processEndEventEndState
    val processEndEventEndStateDescription = new
        ValueStateDescriptor[JsonNode]("processEndEventEndState", TypeInformation.of(classOf[JsonNode]))
    // 设置过期时间
    processEndEventEndStateDescription.enableTimeToLive(hour26TTLConfig)
    eventEndState = getRuntimeContext.getState(processEndEventEndStateDescription)

    //   初始化状态变量 processEndGiveUpEventStartState
    val processEndGiveUpEventStartStateDescription = new
        ValueStateDescriptor[RunEventData]("processEndGiveUpEventStartState", TypeInformation.of(classOf[RunEventData]))
    // 设置过期时间
    processEndGiveUpEventStartStateDescription.enableTimeToLive(hour26TTLConfig)
    giveUpEventStartState = getRuntimeContext.getState(processEndGiveUpEventStartStateDescription)

    // 初始化 earlyRawDataMapState
    val earlyRawDataMapStateDescription: MapStateDescriptor[Long,JsonNode] = new
        MapStateDescriptor[Long,JsonNode]("earlyRawDataMapState", TypeInformation.of(classOf[Long]),TypeInformation.of(classOf[JsonNode]))
    // 设置过期时间
    earlyRawDataMapStateDescription.enableTimeToLive(hour26TTLConfig)
    earlyRawDataMapState = getRuntimeContext.getMapState(earlyRawDataMapStateDescription)

    //   初始化状态变量 processEndEventStartRecipeErrorState
    val processEndEventStartRecipeErrorStateDescription = new
        ValueStateDescriptor[RunEventData]("processEndEventStartRecipeErrorState", TypeInformation.of(classOf[RunEventData]))
    // 设置过期时间
    processEndEventStartRecipeErrorStateDescription.enableTimeToLive(hour26TTLConfig)
    eventStartRecipeErrorState = getRuntimeContext.getState(processEndEventStartRecipeErrorStateDescription)

    // 初始化 recipeErrorRawDataMapState
    val recipeErrorRawDataMapStateDescription: MapStateDescriptor[Long,JsonNode] = new
        MapStateDescriptor[Long,JsonNode]("recipeErrorRawDataMapState", TypeInformation.of(classOf[Long]),TypeInformation.of(classOf[JsonNode]))
    // 设置过期时间
    recipeErrorRawDataMapStateDescription.enableTimeToLive(hour26TTLConfig)
    recipeErrorRawDataMapState = getRuntimeContext.getMapState(recipeErrorRawDataMapStateDescription)

  }

  override def processElement(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]): Unit = {
    try {
//      val toolName = inputValue.get("toolName").asText("")
//      if(toolName.contains("tom")){
//      }
      val dataType = inputValue.get(MainFabConstants.dataType).asText()

      if(dataType == MainFabConstants.eventStart){

        //   处理 EventStart 数据
        processEventStart(inputValue, readOnlyContext, collector)
      } else if(dataType == MainFabConstants.eventEnd){

        //   处理 EventEnd 数据
        processEventEnd(inputValue, readOnlyContext, collector)

      }else if(dataType == MainFabConstants.rawData){

        //   处理 RawData 数据
        processRawData(inputValue, readOnlyContext, collector)
      }
    }catch {
      case ex: Exception => logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> "匹配 context window 失败 : "), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }

  /**
   * 处理 eventStart 带的recipe 错误的流程
   *  1- 使用eventEnd 来匹配 controlPlan 和 window
   *
   * @param runEventEndData
   * @param readOnlyContext
   * @param collector
   */
  def processRecipeError(runEventEndData: RunEventData,
                         readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                         collector: Collector[JsonNode]) = {
    val traceId: String = runEventEndData.traceId
    //   匹配controlPlan
//    val matchedControlPlanMaxScoreListOption: Option[List[MatchedControlPlanScore]] = runEventDataMatchControlPlan(runEventEndData)
    val matchedControlPlanMaxScore2ListOption: Option[List[MatchedControlPlanScore2]] = runEventDataMatchControlPlanConfig2(runEventEndData)


    if(matchedControlPlanMaxScore2ListOption.nonEmpty && matchedControlPlanMaxScore2ListOption.get.nonEmpty){
      //   匹配window
//      eventStartMatchWindow(traceId,matchedControlPlanMaxScoreListOption)
      eventStartMatchWindow2(traceId,matchedControlPlanMaxScore2ListOption)
      if(runMatchedWindowMap.contains(traceId) && runMatchedWindowMap.get(traceId).get.nonEmpty){

        // 正常匹配到了CP 和 WIN 根据Debug条件信息侧道输出匹配的信息
        debugMatchControlPlanAndWindow(traceId,runEventEndData.toolName,runEventEndData.chamberName,runEventEndData.recipeName,readOnlyContext)

        // 处理eventStart
        val runEventStartData = eventStartRecipeErrorState.value()
        collectEventStart(runEventStartData,readOnlyContext,collector)

        // 处理rawData
        if(null != recipeErrorRawDataMapState.values()){
          val length = recipeErrorRawDataMapState.values().iterator().length
          logger.error(s"处理recipeError or materialActual = false 流程 length == ${length} ;\n " +
            s"runEventStartData = ${runEventStartData.toJson}")
          val it = recipeErrorRawDataMapState.values().iterator()
          while (it.hasNext){
            val rawDataJson: JsonNode = it.next()
            val rawData = toBean[MainFabRawData](rawDataJson)
            collectRawData(rawData,readOnlyContext,collector)
          }
        }else{
          logger.error(s"recipe Error recipeErrorRawDataMapState.values is null")
        }

        // 处理早到的rawData
        if(null != earlyRawDataMapState.values()){

          val it = earlyRawDataMapState.values().iterator()
          while (it.hasNext){
            val rawDataJson: JsonNode = it.next()
            val rawData = toBean[MainFabRawData](rawDataJson)
            collectRawData(rawData,readOnlyContext,collector)
          }
        }

        // 处理eventEnd
        collectEventEnd(runEventEndData,readOnlyContext,collector)
      }else{
        logger.error(s"recipe Error Window 失败! eventEnd == ${runEventEndData}")
      }
    }else{
      logger.error(s"recipe Error MatchControlPlan 失败! eventEnd == ${runEventEndData}")
    }
  }


  /**
   * 处理 eventEnd 数据
   *
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processEventEnd(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])= {
    val eventEndData = toBean[RunEventData](inputValue)
    val traceId = eventEndData.traceId

    try{
      if(null != eventStartRecipeErrorState.value()){
        logger.error(s"recipe Error eventEnd == ${inputValue}")
        // todo 需要侧道输出到kafka

        // 说明eventStart 所带的 recipe 是错误的， 需要使用eventEnd 来匹配controlPlan 和window
        processRecipeError(eventEndData,readOnlyContext,collector)
      }else if(null != giveUpEventStartState.value()){
        // 如果当前run 没有匹配到controlPlan 和 window 直接侧道输出
        giveUpData(inputValue,readOnlyContext)
      }else if(runMatchedWindowMap.contains(traceId) && runMatchedWindowMap.get(traceId).get.nonEmpty){
        // 以window为单位 分发eventEnd 数据
        collectEventEnd(eventEndData,readOnlyContext,collector)
      }else{
        val runEventStartData: RunEventData = eventStartState.value()
        if(null == runEventStartData){
          logger.error(s"early eventEnd --> \n " +
            s"runEventStartData == ${runEventStartData} \n " +
            s"eventEndData == ${eventEndData}")

          // 处理 eventEnd数据早于 eventStart数据
          eventEndState.update(inputValue)
        }else{
          // 处理自动重启后需要重新使用eventStart 去匹配controlPlan  和 window
          retryMatchControlPlanAndWindow(eventEndData,readOnlyContext,collector)
          if(runMatchedWindowMap.contains(traceId) && runMatchedWindowMap.get(traceId).get.nonEmpty){
            // 以window为单位 分发eventEnd 数据
            collectEventEnd(eventEndData,readOnlyContext,collector)
          }else{
            // 处理早到的数据 或者 没有匹配到controlPlan 和 Window的数据
            giveUpData(inputValue,readOnlyContext)
          }
        }
      }
    }catch {
      case ex: Exception => logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> s"----处理 EventEnd 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 处理rawData 数据
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processRawData(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])= {
    val rawData = toBean[MainFabRawData](inputValue)
    val traceId = rawData.traceId
    try{
      if(null != eventStartRecipeErrorState.value()){
//        logger.error(s"recipe Error rawData == ${inputValue}")
        // 缓存 recipe 错误的 RawData 数据
        val timestamp = rawData.timestamp
        recipeErrorRawDataMapState.put(timestamp,inputValue)
      }else if(null != giveUpEventStartState.value()){
        // 如果当前run 没有匹配到controlPlan 和 window 直接侧道输出
        giveUpData(inputValue,readOnlyContext)
      }else if(runMatchedWindowMap.contains(traceId) && runMatchedWindowMap.get(traceId).get.nonEmpty){
        // 以window 为单位 分发rawData 数据
        collectRawData(rawData,readOnlyContext,collector)
      }else{
        val runEventStartData: RunEventData = eventStartState.value()
        if(null == runEventStartData){
          // 处理早到的数据  缓存起来;
          val timestamp = rawData.timestamp
          earlyRawDataMapState.put(timestamp,inputValue)
        }else{
          // 处理自动重启后需要重新使用eventStart 去匹配controlPlan  和 window
          retryMatchControlPlanAndWindow(runEventStartData,readOnlyContext,collector)
          if(runMatchedWindowMap.contains(traceId) && runMatchedWindowMap.get(traceId).get.nonEmpty ){
            // 以window 为单位 分发rawData 数据
            collectRawData(rawData,readOnlyContext,collector)
          }else{
            //   处理早到的数据 或者 没有匹配到controlPlan 和 Window的数据
            giveUpData(inputValue,readOnlyContext)
          }
        }
      }
    }catch {
      case ex: Exception => logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> s"----处理 RawData 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 处理 eventStart 数据
   *
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   * @return
   */
  def processEventStart(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])={
    val eventStartData = toBean[RunEventData](inputValue)
    val traceId = eventStartData.traceId
    try{

//      logger.warn(s"processEventStart : inputValue == ${inputValue} \n " +
//        s"MainFabConstants.windowTypeProcessEnd == windowType is ${MainFabConstants.windowTypeProcessEnd == windowType}")
      if((!eventStartData.recipeActual || !eventStartData.materialActual) && MainFabConstants.windowTypeProcessEnd == windowType){

        logger.error(s"recipe Error eventStart == ${inputValue}")
        // todo 需要侧道输出到kafka

        // 缓存eventStart 带的recipe 数据错误
        eventStartRecipeErrorState.update(eventStartData)

        // 重新处理早到的 eventEnd
        processEarlyEventEnd(traceId,readOnlyContext,collector)
      }else{
        // 匹配controlPlan
//        val matchedControlPlanMaxScoreListOption: Option[List[MatchedControlPlanScore]] = runEventDataMatchControlPlan(eventStartData)
        val matchedControlPlanMaxScore2ListOption: Option[List[MatchedControlPlanScore2]] = runEventDataMatchControlPlanConfig2(eventStartData)

        if(matchedControlPlanMaxScore2ListOption.nonEmpty && matchedControlPlanMaxScore2ListOption.get.nonEmpty){

          // 匹配window
//          eventStartMatchWindow(traceId,matchedControlPlanMaxScoreListOption)
          // 匹配window
          eventStartMatchWindow2(traceId,matchedControlPlanMaxScore2ListOption)

          if(runMatchedWindowMap.contains(traceId) && runMatchedWindowMap.get(traceId).get.nonEmpty){
            // 正常匹配到了CP 和 WIN 根据Debug条件信息侧道输出匹配的信息
            debugMatchControlPlanAndWindow(traceId,eventStartData.toolName,eventStartData.chamberName,eventStartData.recipeName,readOnlyContext)

            // 以window 为单位分发eventStart 数据;
            collectEventStart(eventStartData,readOnlyContext,collector)

            // 缓存eventStart状态(eventStart 正常匹配到了controlPlan 和 window)
            eventStartState.update(eventStartData)

            // 重新处理早到的 rawData
            processEarlyRawData(traceId,readOnlyContext,collector)

            // 重新处理早到的 eventEnd
            processEarlyEventEnd(traceId,readOnlyContext,collector)
          }else{
            // 没有匹配到 window 的Run 需要测到输出
            giveUpEventStartState.update(eventStartData)
            giveUpData(inputValue,readOnlyContext)
          }
        }else{
          // 没有匹配到controlPlan 的Run 需要测到输出
          giveUpEventStartState.update(eventStartData)
          logger.error(s"没有匹配到controlPlan 的Run 需要侧道输出 eventStartData == ${eventStartData.toJson}")
          giveUpData(inputValue,readOnlyContext)
        }
      }

    }catch {
      case ex: Exception => {
        //   注意: 如果eventStart 匹配controlPlan 和 windwo 异常, 该Run的所有数据直接丢弃, 保证异常数据不能流入划window的任务;
        giveUpEventStartState.update(eventStartData)
        logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
          Map("msg" -> s"0---- 匹配 controlPlan and window 失败 : 处理 eventStart 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
      }
    }
  }

  /**
   * 重新处理早到的rawData
   * @param traceId
   * @param readOnlyContext
   * @param collector
   */
  def processEarlyRawData(traceId:String, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]) = {

//    val it = earlyRawDataListState.get().iterator()
    var count: Int = 0
    if(null != earlyRawDataMapState.values()){
      val it = earlyRawDataMapState.values().iterator()
      while(it.hasNext){
        val inputValue: JsonNode = it.next()
        val rawData = toBean[MainFabRawData](inputValue)
        processRawData(inputValue,readOnlyContext,collector)
        count += 1
      }
      if(count>0){
        logger.warn(s"处理早到的RawData 数据: traceId == ${traceId} ; count == ${count} ")
      }
      earlyRawDataMapState.clear()
    }
  }


  /**
   * 重新处理早到的eventEnd
   * @param traceId
   * @param readOnlyContext
   * @param collector
   */
  def processEarlyEventEnd(traceId:String, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]) = {

    val runEventEndData = eventEndState.value()
    if(null != runEventEndData){
      logger.error(s"处理早到的EventEnd 数据: runEventEndData == ${runEventEndData}  ")
      processEventEnd(runEventEndData,readOnlyContext,collector)
      eventEndState.clear()
    }
  }



  /**
   * eventStart 匹配window
   * @param matchedControlPlanIdListOption
   * @return
   */
  def eventStartMatchWindow(traceId:String,matchedControlPlanMaxScoreListOption:  Option[List[MatchedControlPlanScore]]) = {

    val tuples: List[(ControlPlanConfig, List[MatchWindowConfig])] = matchedControlPlanMaxScoreListOption.get.flatMap(matchedControlPlanMaxScore => {

      val matchedControlPlanConfigList = matchedControlPlanMaxScore.matchedControlPlanConfigList

      matchedControlPlanConfigList.map((matchedControlPlanConfig: ControlPlanConfig) => {

        val matchedControlPlanId = matchedControlPlanConfig.controlPlanId
        val matchWindowConfigList: List[MatchWindowConfig] = matchWindowConfigMap.getOrElse(matchedControlPlanId, null)

//        logger.warn(s"matchWindowConfigList == ${matchWindowConfigList.toJson}")
        (matchedControlPlanConfig, matchWindowConfigList)
      })
    }).filter((tuple: (ControlPlanConfig, List[MatchWindowConfig])) => {
      null != tuple._2
    })

    // 缓存
    runMatchedWindowMap.put(traceId,tuples)
  }

  /**
   * eventStart 匹配window
   * @param matchedControlPlanIdListOption
   * @return
   */
  def eventStartMatchWindow2(traceId:String, matchedControlPlanMaxScore2ListOption: Option[List[MatchedControlPlanScore2]]) = {

    val tuples: List[(ControlPlanConfig, List[MatchWindowConfig])] = matchedControlPlanMaxScore2ListOption.get.map(matchedControlPlanMaxScore2 => {

      val matchedControlPlanConfig: ControlPlanConfig = matchedControlPlanMaxScore2.matchControlPlanConfig
      val matchedControlPlanId = matchedControlPlanConfig.controlPlanId
      val matchWindowConfigList: List[MatchWindowConfig] = matchWindowConfigMap.getOrElse(matchedControlPlanId, null)
      (matchedControlPlanConfig, matchWindowConfigList)

    }).filter((tuple: (ControlPlanConfig, List[MatchWindowConfig])) => {
      null != tuple._2
    })

    // 缓存
    runMatchedWindowMap.put(traceId,tuples)
  }

  /**
   * Run 开始匹配controlPlan
   *
   * @param runEventData
   * @return
   */
  def runEventDataMatchControlPlanConfig2(runEventData: RunEventData) = {
    try{

      //      val matchedControlPlanScoreList: List[MatchedControlPlanScore] = matchControlPlanByScore(runEventData,key_tool_chamber_recipe)
      val matchedControlPlanScore2List: List[MatchedControlPlanScore2] = matchControlPlanByScore2(runEventData)

      //      val matchedControlPlanMaxScoreList: Option[List[MatchedControlPlanScore]] = getMatchedControlPlanMaxScoreList(matchedControlPlanScoreList)
      val matchedControlPlanMaxScore2List: Option[List[MatchedControlPlanScore2]] = getMatchedControlPlanMaxScore2List(matchedControlPlanScore2List)

      matchedControlPlanMaxScore2List
    }catch {
      case ex: Exception => {
        logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
          Map("msg" -> s"1---- 匹配 controlPlan 失败 ! runEventData == ${runEventData.toJson}"),
          ExceptionInfo.getExceptionInfo(ex)).toJson)

        None
      }
    }
  }


  /**
   * Run 开始匹配controlPlan
   *
   * @param runEventData
   * @return
   */
  def runEventDataMatchControlPlan(runEventData: RunEventData) = {
    try{
      val toolName = runEventData.toolName
      val chamberName = runEventData.chamberName
      val recipeName = runEventData.recipeName
      val key_tool_chamber_recipe = s"${toolName}|${chamberName}|${recipeName}"
      val matchedControlPlanScoreList: List[MatchedControlPlanScore] = matchControlPlanByScore(runEventData,key_tool_chamber_recipe)

      val matchedControlPlanMaxScoreList: Option[List[MatchedControlPlanScore]] = getMatchedControlPlanMaxScoreList(matchedControlPlanScoreList)

      matchedControlPlanMaxScoreList
    }catch {
      case ex: Exception => {
        logger.error(ErrorCode("ff2007d012C", System.currentTimeMillis(),
          Map("msg" -> s"1---- 匹配 controlPlan 失败 ! eventStartData == ${runEventData.toJson}"),
          ExceptionInfo.getExceptionInfo(ex)).toJson)

        None
      }
    }
  }


  /**
   * 根据lotInfo 匹配controlPlanScore
   * @param runEventData
   * @param key_tool_chamber_recipe
   * @return
   */
  def matchControlPlanByScore(runEventData: RunEventData, key_tool_chamber_recipe:String) = {

    val lotMESInfo = runEventData.lotMESInfo
    val matchedControlPlanScoreList: List[MatchedControlPlanScore] =  if(lotMESInfo.nonEmpty){
      lotMESInfo.map(lot => {
        val lotInfo = lot.get
        val productName: String = lotInfo.product.get
        val stageName: String = lotInfo.stage.get

        //   1- by product + stage  : score = 3
        val key_tool_chamber_recipe_by_product_stage = s"${key_tool_chamber_recipe}|${productName}|${stageName}"
        val matchedControlPlanInfoList_by_product_stage = matchControlPlan(key_tool_chamber_recipe_by_product_stage)
        if (matchedControlPlanInfoList_by_product_stage.nonEmpty) {
          MatchedControlPlanScore(3, matchedControlPlanInfoList_by_product_stage.get)
        } else {
          //   2- by stage  : score = 2
          val key_tool_chamber_recipe_by_stage = s"${key_tool_chamber_recipe}||${stageName}"
          val matchedControlPlanIdList_by_stage = matchControlPlan(key_tool_chamber_recipe_by_stage)
          if (matchedControlPlanIdList_by_stage.nonEmpty) {
            MatchedControlPlanScore(2, matchedControlPlanIdList_by_stage.get)
          } else {

            //   3- by product  : score = 1
            val key_tool_chamber_recipe_by_product = s"${key_tool_chamber_recipe}|${productName}|"
            val matchedControlPlanIdList_by_product = matchControlPlan(key_tool_chamber_recipe_by_product)
            if (matchedControlPlanIdList_by_product.nonEmpty) {
              MatchedControlPlanScore(1, matchedControlPlanIdList_by_product.get)
            } else {

              //   4- by tool + chamber + recipe 需要兜底 : score = 0
              val matchedControlPlanIdList_by = matchControlPlan(key_tool_chamber_recipe+"||")
              if (matchedControlPlanIdList_by.nonEmpty) {
                MatchedControlPlanScore(0, matchedControlPlanIdList_by.get)
              } else {

                //   5- 匹配失败
                MatchedControlPlanScore(-1, null)
              }
            }
          }
        }
      }).filter(matchedControlPlan => {
        //   过滤  score >= 0
        matchedControlPlan.score >= 0
      })
    }else{
      //   4- by tool + chamber + recipe 需要兜底 : score = 0
      val matchedControlPlanIdList_by = matchControlPlan(key_tool_chamber_recipe+"||")
      if (matchedControlPlanIdList_by.nonEmpty) {
        val matchedControlPlanScore = MatchedControlPlanScore(0, matchedControlPlanIdList_by.get)
        List[MatchedControlPlanScore](matchedControlPlanScore)
      }else{
        List[MatchedControlPlanScore]()
      }
    }

    matchedControlPlanScoreList
  }


  /**
   * 根据lotInfo 匹配controlPlanScore
 *
   * @param runEventData
   * @param key_tool_chamber_recipe
   * @return
   */
  def matchControlPlanByScore2(runEventData: RunEventData) = {

    val toolName = runEventData.toolName
    val chamberName = runEventData.chamberName
    val recipeName = runEventData.recipeName

    val key_tool_chamber_recipe = s"${toolName}|${chamberName}|${recipeName}"

    // 先捞出tool + chamber + recipe 级别的contorlPlanIdList
    val controlPlanIdSet: Set[Long] = matchControlPlanConfig2Map.getOrElse(key_tool_chamber_recipe, Set[Long]())

    val matchedControlPlanScore2ResultList: List[MatchedControlPlanScore2] = if(controlPlanIdSet.nonEmpty){
      val lotMESInfo = runEventData.lotMESInfo
      if(lotMESInfo.nonEmpty){// 数据源eventStart 带有product 和 stage
        processHiveLotInfo(toolName, recipeName, lotMESInfo,controlPlanIdSet)
      }else{ // 数据源eventStart 没有product 和 stage
        processNoLotInfo(toolName, recipeName, controlPlanIdSet)
      }
    }else{
      // 如果 controlPlanIdSet 为空 匹配失败
      logger.error(s"没有匹配到controlPlan controlPlanIdSet为空! traceid == ${runEventData.traceId}")
      List[MatchedControlPlanScore2]()
    }

    matchedControlPlanScore2ResultList
  }

  /**
   * 如果数据源 中有带product 和 stage
   * 按照一下顺序匹配
   *
   * @param toolName
   * @param recipeName
   * @param lotMESInfo
   * @param controlPlanIdSet
   * @return
   */
  private def processHiveLotInfo(toolName: String, recipeName: String, lotMESInfo: List[Option[Lot]],controlPlanIdSet: Set[Long]) = {
    val matchedControlPlanScore2List: List[MatchedControlPlanScore2] = lotMESInfo.flatMap(lot => {
      val lotInfo = lot.get
      val productName = lotInfo.product.get
      val stageName = lotInfo.stage.get
      controlPlanIdSet.map(controlPlanId => {
        val controlPlanConfig2: ControlPlanConfig2 = controlPlanConfig2Map.getOrElse(controlPlanId, null)
        if (null != controlPlanConfig2) {
          val productNameList = controlPlanConfig2.productNameList
          val stageNameList = controlPlanConfig2.stageNameList

          // 判断 productNameList 是否为空
          val productIsEmpty = judgeListIsEmpty(productNameList)
          // 判断 stageNameList 是否为空
          val stageIsEmpty = judgeListIsEmpty(stageNameList)

          val controlPlanConfig: ControlPlanConfig = generateControlPlanConfig(toolName, chamberName, recipeName, productName, stageName, controlPlanConfig2)

          if (!productIsEmpty && productNameList.contains(productName) && // 匹配 product + stage : score = 3;
            !stageIsEmpty && stageNameList.contains(stageName)) {
            MatchedControlPlanScore2(3, controlPlanConfig)
          } else if (productIsEmpty && // 匹配 stage : score = 2 ;
            !stageIsEmpty && stageNameList.contains(stageName)) {
            MatchedControlPlanScore2(2, controlPlanConfig)
          } else if (!productIsEmpty && productNameList.contains(productName) &&
            stageIsEmpty) { // 匹配 product : score = 1 ;
            MatchedControlPlanScore2(1, controlPlanConfig)
          } else if (productIsEmpty && stageIsEmpty) { // 匹配 兜底 : score = 0 ;
            MatchedControlPlanScore2(0, controlPlanConfig)
          } else { // 匹配失败 score = -1 ;
            MatchedControlPlanScore2(-1, controlPlanConfig)
          }
        } else { // 匹配失败 score = -1 ;
          MatchedControlPlanScore2(-1, null)
        }
      })
    }).filter(matchedControlPlanScore2 => {
      matchedControlPlanScore2.score >= 0 && null != matchedControlPlanScore2.matchControlPlanConfig
    })
    matchedControlPlanScore2List
  }

  /**
   * 如果数据源 中没有带product 和 stage
   * 那么就只能匹配兜底级别的contorlPlan (by tool + chamber + recipe)
   * 所以controlPlanConfig2 中的productNameList 和 stageNameList 都是空的才能匹配上
   *
   * @param toolName
   * @param recipeName
   * @param controlPlanIdSet
   * @return
   */
  def processNoLotInfo(toolName: String, recipeName: String, controlPlanIdSet: Set[Long]) = {
    val matchedControlPlanScore2List: List[MatchedControlPlanScore2] = controlPlanIdSet.map(controlPlanId => {
      val controlPlanConfig2: ControlPlanConfig2 = controlPlanConfig2Map.getOrElse(controlPlanId, null)

      if (null != controlPlanConfig2 ) {
        val productNameList = controlPlanConfig2.productNameList
        val stageNameList = controlPlanConfig2.stageNameList
        val productIsEmpty = judgeListIsEmpty(productNameList)
        val stageIsEmpty = judgeListIsEmpty(stageNameList)

        if(productIsEmpty && stageIsEmpty ){
          // 组装singleControlPlanConfig2
          val controlPlanConfig: ControlPlanConfig = generateControlPlanConfig(toolName, chamberName, recipeName, "", "", controlPlanConfig2)
          val matchedControlPlanScore2 = MatchedControlPlanScore2(0, controlPlanConfig)
          matchedControlPlanScore2
        }else{
          MatchedControlPlanScore2(-1, null)
        }
      } else {
        MatchedControlPlanScore2(-1, null)
      }
    }).filter(matchedControlPlanScore2 => {
      matchedControlPlanScore2.score >= 0 && null != matchedControlPlanScore2.matchControlPlanConfig
    }).toList
    matchedControlPlanScore2List
  }

  /**
   * 判断String 集合是否为空
   * @param list
   * @return
   */
  def judgeListIsEmpty(list: List[String]): Boolean = {
    if (null == list || list.isEmpty) {
      true
    } else {
      false
    }
  }

  /**
   * 组装 ControlPlanConfig
 *
   * @param toolName
   * @param chamberName
   * @param recipeName
   * @param productName
   * @param stageName
   * @param controlPlanConfig2
   * @return
   */
  def generateControlPlanConfig(toolName: String,
                                chamberName: String,
                                recipeName: String,
                                productName: String,
                                stageName: String,
                                controlPlanConfig2: ControlPlanConfig2): ControlPlanConfig = {
    try{
      val toolChamberInfo: ToolChamberInfo = controlPlanConfig2.toolInfoMap.getOrElse(toolName, ToolChamberInfo(0,null))
      val toolId: Long = toolChamberInfo.toolId
      val chamberId: Long = toolChamberInfo.chamberInfoMap.getOrElse(chamberName,0)
      val recipeId: Long = controlPlanConfig2.recipeInfoMap.getOrElse(recipeName, 0)

      ControlPlanConfig(
        locationId = controlPlanConfig2.locationId,
        locationName = controlPlanConfig2.locationName,
        moduleId = controlPlanConfig2.moduleId,
        moduleName = controlPlanConfig2.moduleName,
        toolName = toolName,
        toolId = toolId,
        chamberName = chamberName,
        chamberId = chamberId,
        recipeName = recipeName,
        recipeId = recipeId,
        productName = productName,
        stageName = stageName,
        controlPlanId = controlPlanConfig2.controlPlanId,
        toolGroupId = controlPlanConfig2.toolGroupId,
        toolGroupName = controlPlanConfig2.toolGroupName,
        chamberGroupId = controlPlanConfig2.chamberGroupId,
        chamberGroupName = controlPlanConfig2.chamberGroupName,
        recipeGroupId = controlPlanConfig2.recipeGroupId,
        recipeGroupName = controlPlanConfig2.recipeGroupName,
        limitStatus = controlPlanConfig2.limitStatus,
        area = controlPlanConfig2.area,
        section = controlPlanConfig2.section,
        mesChamberName = null)
    }catch {
      case e:Exception => {
        logger.error(s"组装 SingleControlPlanConfig2 error Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        null
      }
    }

  }


  /**
   * 获取socre 最大值的所有controlPlanConfig
   * @param matchedControlPlanScoreList
   * @return
   */
  def getMatchedControlPlanMaxScoreList(matchedControlPlanScoreList: List[MatchedControlPlanScore]) = {
    if (matchedControlPlanScoreList.nonEmpty) {
      //   获取 score 最大值
      val maxScore = matchedControlPlanScoreList.maxBy(matchedControlPlan => {
        matchedControlPlan.score
      }).score

      //   socre == maxScore 的都命中该controlPlanId
      val matchedControlPlanMaxScoreList: List[MatchedControlPlanScore] =
        matchedControlPlanScoreList.filter((matchedControlPlan: MatchedControlPlanScore) => {
        maxScore == matchedControlPlan.score
      })

      Some(matchedControlPlanMaxScoreList)
    } else {
      None
    }
  }

  /**
   * 获取socre 最大值的所有controlPlanConfig
   * @param matchedControlPlanScore2List
   * @return
   */
  def getMatchedControlPlanMaxScore2List(matchedControlPlanScore2List: List[MatchedControlPlanScore2]) = {
    if (matchedControlPlanScore2List.nonEmpty) {
      //   获取 score 最大值
      val maxScore = matchedControlPlanScore2List.maxBy(matchedControlPlanScore2 => {
        matchedControlPlanScore2.score
      }).score

      //   socre == maxScore 的都命中该controlPlanId
      val matchedControlPlanMaxScore2List: List[MatchedControlPlanScore2] =
        matchedControlPlanScore2List.filter(matchedControlPlanScore2 => {
          maxScore == matchedControlPlanScore2.score
        })

      Some(matchedControlPlanMaxScore2List)
    } else {
      None
    }
  }

  /**
   * 根据key 获取 controlPlanId
   * @param key
   * @return
   */
  def matchControlPlan(key: String)  = {
    if(controlPlanConfigMap.contains(key)){
      val controlPlanIdControlPlanConfigList = controlPlanConfigMap(key)
      if(controlPlanIdControlPlanConfigList.nonEmpty){
        Some(controlPlanIdControlPlanConfigList.values.toList)
      }else{
        None
      }
    }else{
      None
    }
  }


  /**
   * 重新匹配 controlPlan  和  window
   * 用于重启后后面的RawData 可以正常匹配到 controlPlan 和 window
   */
  def retryMatchControlPlanAndWindow(runEventData: RunEventData, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]) = {

    val traceId = runEventData.traceId

    //   匹配controlPlan
//    val matchedControlPlanMaxScoreListOption: Option[List[MatchedControlPlanScore]] = runEventDataMatchControlPlan(runEventStartData)

    val matchedControlPlanMaxScore2ListOption: Option[List[MatchedControlPlanScore2]] = runEventDataMatchControlPlanConfig2(runEventData)

    if(matchedControlPlanMaxScore2ListOption.nonEmpty && matchedControlPlanMaxScore2ListOption.get.nonEmpty){
      //   匹配window
//      eventStartMatchWindow(traceId,matchedControlPlanMaxScoreListOption)
      eventStartMatchWindow2(traceId,matchedControlPlanMaxScore2ListOption)
    }

  }

  /**
   *
   * @param runEventStartData
   * @param readOnlyContext
   * @param collector
   */
  def collectEventStart(runEventStartData: RunEventData, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]) = {
    val traceId = runEventStartData.traceId
    val matchedControlPlanAndWindowsList = runMatchedWindowMap.get(traceId).get
    matchedControlPlanAndWindowsList.foreach(matchedControlPlanAndWindows => {
      val matchedControlPlanConfig: ControlPlanConfig = matchedControlPlanAndWindows._1
      val matchedWindowConfigList = matchedControlPlanAndWindows._2
      matchedWindowConfigList.foreach(matchedWindowPartitionConfig => {
        //   以window为单位 输出eventStart 数据

        val eventDataMatchedWindow = generateEventDataMatchedWindow(matchedWindowPartitionConfig, runEventStartData,matchedControlPlanConfig)
//        logger.warn(s"collectEventStart eventDataMatchedWindow == ${eventDataMatchedWindow.toJson}")
        collector.collect(beanToJsonNode[EventDataMatchedWindow](eventDataMatchedWindow))
      })
    })
  }


  /**
   *
   * @param readOnlyContext
   * @param collector
   */
  def collectRawData(rawData:MainFabRawData , readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]) = {
    val traceId = rawData.traceId
    val matchedControlPlanAndWindowsList = runMatchedWindowMap.get(traceId).get
    matchedControlPlanAndWindowsList.foreach(matchedControlPlanAndWindows => {
      val matchedWindowConfigList = matchedControlPlanAndWindows._2
      matchedWindowConfigList.foreach(matchedWindowConfig => {
        val rawDataMatchedWindow: RawDataMatchedWindow = generateRawDataMatchedWindow(matchedWindowConfig, rawData)
//        logger.warn(s"collectRawData rawDataMatchedWindow == ${rawDataMatchedWindow.toJson}")
        collector.collect(beanToJsonNode[RawDataMatchedWindow](rawDataMatchedWindow))
      })
    })
  }

  /**
   *
   * @param runEventEndData
   * @param readOnlyContext
   * @param collector
   */
  def collectEventEnd(runEventEndData: RunEventData, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]) = {

    val traceId = runEventEndData.traceId
    try{
      val matchedControlPlanAndWindowsList = runMatchedWindowMap.get(traceId).get
      matchedControlPlanAndWindowsList.foreach(matchedControlPlanAndWindows => {
        val matchedWindowConfigList = matchedControlPlanAndWindows._2
        matchedWindowConfigList.foreach(matchedWindowConfig => {
          val eventDataMatchedWindow = generateEventDataMatchedWindow(matchedWindowConfig,runEventEndData)
          //        logger.warn(s"collectEventEnd eventDataMatchedWindow == ${eventDataMatchedWindow.toJson}")
          collector.collect(beanToJsonNode[EventDataMatchedWindow](eventDataMatchedWindow))
        })
      })
    }catch{
      case e:Exception => {
        logger.error(s"collectEventEnd error ! \n " +
          s"runEventEndData == ${runEventEndData} ")
      }
    }finally {
      runMatchedWindowMap.remove(traceId)
      cleanAllState()
    }
  }

  /**
   * 没有匹配到 controlPlan 的run 数据都需要侧道输出到指定kafka
   *
   * @param inputValue
   * @param readOnlyContext
   */
  def giveUpData(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val dataType = inputValue.get(MainFabConstants.dataType).asText("rawData")
    if(MainFabConstants.eventStart == dataType || MainFabConstants.eventEnd == dataType){
      // 没有匹配上的Run侧道输出到指定kafka(只输出eventStart 和 eventEnd)
      readOnlyContext.output(matchControlPlanAndWindowGiveUpRunStreamOutput,inputValue)
    }
  }

  /**
   * 清理所有的状态信息
   * 这里只是正常流程的时候清理所有状态信息，异常情况下还是需要依赖 state 的 TTL;
   */
  def cleanAllState() = {
    eventStartState.clear()
    eventEndState.clear()
    giveUpEventStartState.clear()
    earlyRawDataMapState.clear()

    eventStartRecipeErrorState.clear()
    recipeErrorRawDataMapState.clear()
  }

  /**
   * 1- 解析controlPlan 配置信息
   * 2- 解析window配置信息
   *
   * @param inputData
   * @param context
   * @param collector
   */
  override def processBroadcastElement(inputDimValue: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {

    try{
      val dataType = inputDimValue.get(MainFabConstants.dataType).asText()

      if(MainFabConstants.controlPlanConfig2 == dataType ){

        try {
          //   解析 ControlPlanConfig2 信息
//          logger.error(s"inputDimValue == ${inputDimValue}")
          val configData = toBean[ConfigData[ControlPlanConfig2]](inputDimValue)

//          logger.error(s"configData == ${configData.toJson}")
          parseControlPlanConfig2(configData)
        } catch {
          case e: Exception => logger.error(s"parseControlPlanConfig2 error; " +
            s"inputDimValue == ${inputDimValue}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      }else if (MainFabConstants.controlPlanConfig == dataType ) {
        try {

          //   解析 ControlPlanConfig 信息
          val configData = toBean[ConfigData[List[ControlPlanConfig]]](inputDimValue)
          parseControlPlanConfig(configData)
        } catch {
          case e: Exception => logger.error(s"parseControlPlanConfig error; " +
            s"inputDimValue == ${inputDimValue}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      }else if(MainFabConstants.window2Config == dataType){
        try {

          //   解析 Window2Config 信息
          val configData = toBean[ConfigData[Window2Config]](inputDimValue)
          parseWindowConfig(configData)

        } catch {
          case e: Exception => logger.error(s"parseWindowConfig error; " +
            s"inputDimValue == ${inputDimValue}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
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
      }else if (dataType == "debugMatchControlPlan") {
        // 用于window切窗口debug调试
        try {
          val debugMatchControlPlan = toBean[DebugMatchControlPlan](inputDimValue)
          parseDebugMatchControlPlan(debugMatchControlPlan)
        } catch {
          case e: Exception => {
            logger.warn(s"debugMatchControlPlan json error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }
      }else if (dataType == "printControlPlanConfigInfo") {
        // 用于window切窗口debug调试
        try {
          val debugMatchControlPlan = toBean[DebugMatchControlPlan](inputDimValue)
          printControlPlanConfig2Info(debugMatchControlPlan)
        } catch {
          case e: Exception => {
            logger.warn(s"debugMatchControlPlan json error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
          }
        }
      }
    }catch {
      case e:Exception => {
        logger.warn(s"processBroadcastElement error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
      }
    }

  }

  /**
   * 解析 DebugMatchControlPlan
   * 用于debug 查看指定的 tool chamber recipe 匹配上的cp 和 win 信息;
   * @param debugMatchControlPlan
   */
  def parseDebugMatchControlPlan(debugMatchControlPlan : DebugMatchControlPlan) = {
    val status = debugMatchControlPlan.status
    val toolName = debugMatchControlPlan.toolName
    val chamberName = debugMatchControlPlan.chamberName
    val recipeName = debugMatchControlPlan.recipeName

    if(StringUtils.isNotBlank(toolName) && StringUtils.isNotBlank(chamberName) && StringUtils.isNotBlank(recipeName)){
      val tool_chamber_recipe = toolName + "|" + chamberName + "|" + recipeName
      if(status && !debugMatchControlPlanList.contains(tool_chamber_recipe)){
        debugMatchControlPlanList += tool_chamber_recipe
      }else{
        if(debugMatchControlPlanList.contains(tool_chamber_recipe)){
          debugMatchControlPlanList -= tool_chamber_recipe
        }
      }
    }

  }

  /**
   * 打印controlPlanConfig2 策略信息
   * @param debugMatchControlPlan
   * @return
   */
  def printControlPlanConfig2Info(debugMatchControlPlan : DebugMatchControlPlan) = {
    val toolName = debugMatchControlPlan.toolName
    val chamberName = debugMatchControlPlan.chamberName
    val recipeName = debugMatchControlPlan.recipeName

    if(StringUtils.isNotBlank(toolName) && StringUtils.isNotBlank(chamberName) && StringUtils.isNotBlank(recipeName)){
      val tool_chamber_recipe = toolName + "|" + chamberName + "|" + recipeName
      // print
      if(matchControlPlanConfig2Map.contains(tool_chamber_recipe)){
        val controlPlanIdSet: Set[Long] = matchControlPlanConfig2Map.get(tool_chamber_recipe).get
        logger.warn(s"你要查询的策略信息如下:${tool_chamber_recipe} --> ${controlPlanIdSet}")
        var count = 1
        controlPlanIdSet.foreach(controlPlanId => {
          if(controlPlanConfig2Map.contains(controlPlanId)){
            val controlPlanConfig2 = controlPlanConfig2Map.get(controlPlanId).get
            logger.warn(s"第${count}个controlPlanConfig2(${controlPlanId}): ${controlPlanConfig2.toJson}")
            logger.warn(s"第${count}个controlPlanConfig2(${controlPlanId}) 所有window信息:")
            printWindowConfigMap(controlPlanId,null)
            count = count + 1
          }
        })
      }else{
        logger.warn(s"printControlPlanConfig2Info error ! matchControlPlanConfig2Map 中没有tool + chamber + recipe 信息" +
          s"debugMatchControlPlan == ${debugMatchControlPlan.toJson}")
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
        //   打印 controlPlanConfigMap 信息
        val controlPlanId = debugWindowJobInfo.controlPlanId
        printControlPlanConfigMap(controlPlanId)

        //   打印 windowConfigMap 信息
        printWindowConfigMap(controlPlanId,debugWindowJobInfo.windowIdList)
      })
    }
  }

  /**
   * 打印windowConfig 信息
   * @param controlPlanId
   * @param windowIdList
   */
  def printWindowConfigMap(controlPlanId:Long,windowIdList:List[Long]) = {
    if(matchWindowConfigMap.contains(controlPlanId)){
      val matchWindowConfigList: List[MatchWindowConfig] = matchWindowConfigMap.get(controlPlanId).get
      matchWindowConfigList.foreach((matchWindowConfig: MatchWindowConfig) => {
        val cacheWindowId = matchWindowConfig.windowId
        if(null != windowIdList && windowIdList.nonEmpty){
          if(windowIdList.contains(cacheWindowId)){
            // 如果指定了windowId 就只打印指定的windowId信息
            logger.warn(s"controlPlanId -> windowId : ${controlPlanId} -> ${cacheWindowId} ; ")
            logger.warn(s"matchWindowConfig == ${matchWindowConfig.toJson}")
          }
        }else{
          // 如果没有指定windowId 就打印该controlPlan下所有的windowId信息
          logger.warn(s"controlPlanId -> windowId : ${controlPlanId} -> ${cacheWindowId} ; ")
          logger.warn(s"matchWindowConfig == ${matchWindowConfig.toJson}")
        }
      })
    }
  }

  /**
   * 打印指定的controlPlan 配置信息
   * @param controlPlanId
   */
  def printControlPlanConfigMap(controlPlanId:Long) = {
    controlPlanConfigMap.foreach(controlPlanConfig_key_value => {
      val contextKey = controlPlanConfig_key_value._1
      val controlPlanIdControlPlanConfigMap: TrieMap[Long, ControlPlanConfig] = controlPlanConfig_key_value._2
      controlPlanIdControlPlanConfigMap.foreach(controlPlanId_ControlPlanConfig => {
        val cacheControlPlanId = controlPlanId_ControlPlanConfig._1

        if(controlPlanId == cacheControlPlanId){
          logger.warn(s"contextKey -> cacheControlPlanId : ${contextKey} -> ${cacheControlPlanId}")
          val controlPlanConfig = controlPlanId_ControlPlanConfig._2
          logger.warn(s"controlPlanConfig == ${controlPlanConfig.toJson}")
        }
      })

    })

  }


  /**
   *
   *  optionCode:
   *    0:修改window 配置;
   *      默认都为0，全删全建;
   *    1:删除controlPlan;
   *      当删除controlPlan时，需要讲该controlPlanId下的所有window配置都删除
   * @param configData
   * @return
   */
  def parseWindowConfig(configData: ConfigData[Window2Config],isOnLine:Boolean = false) = {

    val window2Config: Window2Config = configData.datas
    val optionCode = window2Config.optionCode

    optionCode match {
      case 0 =>{
        val controlPlanId = window2Config.controlPlanId
        val controlPlanVersion = window2Config.controlPlanVersion
        val window2DataList: List[Window2Data] = window2Config.window2DataList

        val matchWindowConfigList: List[MatchWindowConfig] = parseMatchWindowConfig(controlPlanId, controlPlanVersion, window2DataList)
        if(isOnLine){
          logger.error(s"matchWindowConfigList == ${matchWindowConfigList.toJson}")
        }
        matchWindowConfigMap.put(controlPlanId,matchWindowConfigList)
      }
      case 1 =>{
        val controlPlanId = window2Config.controlPlanId
        if(matchWindowConfigMap.contains(controlPlanId)){
          matchWindowConfigMap.remove(controlPlanId)
        }
      }
      case _ =>{
        logger.error(s"parseWindowConfig error : optionCode error == ${optionCode} " +
          s"configData == ${configData.toJson} ")
      }
    }
  }

  /**
   *
   * @param controlPlanId
   * @param controlPlanVersion
   * @param window2DataList
   * @return
   */
  def parseMatchWindowConfig(controlPlanId: Long, controlPlanVersion: Long, window2DataList: List[Window2Data], isOnLine:Boolean = false) = {

    val window2DataMap = window2DataList.map(window2Data => {
      window2Data.controlWindowId -> window2Data
    }).toMap

    val matchWindowConfigList = window2DataList.map(window2Data => {
      val sensorAliasNameList = window2Data.sensorAliasNameList

      val allSensorAliasNameList: List[String]  = getSingleWindowAllSensorAliasName(sensorAliasNameList.toList,window2Data,window2DataMap)

      val matchWindowCofnig = MatchWindowConfig(window2Data.controlWindowId,
          controlPlanId,
          controlPlanVersion,
          window2Data.calculationTrigger,
          allSensorAliasNameList.distinct)
      matchWindowCofnig
    }).filter(matchWindowConfig => {

      // 根据windowType只保留windowEnd 或者processEnd
      windowType.equalsIgnoreCase(matchWindowConfig.calculationTrigger)
    })

    matchWindowConfigList
  }


  /**
   * 获取切该窗口时需要哪些sensor ;
   * @param window2Data
   * @param window2DataMap
   * @return
   */
  def getSplitWindowAllSensorAliasName(window2Data: Window2Data,
                                       window2DataMap: Map[Long, Window2Data]): List[String] = {

    val splitWindowBy: List[String] = window2Data.splitWindowBy.split("####").toList
    val extraSensorAliasNameList: List[String] = window2Data.extraSensorAliasNameList
    val splitWindowAllSensorAliasNameList = splitWindowBy ++ extraSensorAliasNameList

    if(!window2Data.isTopWindows){
      val parentWindowId = window2Data.parentWindowId
      if(window2DataMap.contains(parentWindowId)){
        splitWindowAllSensorAliasNameList ++ getSplitWindowAllSensorAliasName(window2DataMap.get(parentWindowId).get, window2DataMap)
      }else{
        logger.error(s"获取切该窗口时需要哪些sensor:（子窗口）controlWindowId:${window2Data.controlWindowId} 的 （父窗口）parentWindowId:${parentWindowId} 配置信息丢失!")
        splitWindowAllSensorAliasNameList
      }
    }else{
      splitWindowAllSensorAliasNameList
    }

  }

  /**
   * 获取该window下使用的所有sensorAliasName
   * 注意: 如果是子window,还需要把父window所使用的所有sensor累加起来;
   * @param window2Data
   * @param window2DataMap
   * @return
   */
  def getSingleWindowAllSensorAliasName(partitionSensorList:List[String],
                                        window2Data: Window2Data,
                                        window2DataMap: Map[Long, Window2Data]): List[String] = {

    // 这里需要将 父window切窗口的sensor都获取到
    val splitWindowByAllSensor = getSplitWindowAllSensorAliasName(window2Data, window2DataMap)
    val allSensorAliasNameList = splitWindowByAllSensor ++ partitionSensorList

    allSensorAliasNameList
  }

  /**
   * 解析controlPlanConfig
   *
   * @param configData
   */
  def parseControlPlanConfig(configData: ConfigData[List[ControlPlanConfig]]): Unit = {
    val status = configData.status
    val controlPlanConfigList = configData.datas
    if(status){
      // true : 新增controlPlanId
      controlPlanConfigList.foreach(controlPlanConfig => {
        val controlPlanMapKey = getControlPlanMapKey(controlPlanConfig)
        val controlPlanId = controlPlanConfig.controlPlanId
        val controlPlanIdControlPlanConfigMap = controlPlanConfigMap.getOrElse(controlPlanMapKey, new TrieMap[Long,ControlPlanConfig])
        controlPlanIdControlPlanConfigMap.put(controlPlanId,controlPlanConfig)
        controlPlanConfigMap.put(controlPlanMapKey,controlPlanIdControlPlanConfigMap)
      })
    }else{
      // false : 删除 只发送删除的 tool + chamber + recipe + product + stage
      controlPlanConfigList.foreach(controlPlanConfig => {
        val controlPlanMapKey = getControlPlanMapKey(controlPlanConfig)
        val controlPlanId = controlPlanConfig.controlPlanId
        val controlPlanIdControlPlanConfigMap: TrieMap[Long, ControlPlanConfig] = controlPlanConfigMap.getOrElse(controlPlanMapKey, new TrieMap[Long,ControlPlanConfig])
        if(controlPlanIdControlPlanConfigMap.nonEmpty && controlPlanIdControlPlanConfigMap.contains(controlPlanId)){
          controlPlanIdControlPlanConfigMap.remove(controlPlanId)
        }

        if(controlPlanIdControlPlanConfigMap.nonEmpty){
          // 如果还有controlPlanId,需要保留该key-value;
          controlPlanConfigMap.put(controlPlanMapKey,controlPlanIdControlPlanConfigMap)
        }else{
          // 如果没有了controlPlanId,清掉key;
          controlPlanConfigMap.remove(controlPlanMapKey)
        }
      })
    }
  }

  /**
   * 解析controlPlanConfig2
   * 根据optionCode 来判断
   * - optionCode
   *    0: 激活controlplan;
   *    1: 删除controlplan;
   *    2: toolConfig 中删除tool 或者chamber 或者 recipe;
   * @param configData
   */
  def parseControlPlanConfig2(configData: ConfigData[ControlPlanConfig2]): Unit = {
    val controlPlanConfig2: ControlPlanConfig2 = configData.datas
    val optionCode = controlPlanConfig2.optionCode
    optionCode match {
      case 0 => {   //0: 激活controlplan;

        //step1: 根据版本号判断是否能更新策略
        val isUpdate: Boolean = judgeIsUpdateControlPlanConfig(controlPlanConfig2)

        if(isUpdate){
          // step2: 更新 matchControlPlanConfig2Map
          updateMatchControlPlanConfig2Map(controlPlanConfig2)

          // step3: 更新 controlPlanConfig2Map
          updateControlPlanConfig2Map(controlPlanConfig2)
        }
      }
      case 1 => {   //1: 删除controlplan;

        // step1 删除 matchControlPlanConfig2Map
        deleteMatchControlPlanConfig2Map(controlPlanConfig2)

        // step2: 删除 controlPlanConfig2Map
        deleteControlPlanConfig2Map(controlPlanConfig2)
      }
      case 2 => {   //2: toolConfig 中删除tool 或者chamber 或者 recipe;
        deleteMatchControlPlanConfig2MapAll(controlPlanConfig2)
      }
      case _ => {
        logger.error(s"策略信息中 optionCode 有误! controlPlanConfig2 == ${controlPlanConfig2.toJson}")
      }
    }

  }

  /**
   * 根据版本号判断是否能更新策略
   * @param controlPlanConfig2
   * @return
   */
  def judgeIsUpdateControlPlanConfig(controlPlanConfig2: ControlPlanConfig2) = {
    // 根据版本号判断是否能更新策略
    val controlPlanId = controlPlanConfig2.controlPlanId
    val controlPlanVersion = controlPlanConfig2.controlPlanVersion
    val currentControlPlanConfig2: ControlPlanConfig2 = controlPlanConfig2Map.getOrElse(controlPlanId, null)
    var currentControlPlanVersion: Long = 0L
    val isUpdate = if (null == currentControlPlanConfig2) {
      true
    } else {
      currentControlPlanVersion = currentControlPlanConfig2.controlPlanVersion
      controlPlanVersion >= currentControlPlanVersion
    }
    if(!isUpdate){
      logger.error(s"更新 contorlPlanConfig 失败! newControlPlanVersion == ${controlPlanVersion} ; currentControlPlanVersion == ${currentControlPlanVersion}")
    }
    isUpdate
  }

  /**
   * 更新 controlPlanConfig2Map
   * @param controlPlanConfig2
   * @return
   */
  def updateControlPlanConfig2Map(controlPlanConfig2: ControlPlanConfig2) = {
    val controlPlanId = controlPlanConfig2.controlPlanId
    controlPlanConfig2Map.put(controlPlanId,controlPlanConfig2)

//    logger.error(s"controlPlanConfig2Map == ${controlPlanConfig2Map.toJson}")
  }

  /**
   * 删除 controlPlanConfig2Map
   * @param controlPlanConfig2
   * @return
   */
  def deleteControlPlanConfig2Map(controlPlanConfig2: ControlPlanConfig2) = {
    val controlPlanId = controlPlanConfig2.controlPlanId
    if(controlPlanConfig2Map.contains(controlPlanId)){
      controlPlanConfig2Map.remove(controlPlanId)
    }

//    logger.error(s"controlPlanConfig2Map == ${controlPlanConfig2Map.toJson}")
  }

  /**
   *  根据 toolName|chamberName|recipeName 更新 matchControlPlanConfig2Map
   * @param controlPlanConfig2
   */
  def updateMatchControlPlanConfig2Map(controlPlanConfig2: ControlPlanConfig2) = {

    val toolInfoMap = controlPlanConfig2.toolInfoMap
    val recipeInfoMap = controlPlanConfig2.recipeInfoMap
    val controlPlanId = controlPlanConfig2.controlPlanId

    toolInfoMap.foreach(toolInfo => {                       // tool 级别
      val toolName: String = toolInfo._1
      val toolChamberInfo = toolInfo._2
      val chamberInfoMap = toolChamberInfo.chamberInfoMap
      chamberInfoMap.foreach(chamberInfo => {               // chamber 级别
        val chamberName = chamberInfo._1
        recipeInfoMap.foreach(recipeInfo => {               // recipe 级别
          val recipeName = recipeInfo._1
          val matchControlPlanConfig2Key = s"${toolName}|${chamberName}|${recipeName}"
          var controlPlanIdSet: Set[Long] = matchControlPlanConfig2Map.getOrElse(matchControlPlanConfig2Key, Set[Long]())
          controlPlanIdSet = controlPlanIdSet + controlPlanId
          matchControlPlanConfig2Map.put(matchControlPlanConfig2Key,controlPlanIdSet)
        })
      })
    })

//    logger.error(s"matchControlPlanConfig2Map == ${matchControlPlanConfig2Map.toJson}")
  }


  /**
   *  根据 toolName|chamberName|recipeName 删除 matchControlPlanConfig2Map
   * @param controlPlanConfig2
   */
  def deleteMatchControlPlanConfig2Map(controlPlanConfig2: ControlPlanConfig2) = {

    val toolInfoMap = controlPlanConfig2.toolInfoMap
    val recipeInfoMap = controlPlanConfig2.recipeInfoMap
    val controlPlanId = controlPlanConfig2.controlPlanId

    toolInfoMap.foreach(toolInfo => {                       // tool 级别
      val toolName: String = toolInfo._1
      val toolChamberInfo = toolInfo._2
      val chamberInfoMap = toolChamberInfo.chamberInfoMap
      chamberInfoMap.foreach(chamberInfo => {               // chamber 级别
        val chamberName = chamberInfo._1
        recipeInfoMap.foreach(recipeInfo => {               // recipe 级别
          val recipeName = recipeInfo._1
          val matchControlPlanConfig2Key = s"${toolName}|${chamberName}|${recipeName}"
          var controlPlanIdSet: Set[Long] = matchControlPlanConfig2Map.getOrElse(matchControlPlanConfig2Key, Set[Long]())
          if(controlPlanIdSet.contains(controlPlanId)){
            controlPlanIdSet = controlPlanIdSet - controlPlanId
          }

          // 如果set 中还有controlPlanId 保留 ; 如果set 中没有了controlPlanId 该key就需要删除;
          if(controlPlanIdSet.nonEmpty || controlPlanIdSet.size > 0){
            matchControlPlanConfig2Map.put(matchControlPlanConfig2Key,controlPlanIdSet)
          }else{
            matchControlPlanConfig2Map.remove(matchControlPlanConfig2Key)
          }

        })
      })
    })

//    logger.error(s"matchControlPlanConfig2Map == ${matchControlPlanConfig2Map.toJson}")
  }

  /**
   *  根据 toolName|chamberName|recipeName 删除 matchControlPlanConfig2Map
   * @param controlPlanConfig2
   */
  def deleteMatchControlPlanConfig2MapAll(controlPlanConfig2: ControlPlanConfig2) = {

    val toolInfoMap = controlPlanConfig2.toolInfoMap
    val recipeInfoMap = controlPlanConfig2.recipeInfoMap

    toolInfoMap.foreach(toolInfo => {                       // tool 级别
      val toolName: String = toolInfo._1
      val toolChamberInfo = toolInfo._2
      val chamberInfoMap = toolChamberInfo.chamberInfoMap
      chamberInfoMap.foreach(chamberInfo => {               // chamber 级别
        val chamberName = chamberInfo._1
        recipeInfoMap.foreach(recipeInfo => {               // recipe 级别
          val recipeName = recipeInfo._1
          val matchControlPlanConfig2Key = s"${toolName}|${chamberName}|${recipeName}"
          if(matchControlPlanConfig2Map.contains(matchControlPlanConfig2Key)){
            matchControlPlanConfig2Map.remove(matchControlPlanConfig2Key)
          }
        })
      })
    })

//    logger.error(s"matchControlPlanConfig2Map == ${matchControlPlanConfig2Map.toJson}")
  }


  /**
   * 组装controlPlanMap 的key
   * 必选项: toolName , chamberName , recipeName
   * 非空即选: productName , stageName
 *
   * @param controlPlanConfig
   * @return
   */
  def getControlPlanMapKey(controlPlanConfig: ControlPlanConfig): String = {
    val toolName = if(StringUtils.isNotBlank(controlPlanConfig.toolName)){controlPlanConfig.toolName.trim}else{""}
    val chamberName = if(StringUtils.isNotBlank(controlPlanConfig.chamberName)){controlPlanConfig.chamberName.trim}else{""}
    val recipeName = if(StringUtils.isNotBlank(controlPlanConfig.recipeName)){controlPlanConfig.recipeName.trim}else{""}
    val productName = if(StringUtils.isNotBlank(controlPlanConfig.productName)){controlPlanConfig.productName.trim}else{""}
    val stageName = if(StringUtils.isNotBlank(controlPlanConfig.stageName)){controlPlanConfig.stageName.trim}else{""}
    val controlPlanMapKey = toolName + "|" + chamberName + "|" + recipeName + "|" + productName + "|" + stageName

    controlPlanMapKey
  }


  /**
   * 生产 EventDataMatchedWindow
   * eventStart eventEnd 新增以下字段
   * 匹配的   matchedControlPlanId:Long,
   * 匹配的   controlPlanVersion:Long,
   * 匹配的   matchedWindowId:Long
   * @param matchWindowConfig
   * @param eventData
   * @return
   */
  def generateEventDataMatchedWindow(matchWindowConfig: MatchWindowConfig, eventData: RunEventData, matchedControlPlanConfig: ControlPlanConfig = null) = {

    EventDataMatchedWindow(dataType = eventData.dataType,
      locationName = eventData.locationName,
      moduleName = eventData.moduleName,
      toolName = eventData.toolName,
      chamberName = eventData.chamberName,
      recipeName = eventData.recipeName,
      runStartTime = eventData.runStartTime,
      runEndTime = eventData.runEndTime,
      runId = eventData.runId,
      traceId = eventData.traceId,
      DCType = eventData.DCType,
      dataMissingRatio = eventData.dataMissingRatio,
      timeRange = eventData.timeRange,
      completed = eventData.completed,
      materialName = eventData.materialName,
      pmStatus = eventData.pmStatus,
      pmTimestamp = eventData.pmTimestamp,
      dataVersion = eventData.dataVersion,
      lotMESInfo = eventData.lotMESInfo,
      errorCode = eventData.errorCode,
      matchedControlPlanId = matchWindowConfig.controlPlanId,
      controlPlanVersion = matchWindowConfig.controlPlanVersion,
      matchedWindowId = matchWindowConfig.windowId,
      windowPartitionId = "partition_0",
      matchedControlPlanConfig = matchedControlPlanConfig)
  }


  /**
   * 生产 RawDataMatchedWindow
   * rawData 新增以下字段
   * 匹配的   matchedControlPlanId:Long,
   * 匹配的   controlPlanVersion:Long,
   * 匹配的   matchedWindowId:Long
   * @param matchedWindowConfig
   * @param rawData
   * @return
   */
  def generateRawDataMatchedWindow(matchedWindowConfig: MatchWindowConfig, rawData: MainFabRawData) = {
    val matchedWindowId = matchedWindowConfig.windowId
    val matchedControlPlanId = matchedWindowConfig.controlPlanId
    val controlPlanVersion = matchedWindowConfig.controlPlanVersion
    val windowAllSendorAliasNameList = matchedWindowConfig.windowAllSendorAliasNameList

    //   只保留该window中使用到的sensor 数据
    val windowAllSendorInfoList = rawData.data.filter(sensorInfo => {
      windowAllSendorAliasNameList.contains(sensorInfo.sensorAlias)
    }).map((sensorInfo: sensorData) => {
      (sensorInfo.sensorAlias,sensorInfo.sensorValue,sensorInfo.unit) // 1: sensorAlias, 2: sensorValue, 3: unit
    })

    RawDataMatchedWindow(dataType = rawData.dataType,
      dataVersion = rawData.dataVersion,
      toolName = rawData.toolName,
      chamberName = rawData.chamberName,
      timestamp = rawData.timestamp,
      traceId = rawData.traceId,
      stepId = rawData.stepId,
      stepName = rawData.stepName,
      data = windowAllSendorInfoList,
      matchedControlPlanId = matchedControlPlanId,
      controlPlanVersion = controlPlanVersion,
      matchedWindowId = matchedWindowId,
      windowPartitionId = "partition_0")
  }


  /**
   * 侧道输出调试信息
   * @param toolName
   * @param chamberName
   * @param recipeName
   * @param readOnlyContext
   */
  def debugMatchControlPlanAndWindow(traceId:String,
                                     toolName: String,
                                     chamberName: String,
                                     recipeName: String,
                                     readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {
    val tool_chamber_recipe = toolName + "|" + chamberName + "|" + recipeName
    if(debugMatchControlPlanList.contains(tool_chamber_recipe)){
      val matchedControlPlanAndWindowsList = runMatchedWindowMap.get(traceId).get
      val outputInfo = s"traceId == ${traceId}; toolName == ${toolName} ; chamberName == ${chamberName} ; recipeName == ${recipeName} \n " +
        s"matchedControlPlanAndWindowsList == ${matchedControlPlanAndWindowsList.toString} "

      // 侧道输出到指定kafka
      readOnlyContext.output(mainFabDebugInfoOutput,outputInfo)
    }
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
