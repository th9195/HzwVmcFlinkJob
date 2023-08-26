package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{ConfigData, ContextConfigData, ContextProductStageInfo, ErrorCode, MainFabRawData, MainFabRawDataTuple, RunEventData, RunEventDataMatchWindow, WindowConfigData}
import com.hzw.fdc.util.MainFabConstants.IS_DEBUG
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}





/**
 * MainFabWindowConfigStaySensorMatchControlPlanBroadcastProcessFunction
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/8 10:38
 * @since 1.0.0
 * @update 2022/11/8 10:38
 * */
class MainFabWindowConfigStaySensorMatchControlPlanBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,  JsonNode] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowConfigStaySensorMatchControlPlanBroadcastProcessFunction])

  lazy val GIVEUP_RUN = new OutputTag[JsonNode]("giveUpRun")

  //缓存 context 配置信息
  //tool|chamber|recipe --> contextId  --> ContextInfo
  var allContextMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long,ContextProductStageInfo]]()

  //缓存 windowConfig 配置信息
  //contextId --> List[WindowConfigData]
  var windowConfigMap = new concurrent.TrieMap[Long, ListBuffer[WindowConfigData]]()

  //缓存 eventStart 匹配上的windowId ,用于 : 1- rawData 过滤出sensor ; 2- eventEnd需要添加相同信息
  //在eventStart时 添加, 在eventEnd时 清除;
  //traceId --> (contextId,List[WindowId])
  var runMatchedWindowIdMap = new concurrent.TrieMap[String, (Long,List[Long])]()

  // 缓存 rawData 比 eventStart 早到的数据
  // 在rawData 中 添加 , 在eventStart 中 清除;
  // traceId --> List[JsonNode]  : MainFabRawData
  var rawDataBeforeEventStartMap = new concurrent.TrieMap[String, ListBuffer[JsonNode]]()

  //收集需要丢弃的 traceId ;  eventStart没有匹配到context,window Run,需要告知 rawData
  // 在 eventStart 中 添加 , 在 eventEnd 中 清除;
  var giveUpTraceId = ListBuffer[String]()

  override def open(parameters: Configuration): Unit = {

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    if(!IS_DEBUG){
      val startTime = System.currentTimeMillis()
      // 初始化ContextConfig
      val contextConfigList = InitFlinkFullConfigHbase.ContextConfigList
      contextConfigList.foreach(addContextMapToAllContextMap)

      // 初始化windowConfig
      val runWindowConfigList = InitFlinkFullConfigHbase.RunWindowConfigList
      runWindowConfigList.foreach(addWindowConfigToWindowConfigMap)

      val endTIme = System.currentTimeMillis()
      logger.warn("---SplitSensor Total Time: " + (endTIme - startTime))
    }

  }


  override def processElement(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]): Unit = {
    try {
      val dataType = inputValue.get(MainFabConstants.dataType).asText()

      if(dataType == MainFabConstants.eventStart){

        // todo 处理 EventStart 数据
        processEventStart(inputValue, readOnlyContext, collector)
      } else if(dataType == MainFabConstants.eventEnd){

        // todo 处理 EventEnd 数据
        processEventEnd(inputValue, readOnlyContext, collector)
      }else if(dataType == MainFabConstants.rawData){

        // todo 处理 RawData 数据
        processRawData(inputValue, readOnlyContext, collector)
      }

    }catch {
      case ex: Exception => logger.warn(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> "匹配 context window 失败 : "), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }

  }

  override def processBroadcastElement(inputDimValue: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {

    try {
      val dataType = inputDimValue.get(MainFabConstants.dataType).asText()

      if (dataType == MainFabConstants.context) {
        try {
          val contextConfig = toBean[ConfigData[List[ContextConfigData]]](inputDimValue)
          addContextMapToAllContextMap(contextConfig)

        } catch {
          case e: Exception => logger.warn(s"contextConfig json error data:${inputDimValue.toJson}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      } else if (dataType == MainFabConstants.runwindow) {
        try {
          val windowConfig = toBean[ConfigData[List[WindowConfigData]]](inputDimValue)
          addWindowConfigToWindowConfigMap(windowConfig)
        } catch {
          case e: Exception => logger.warn(s"windowConfig json error  data:${inputDimValue.toJson}  " +
            s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }
      }

    }catch {
      case e: Exception => logger.warn(s"windowJob processBroadcastElement error data:${inputDimValue.toJson}  " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }

  }


  /**
   * 处理 RawData 数据
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processRawData(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])={
    try{
      val rawData = toBean[MainFabRawData](inputValue)
      val traceId = rawData.traceId
      if(giveUpTraceId.contains(traceId)){

        // todo 没有匹配到context 丢弃 或者 侧道输出到指定的kafka
        giveUpRun(inputValue,readOnlyContext)

      }else{

        if(runMatchedWindowIdMap.contains(traceId)){
          // todo 根据配置信息过滤掉 RawData 中使用不到的sensor 信息;


          collector.collect(inputValue)
        }else{
          // todo 说明 RawData 比 EventStart 先到
          if(rawDataBeforeEventStartMap.contains(traceId)){
            val cacheRawDataList = rawDataBeforeEventStartMap(traceId)
            cacheRawDataList += inputValue
            rawDataBeforeEventStartMap.put(traceId,cacheRawDataList)
          }else{
            rawDataBeforeEventStartMap.put(traceId,ListBuffer[JsonNode](inputValue))
          }
        }
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> "匹配 context window 失败 : 处理 rawData 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }

  /**
   * 处理 eventEnd 数据
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   */
  def processEventEnd(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])={
    try{
      val eventEndData = toBean[RunEventData](inputValue)
      val traceId = eventEndData.traceId

      // todo 是否giveUp
      if(giveUpTraceId.contains(traceId)){
        giveUpRun(inputValue,readOnlyContext)
      }

      // todo 获取eventStart 匹配到的 windowId
      if(runMatchedWindowIdMap.contains(traceId)){
        val matchedContextId_windowIdList: (Long, List[Long]) = runMatchedWindowIdMap(traceId)
        val runEventDataMatchWindow = generateRunEventDataMatchWindow(eventEndData, matchedContextId_windowIdList._1, matchedContextId_windowIdList._2)

        // todo eventEnd 数据处理完, 该Run 默认就跑完了
        runMatchedWindowIdMap.remove(traceId)

        // todo 输出 eventEndData
        val outputData = beanToJsonNode[RunEventDataMatchWindow](runEventDataMatchWindow.get)
        collector.collect(outputData)
      }else{
        // 说明 eventStart 没有匹配到window , 或者 eventEnd 比 eventStart先到
        // 目前先抛出异常
        logger.error(s"${traceId},eventEnd before eventStart")
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> "匹配 context window 失败 : 处理 eventEnd 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }

  }

  /**
   * 处理 eventStart 数据
   * @param inputValue
   * @param readOnlyContext
   * @param collector
   * @return
   */
  def processEventStart(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode])={
    try{
      val eventStartData = toBean[RunEventData](inputValue)
      val traceId = eventStartData.traceId
      val toolName = eventStartData.toolName
      val chamberName = eventStartData.chamberName
      val recipeName = eventStartData.recipeName
      val productNameList = new ListBuffer[String]
      val stageNameList = new ListBuffer[String]
      eventStartData.lotMESInfo.foreach(lot => {
        val lotInfo = lot.get
        val productName = lotInfo.product
        val stageName = lotInfo.stage
        productNameList.append(productName.get)
        stageNameList.append(stageName.get)
      })

      val key_tool_chamber_recipe = s"${toolName}|${chamberName}|${recipeName}"

      if(allContextMap.contains(key_tool_chamber_recipe)){
        val contextInfoMap = allContextMap(key_tool_chamber_recipe)
        // todo 匹配当前 Run 属于哪个context (controlPlan)
        val contextProductStageInfoList: List[ContextProductStageInfo] = contextInfoMap.values.toList
        val matchedContextIdOption: Option[Long] = matchContext(contextProductStageInfoList,productNameList,stageNameList)

        if(matchedContextIdOption.nonEmpty){
          val matchedContextId = matchedContextIdOption.get
          if(windowConfigMap.contains(matchedContextId)){

            val windowConfigList = windowConfigMap(matchedContextId)
            val matchedWindowIdList: List[Long] = windowConfigList.map(windowConfig => {
              windowConfig.controlWindowId
            }).toList

            val runEventDataMatchWindow = generateRunEventDataMatchWindow(eventStartData,matchedContextId, matchedWindowIdList)
            if(!runEventDataMatchWindow.nonEmpty){

              logger.warn(s"匹配window失败4444: 没有匹配的 window,可能没有创建Indicator ${traceId}")
              giveUpRun(inputValue,readOnlyContext)

            }else{
              val windowIdList: List[Long] = runEventDataMatchWindow.get.windowIdList
              // todo 缓存该Run 匹配上了哪些window , 用于 rawData
              runMatchedWindowIdMap.put(traceId,(matchedContextId,windowIdList))

              // todo 重新处理比eventStart早到的数据
              if(rawDataBeforeEventStartMap.contains(traceId)){
                val earlyRawDataList = rawDataBeforeEventStartMap(traceId)
                earlyRawDataList.foreach(earlyRawData => {
                  processRawData(earlyRawData , readOnlyContext,collector)
                })
                rawDataBeforeEventStartMap.remove(traceId)
              }

              // todo 输出 eventStartData
              val outputData = beanToJsonNode[RunEventDataMatchWindow](runEventDataMatchWindow.get)
              collector.collect(outputData)
            }

          }else{

            logger.warn(s"匹配window失败33333: 没有匹配的 window,可能没有创建Indicator ${traceId}")
            // todo  没有匹配到 window
            giveUpRun(inputValue,readOnlyContext)
          }
        }else{

          logger.warn(s"匹配context失败22222: 没有匹配的 product 和 stage ${traceId}")
          // todo  没有匹配到 context
          giveUpRun(inputValue,readOnlyContext)
        }
      }else{
        logger.warn(s"匹配context失败11111: 没有匹配的 tool,chamber,recipe ${traceId}")
        // todo  没有匹配到 context
        giveUpRun(inputValue,readOnlyContext)

      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("ff2007d012C", System.currentTimeMillis(),
        Map("msg" -> s"匹配 context window 失败 : 处理 eventStart 数据 异常 inputValue == ${inputValue}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 样例类转换 RunEventData --> RunEventDataMatchWindow
   * 新增两个字段信息
   *  1- 匹配上的contextId ;
   *  2- 匹配上的windowId ;
   * @param runEventData
   * @param matchedContextId
   * @return
   */
  def generateRunEventDataMatchWindow(runEventData: RunEventData, matchedContextId:Long,matchedWindowIdList: List[Long] ): Option[RunEventDataMatchWindow] = {

    if(matchedWindowIdList.nonEmpty){
      Option(RunEventDataMatchWindow(dataType = runEventData.dataType,
        locationName = runEventData.locationName,
        moduleName = runEventData.moduleName,
        toolName = runEventData.toolName,
        chamberName = runEventData.chamberName,
        recipeName = runEventData.recipeName,
        runStartTime = runEventData.runStartTime,
        runEndTime = runEventData.runEndTime,
        runId = runEventData.runId,
        traceId = runEventData.traceId,
        DCType = runEventData.DCType,
        dataMissingRatio = runEventData.dataMissingRatio,
        timeRange = runEventData.timeRange,
        completed = runEventData.completed,
        materialName = runEventData.materialName,
        pmStatus = runEventData.pmStatus,
        pmTimestamp = runEventData.pmTimestamp,
        dataVersion = runEventData.dataVersion,
        lotMESInfo = runEventData.lotMESInfo,
        errorCode = runEventData.errorCode,
        contextId = matchedContextId,
        windowIdList = matchedWindowIdList))
    }else{
      None
    }



  }

  /**
   * 匹配原则:
   * stage积分 (包含 stage 积2分 ，为空积 0 分  , 不包含 -1)
   * 1- 每个 context 都有个score字段， 默认值为 -1;
   * 2- 如果 context 中的stages 为空 score = 0;
   * 3- 如果 context 中的stages 不为空， 并且 与 当前的runData中的stages 有交集 score = 2;
   * 4- 如果 context 中的stages 不为空， 并且 与 当前的runData中的stages 没有交集 score = -1;
   * product 积分 (包含 product 积 1分 ，为空积 0 分  , 不包含 -1)
   * 1- 如果 context 中的products 为空 score = score + 0;
   * 2- 如果 context 中的products 不为空 并且 与 当前的runData中的products 有交集 score = score + 1;
   * 3- 如果 context 中的products 不为空 并且 与 当前的runData中的products 没有有交集 score = -1;
   * 最后取 score最大值
   * @param contextInfoList
   * @param productNameList
   * @param stageNameList
   * @return
   */
  def matchContext(contextProductStageInfoList: List[ContextProductStageInfo], currentProductNameList: ListBuffer[String], currentStageNameList: ListBuffer[String]) = {

    val matchedContextList = contextProductStageInfoList.map(contextProductStageInfo => {
      val configStageNameList = contextProductStageInfo.stageNameList

      if (configStageNameList.isEmpty) {
        contextProductStageInfo.setScore(0)
      } else if (configStageNameList.intersect(currentStageNameList).nonEmpty) {
        contextProductStageInfo.setScore(2)
      } else {
        contextProductStageInfo.setScore(-1)
      }
      contextProductStageInfo

    }).filter(contextProductStageInfo => {
      // 过滤出 得分 >=0 的
      contextProductStageInfo.getScore >= 0
    }).map(contextProductStageInfo => {
      val configProductNameList = contextProductStageInfo.productNameList
      if (configProductNameList.isEmpty) {
        contextProductStageInfo.setScore(contextProductStageInfo.getScore + 0)
      } else if (configProductNameList.intersect(currentProductNameList).isEmpty) {
        contextProductStageInfo.setScore(-1)
      } else {
        contextProductStageInfo.setScore(contextProductStageInfo.getScore + 1)
      }
      contextProductStageInfo
    }).filter(contextProductStageInfo => {

      // 过滤出 得分 >=0 的
      contextProductStageInfo.getScore >= 0
    })

    if(matchedContextList.nonEmpty){
      val matchedContextId = matchedContextList.sortBy(contextProductStageInfo => {
        contextProductStageInfo.score
      }).reverse.head.contextId

      Option(matchedContextId)
    }else{
      None
    }
  }


  /**
   * 没有匹配到context的run 数据都需要丢弃 或者 侧道输出到指定kafka
   * @param inputValue
   * @param readOnlyContext
   */
  def giveUpRun(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext) = {

    val dataType = inputValue.get(MainFabConstants.dataType).asText()
    val traceId = inputValue.get("traceId").asText()

    // todo 如果没有到context,该Run所有的数据(rawData)需要全部丢弃(或者 侧道输出到指定的kafka中)
    if(dataType == MainFabConstants.eventStart){
      // eventStart 时 需要记录该 run 的traceId, 在处理rawData时,也是需要丢弃
      giveUpTraceId += traceId
    }else if(dataType == MainFabConstants.eventEnd){
      // eventEnd 时 需要将记录的 traceId 移除
      giveUpTraceId -= traceId
    }

    // todo 侧道输出到指定kafka
    readOnlyContext.output(GIVEUP_RUN,inputValue)
  }

  /**
   *
   * @param contextId
   * @param configProductName
   * @param configStage
   * @return
   */
  def getContextProductStageInfo(contextId:Long,configProductName:String,configStage:String) = {
    val productNameList: ListBuffer[String] = new ListBuffer[String]()
    productNameList.append(configProductName)

    val stageNameList = new ListBuffer[String]()
    stageNameList.append(configStage)

    ContextProductStageInfo(contextId = contextId,
      productNameList = productNameList,
      stageNameList = stageNameList,
      score = -1)
  }


  /**
   * 添加context config
   */
  def addContextMapToAllContextMap(contextConfigList: ConfigData[List[ContextConfigData]]): Unit = {
    try {
      for (contextConfig: ContextConfigData <- contextConfigList.datas) {
        val key_tool_chamber_recipe = s"${contextConfig.toolName}|${contextConfig.chamberName}|${contextConfig.recipeName}"
        val contextId = contextConfig.contextId
        val configProductName = contextConfig.productName
        val configStage = contextConfig.stage

        if (contextConfigList.status) {
          if(allContextMap.contains(key_tool_chamber_recipe)){
            val contextInfoMap = allContextMap(key_tool_chamber_recipe)

            if(contextInfoMap.contains(contextId)){
              val contextProductStageInfo = contextInfoMap(contextId)
              val productNameList = contextProductStageInfo.productNameList
              val stageNameList = contextProductStageInfo.stageNameList

              if(!productNameList.contains(configProductName)){
                productNameList.append(configProductName)
              }
              if(!stageNameList.contains(configStage)){
                stageNameList.append(configStage)
              }

              contextProductStageInfo.setProductNameList(productNameList)
              contextProductStageInfo.setStageNameList(stageNameList)
              contextInfoMap.put(contextId,contextProductStageInfo)
              allContextMap.put(key_tool_chamber_recipe,contextInfoMap)
            }else{
              // 还没有存储 的 contextId
              val contextProductStageInfo = getContextProductStageInfo(contextId,configProductName, configStage)
              contextInfoMap.put(contextId , contextProductStageInfo)
              allContextMap.put(key_tool_chamber_recipe,contextInfoMap)
            }
          }else{
            // 还没有存储 的 tool|chamber|recipe
            val contextProductStageInfo = getContextProductStageInfo(contextId,configProductName, configStage)

            val contextInfoMap = TrieMap[Long, ContextProductStageInfo]()
            contextInfoMap.put(contextId,contextProductStageInfo)

            allContextMap.put(key_tool_chamber_recipe,contextInfoMap)

          }
        }else{
          // 删除
          if(allContextMap.contains(key_tool_chamber_recipe)){
            allContextMap.remove(key_tool_chamber_recipe)
          }
        }
      }
    }catch {
      case e: Exception =>
        logger.warn(ErrorCode("ff2003b010C", System.currentTimeMillis(), Map(), e.toString).toJson)
    }
  }


  /**
   * 添加window config; 加载所有window
   */
  def addWindowConfigToWindowConfigMap(windowConfigList: ConfigData[List[WindowConfigData]]): Unit = {

    try {
      if(windowConfigList.datas.isEmpty){
        return
      }
      logger.warn(s"addWindowConfigToWindowConfigMap: ${windowConfigList.toJson}")

      val contextId: Long = windowConfigList.datas.head.contextId

      // 全删全建
      windowConfigMap.remove(contextId)

      windowConfigList.datas.foreach((windowConfig: WindowConfigData) => {

        if(windowConfigMap.contains(contextId)){
          val windowConfigList = windowConfigMap(contextId)
          windowConfigList += windowConfig
          windowConfigMap.put(contextId,windowConfigList)

        }else{
          val windowConfigList = ListBuffer[WindowConfigData](windowConfig)
          windowConfigMap.put(contextId,windowConfigList )
        }
      })

    }catch {
      case ex: Exception => logger.warn(ErrorCode("ff2007d001C", System.currentTimeMillis(),
        Map("range" -> "添加window config配置失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }
}
