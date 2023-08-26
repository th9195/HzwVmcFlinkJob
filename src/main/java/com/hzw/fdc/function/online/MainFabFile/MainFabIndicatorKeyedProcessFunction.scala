package com.hzw.fdc.function.online.MainFabFile

import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{AlarmRuleResult, ConfigData, ErrorCode, FdcData, IndicatorFileResult, IndicatorResultFileScala, IndicatorValue, RunData, indicatorTimeOutTimestamp, taskIdTimestamp}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}

import scala.collection.JavaConversions.iterableAsScalaIterable
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer


class MainFabIndicatorKeyedProcessFunction extends KeyedProcessFunction[String,
  JsonNode, IndicatorResultFileScala]  {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabIndicatorKeyedProcessFunction])

  // {runId: {"IndicatorId": IndicatorFileResult}}
  private val indicatorRunCacheMap = concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorFileResult]]()

  /** The state that is maintained by this process function */
  private var runIdState: ValueState[taskIdTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

//    val indicatorStateDescription: ListStateDescriptor[IndicatorFileResult] = new
//        ListStateDescriptor[IndicatorFileResult]("FileListIndicatorState", TypeInformation.of(classOf[IndicatorFileResult]))
//    indicatorRunCacheState = getRuntimeContext.getListState(indicatorStateDescription)


    val eventStartStateDescription = new ValueStateDescriptor[taskIdTimestamp](
      "indicatorOutValueState", TypeInformation.of(classOf[taskIdTimestamp]))
    eventStartStateDescription.enableTimeToLive(ttlConfig)
    runIdState = getRuntimeContext.getState(eventStartStateDescription)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JsonNode, IndicatorResultFileScala]#OnTimerContext,
                       out: Collector[IndicatorResultFileScala]): Unit = {
    try {
      val key = ctx.getCurrentKey
        runIdState.value() match {
        case taskIdTimestamp(cacheKey, lastModified)
          if (System.currentTimeMillis() >= lastModified+90000) =>

//          logger.warn(s"onTimer_step1: ${cacheKey}\t getCurrentKey: ${key}")

          val cacheKeyList = cacheKey.split("\\|")
          val runId = cacheKeyList.head
          val runIdTmp = runId.split("--")
          val runStartTime = runIdTmp.last
          val runEndTime = cacheKeyList.last

          // 按照controlPlnId 维度聚合返回
          if(indicatorRunCacheMap.contains(runId)){
            val indicatorCache = indicatorRunCacheMap(runId)

//            logger.warn(s"indicatorCache: ${indicatorCache.values.toList} runStartTime: ${runStartTime} runEndTime: ${runEndTime}")
            val resList = Parse(indicatorCache.values.toList, runStartTime, runEndTime)

            resList.foreach(elem => {
              out.collect(elem)
            })

            indicatorRunCacheMap.remove(runId)
          }

          close()
        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("onTimer" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }

  /**
   *  数据流
   */
  override def processElement(elem: JsonNode, ctx: KeyedProcessFunction[String, JsonNode, IndicatorResultFileScala]#Context,
                              out: Collector[IndicatorResultFileScala]): Unit = {
    try {
      val dataType = elem.get(MainFabConstants.dataType).asText()

      if (dataType == "AlarmLevelRule") {

        val e = toBean[ConfigData[AlarmRuleResult]](elem)
        val value = e.datas
        val runId = value.runId

        val indicatorFileResult = IndicatorFileResult(
          dataType = e.`dataType`,
          controlPlanVersion = value.controlPlanVersion,
          controlPlanId = value.controlPlnId.toString,
          controlPlanName = value.controlPlanName,
          runId = value.runId,
          toolName = value.toolName,
          toolId = value.toolId.toString,
          chamberName = value.chamberName,
          chamberId = value.chamberId.toString,
          indicatorValue = value.indicatorValue,
          indicatorId = value.indicatorId.toString,
          indicatorName = value.indicatorName,
          createTime = value.alarmCreateTime.toString,
          limit = value.limit,
          locationName = value.locationName,
          moduleName = value.moduleName,
          recipeName = value.recipeName,
          recipeId = value.recipeId.toString,
          toolGroupName = value.toolGroupName,
          chamberGroupName = value.chamberGroupName,
          recipeGroupName = value.recipeGroupName,
          alarmLevel = if (value.alarmLevel >= -3) value.alarmLevel.toString else "0",
          dataMissingRatio = value.dataMissingRatio.toString,
          runStartTime = value.runStartTime,
          runEndTime = value.runEndTime,
          materialName = value.materialName,
          lotMESInfo = value.lotMESInfo
        )

        // 缓存indicator
        if(indicatorRunCacheMap.contains(runId)){
          val indicatorMap = indicatorRunCacheMap(runId)
          indicatorMap.put(value.indicatorId, indicatorFileResult)
          indicatorRunCacheMap.put(runId, indicatorMap)
        }else{
          val indicatorScala = concurrent.TrieMap[Long, IndicatorFileResult](value.indicatorId -> indicatorFileResult)
          indicatorRunCacheMap.put(runId, indicatorScala)
        }


        if (value.runEndTime != 0L) {
          // write the state back
          runIdState.update(taskIdTimestamp(s"$runId|${value.runEndTime}", System.currentTimeMillis()))

          // schedule the next timer 60 seconds from the current event time
          ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis()+90000)
        }
      }else if(dataType == "rundata"){
        val rundata = toBean[FdcData[RunData]](elem)
        if(rundata.datas.runEndTime.nonEmpty && rundata.datas.runEndTime.get != -1L){
          val runId = rundata.datas.runId

          // write the state back
          runIdState.update(taskIdTimestamp(s"$runId|${rundata.datas.runEndTime.get}", System.currentTimeMillis()))

          // schedule the next timer 60 seconds from the current event time
          ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis()+90000)
        }
      }
    }catch {
      case e: Exception => logger.warn(s"IndicatorFileResult math error ${ExceptionInfo.getExceptionInfo(e)}\t $elem")
    }
  }

  /**
   *   解析成IndicatorResultFileScala
   */
  def Parse(indicatorList: List[IndicatorFileResult], runStartTime: String, runEndTime: String): ListBuffer[IndicatorResultFileScala] = {

    val resList: ListBuffer[IndicatorResultFileScala] = ListBuffer()

    val controlPlanIdMap: mutable.HashMap[String, ListBuffer[IndicatorFileResult]] = mutable.HashMap()

    indicatorList.foreach(elem => {
      if(controlPlanIdMap.contains(elem.controlPlanId)){
        val indicatorSet = controlPlanIdMap(elem.controlPlanId)
        indicatorSet += elem
        controlPlanIdMap.put(elem.controlPlanId, indicatorSet)
      }else{
        controlPlanIdMap.put(elem.controlPlanId, ListBuffer(elem))
      }
    })

    for(elem <- controlPlanIdMap){
      try{
        val value = elem._2
        val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
        val timestampStr = fm.format(value.head.createTime.toLong)
        val res = value.map(x => {
          IndicatorValue(timestampStr, x.indicatorName, x.indicatorValue, x.limit, x.indicatorId, x.alarmLevel)
        }).distinct.toList


        val indicatorScala = IndicatorResultFileScala(
          value.head.controlPlanVersion.toInt,
          value.head.controlPlanId,
          value.head.controlPlanName,
          value.head.locationName,
          value.head.moduleName,
          value.head.toolName,
          value.head.toolId,
          value.head.chamberName,
          value.head.chamberId,
          value.head.recipeName,
          value.head.recipeId,
          value.head.toolGroupName,
          value.head.chamberGroupName,
          value.head.recipeGroupName,
          value.head.dataMissingRatio,
          runStartTime.toLong,
          runEndTime.toLong,
          value.head.runId,
          value.head.materialName,
          value.head.lotMESInfo,
          res
        )

        resList += indicatorScala
      }catch {
        case ex: Exception => logger.warn(s"${ex.toString}")
      }
    }

    resList
  }
}
