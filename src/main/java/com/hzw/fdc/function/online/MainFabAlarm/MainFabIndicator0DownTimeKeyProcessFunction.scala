package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{AlarmRuleResult, ErrorCode, FdcData, RunEventData, indicator0DownTime, taskIdTimestamp}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent



class MainFabIndicator0DownTimeKeyProcessFunction extends KeyedProcessFunction[String, JsonNode, JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabIndicator0DownTimeKeyProcessFunction])


  val RunIdMap = new concurrent.TrieMap[String, scala.collection.mutable.Set[String]]()

  private var indicatorResultStateMap: MapState[String, JsonNode] = _


  /** The state that is maintained by this process function */
  private var indicatorTimeOutState: ValueState[indicator0DownTime] = _

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

    val eventStartStateDescription = new ValueStateDescriptor[indicator0DownTime](
      "indicator0DownTimeOutValueState", TypeInformation.of(classOf[indicator0DownTime]))
    eventStartStateDescription.enableTimeToLive(ttlConfig)
    indicatorTimeOutState = getRuntimeContext.getState(eventStartStateDescription)


    val traceIdRecipeStateDescription: MapStateDescriptor[String,JsonNode] = new
        MapStateDescriptor[String,JsonNode]("indicatorStateMapDescription",
          TypeInformation.of(classOf[String]),TypeInformation.of(classOf[JsonNode]))
    eventStartStateDescription.enableTimeToLive(ttlConfig)
    indicatorResultStateMap = getRuntimeContext.getMapState(traceIdRecipeStateDescription)
  }

  /**
   * 超时
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JsonNode, JsonNode]#OnTimerContext,
                       out: Collector[JsonNode]): Unit = {
    try {
      indicatorTimeOutState.value() match {
        case indicator0DownTime(runId, lastModified)
          if (System.currentTimeMillis() >= lastModified + 90000L) =>

          // 把缓存的旧流开始写入下游
          val indicatorIterator = indicatorResultStateMap.values().iterator()
          while (indicatorIterator.hasNext){
            val indicatorJson = indicatorIterator.next()
            out.collect(indicatorJson)
          }

          RunIdMap.remove(runId)
          indicatorTimeOutState.clear()
          indicatorResultStateMap.clear()
          close()
        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("onTimer" -> "结果匹配失败!!", "function" -> "MainFabIndicator0DownTimeKeyProcessFunction"), exception.toString).toString)
    }
  }



  /**
   *  数据流
   */
  override def processElement(in1: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,
                              collector: Collector[JsonNode]): Unit = {
    try{
      val alarmRuleResult = toBean[FdcData[AlarmRuleResult]](in1)
      val data = alarmRuleResult.datas

      if(!RunIdMap.contains(data.runId)){

        try {
          // write the state back
          indicatorTimeOutState.update(indicator0DownTime(data.runId, System.currentTimeMillis()))

          // schedule the next timer 60 seconds from the current event time
          context.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 90000L)
        }catch {
          case ex: Exception => logger.warn("processEnd registerProcessingTimeTimer error: " + ex.toString)
        }

        RunIdMap.put(data.runId, scala.collection.mutable.Set())
      }


      val key = data.indicatorId.toString
      val cacheKey = s"${data.runId}|${data.indicatorId.toString}"


      // 新流处理
      if(data.configVersion == data.dataVersion){

        if(indicatorResultStateMap.contains(cacheKey)){
          // 旧流先到了, 那就把缓存的旧流删掉
          indicatorResultStateMap.remove(cacheKey)
        }

        // 缓存indicator
        if(RunIdMap.contains(data.runId)){
          val indicatorSet = RunIdMap(data.runId)

          // 不是cycle window 的indicator, 之前已经写过一次了就不在重复写入
          if(alarmRuleResult.datas.cycleIndex == "-1" && indicatorSet.contains(key)){
            return
          }
          indicatorSet.add(key)
          RunIdMap.put(data.runId, indicatorSet)
        }else{
          RunIdMap.put(data.runId, scala.collection.mutable.Set(key))
        }
      }

      // 旧流处理
      if(data.configVersion != data.dataVersion){

        if(RunIdMap.contains(data.runId)){
          val indicatorSet = RunIdMap(data.runId)

          // 新流还没有写入，旧流就先保存下来
          if(!indicatorSet.contains(key)){
            indicatorResultStateMap.put(s"${data.runId}|${data.indicatorId.toString}", in1)
          }
        }
        return
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(),
        Map("function" -> "MainFabIndicator0DownTimeKeyProcessFunction"), ex.toString).toJson)
    }
    collector.collect(in1)
  }
}
