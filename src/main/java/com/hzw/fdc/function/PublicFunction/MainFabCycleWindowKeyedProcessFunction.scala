package com.hzw.fdc.function.PublicFunction

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.scalabean.{AlarmRuleConfig, AlarmRuleResult, FdcData}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.mutable.ListBuffer

/**
 *  处理 window end 类型的 cycle window 的indicator 值拼接在一起
 */
class MainFabCycleWindowKeyedProcessFunction extends KeyedProcessFunction[String, JsonNode, JsonNode]  {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabCycleWindowKeyedProcessFunction])

  private var indicatorCacheMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, concurrent.TrieMap[Int, String]]]()

  /**
   *   初始化
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)
  }

  /**
   *  数据流
   */
  override def processElement(value: JsonNode, ctx: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,
                              out: Collector[JsonNode]): Unit = {
    try{
      val dataType = value.findPath("dataType").asText()
      if ("AlarmLevelRule".equals(dataType)) {
        val data = toBean[FdcData[AlarmRuleResult]](value)
        val alarmRuleResult = data.datas
        val runId = alarmRuleResult.runId
        val indicatorId = alarmRuleResult.indicatorId

        if (alarmRuleResult.runEndTime == 0L && alarmRuleResult.cycleIndex != "-1") {
//          logger.warn("alarmRuleResult: " + alarmRuleResult)
          // 缓存window end 类型 cycle window 的indicator值
          if (indicatorCacheMap.contains(runId)) {
            val indicatorIdMap = indicatorCacheMap(runId)
            if (indicatorIdMap.contains(indicatorId)) {
              val indicatorCacheValueMap = indicatorIdMap(indicatorId)
              indicatorCacheValueMap.put(alarmRuleResult.cycleIndex.toInt, alarmRuleResult.indicatorValue)
              indicatorIdMap.put(indicatorId, indicatorCacheValueMap)
              indicatorCacheMap.put(runId, indicatorIdMap)

              // 根据key排序
              val indicatorDataValueList = scala.collection.immutable.ListMap(indicatorCacheValueMap.toSeq.sortBy(_._1): _*).values.to[ListBuffer]

              val indicatorDataValue = indicatorDataValueList.reduce(_ + "|" + _)

              val alarm = alarmRuleResult.copy(indicatorValue = indicatorDataValue)
              val res = beanToJsonNode[FdcData[AlarmRuleResult]](FdcData[AlarmRuleResult]("AlarmLevelRule", alarm))

              /**
               *  修改value值输出
               */
              out.collect(res)
              return
            } else {
              val indicatorMapScala = concurrent.TrieMap[Int, String](alarmRuleResult.cycleIndex.toInt -> alarmRuleResult.indicatorValue)
              val indicatorCacheValueMapScala = concurrent.TrieMap[Long, concurrent.TrieMap[Int, String]](indicatorId -> indicatorMapScala)
              indicatorCacheMap.put(runId, indicatorCacheValueMapScala)
            }
          } else {
            // 清空indicatorCacheMap， 正常情况下只保留一个runId
            if (indicatorCacheMap.nonEmpty) {
              indicatorCacheMap.clear()
            }
            val indicatorMapScala = concurrent.TrieMap[Int, String](alarmRuleResult.cycleIndex.toInt -> alarmRuleResult.indicatorValue)
            val indicatorCacheValueMapScala = concurrent.TrieMap[Long, concurrent.TrieMap[Int, String]](indicatorId -> indicatorMapScala)
            indicatorCacheMap.put(runId, indicatorCacheValueMapScala)
          }
        } else {
          if (alarmRuleResult.runEndTime != 0L) {
            if (indicatorCacheMap.contains(runId)) {
              indicatorCacheMap.remove(runId)
            }
          }
        }
      }
    }catch {
      case e: Exception => logger.warn(s"MainFabCycleWindowKeyedProcessFunction error ${ExceptionInfo.getExceptionInfo(e)}\t $value")
    }finally {
      out.collect(value)
    }
  }
}
