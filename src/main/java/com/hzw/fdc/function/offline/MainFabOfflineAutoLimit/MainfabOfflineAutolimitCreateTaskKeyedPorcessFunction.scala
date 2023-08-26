package com.hzw.fdc.function.offline.MainFabOfflineAutoLimit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, fromJson, toBean}
import com.hzw.fdc.scalabean.{OfflineAutoLimitIndicatorConfig, OfflineAutoLimitSettings, OfflineAutoLimitTask}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 * MainfabOfflineAutolimitCreateTaskKeyedPorcessFunction
 * 生成 OfflineAutoLimitTask
 * @desc:
 * @author tobytang
 * @date 2022/8/23 17:58
 * @since 1.0.0
 * @update 2022/8/23 17:58
 * */
class MainfabOfflineAutolimitCreateTaskKeyedPorcessFunction extends KeyedProcessFunction[String,JsonNode,JsonNode]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainfabOfflineAutolimitCreateTaskKeyedPorcessFunction])


  override def processElement(inRow: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    val offlineAutoLimitSettings = toBean[OfflineAutoLimitSettings](inRow)

    val offlineAutoLimitConfig = offlineAutoLimitSettings.datas

    val offlineAutoLimitTask = OfflineAutoLimitTask("offlineCreateTask",
      s"${offlineAutoLimitConfig.controlPlanId}-${offlineAutoLimitConfig.version}-${offlineAutoLimitConfig.endTimestamp}",
      offlineAutoLimitConfig.controlPlanId,
      System.currentTimeMillis()
    )

    logger.warn(s"offlineCreateTask == ${offlineAutoLimitTask}")
    val jsonNode = beanToJsonNode[OfflineAutoLimitTask](offlineAutoLimitTask)
    collector.collect(jsonNode)
  }
}
