package com.hzw.fdc.function.online.MainFabAlarmHbase

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.util.MainFabConstants
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

class IndicatorWriteCountTrigger extends Trigger[JsonNode ,TimeWindow]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[IndicatorWriteCountTrigger])

  val maxCount: Int = 500
  var count = new concurrent.TrieMap[String, Int]()

  override def onElement(data: JsonNode, timestamp: Long, window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    val toolKey = try{
      val toolName = data.findPath(MainFabConstants.toolName).asText()
      val chamberName = data.findPath(MainFabConstants.chamberName).asText()
      s"$toolName|$chamberName"
    }catch {
      case ex: Exception => logger.warn("IndicatorWriteCountTrigger onElement error: " + ex.toString)
        ""
    }

    val currentCount = if (count.contains(toolKey)) { count(toolKey) + 1} else 1

    if (currentCount >= maxCount) {
      count.remove(toolKey)
      TriggerResult.FIRE_AND_PURGE
    } else {
      count.put(toolKey, currentCount)
      TriggerResult.CONTINUE
    }
  }


  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    super.onMerge(window, ctx)
  }

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    triggerContext.deleteProcessingTimeTimer(w.maxTimestamp())
  }

}
