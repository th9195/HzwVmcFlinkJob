package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.scalabean.OfflineVirtualSensorOpentsdbResult
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.concurrent

class VirtualSensorCountTrigger extends Trigger[OfflineVirtualSensorOpentsdbResult ,TimeWindow]{

  var count = new concurrent.TrieMap[String, Int]()

  override def onElement(t: OfflineVirtualSensorOpentsdbResult, l: Long, w: TimeWindow, triggerContext:
  Trigger.TriggerContext): TriggerResult = {

    val key = t.taskId.toString

    val num = if (count.contains(key)) {
      count(key) + 1
    } else {
      1
    }

    if (num >= 100) {
      count.remove(key)
      TriggerResult.FIRE_AND_PURGE
    } else {
      count.put(key, num)
      TriggerResult.CONTINUE
    }

  }

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    super.onMerge(window, ctx)
  }

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    triggerContext.deleteProcessingTimeTimer(w.maxTimestamp())
  }
}
