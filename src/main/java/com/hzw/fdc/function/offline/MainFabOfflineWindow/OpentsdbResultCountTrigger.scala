package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.scalabean.{OfflineOpentsdbResult, OfflineTask}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

@SerialVersionUID(1L)
class OpentsdbResultCountTrigger extends Trigger[(OfflineTask, OfflineOpentsdbResult), GlobalWindow] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[OpentsdbResultCountTrigger])
  var count = new concurrent.TrieMap[String, Int]()

  override def onElement(t: (OfflineTask, OfflineOpentsdbResult), l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    val key = s"${t._1.taskId}|${t._2.runId}"

    val num = if (count.contains(key)) {
      count.get(key).get + 1
    } else {
      1
    }

    val max = t._2.taskSvidNum

    if (num >= max) {
      count.remove(key)
      TriggerResult.FIRE_AND_PURGE
    } else {
      count.put(key, num)
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: GlobalWindow, triggerContext: Trigger.TriggerContext): Unit = {

  }

  override def canMerge = true

  @throws[Exception]
  override def onMerge(w: GlobalWindow, ctx: Trigger.OnMergeContext): Unit = {

  }

  override def toString: String = "CountTrigger"

}
