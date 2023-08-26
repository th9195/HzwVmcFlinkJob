package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.scalabean.{OfflineMainFabRawData, OfflineOpentsdbResult, OfflineTask, OfflineTaskRunData}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

@SerialVersionUID(1L)
class OfflineTaskCountTrigger extends Trigger[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), GlobalWindow] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineTaskCountTrigger])
  var count = new concurrent.TrieMap[String, Int]()

  override def onElement(t: (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    val key = s"${t._1.taskId}"

    val num = if (count.contains(key)) {
      count.get(key).get + 1
    } else {
      1
    }

    val max = t._1.runData.size

    if (num >= max) {

      logger.warn(s"task count $count max $max taskId ${t._1.taskId} FIRE_AND_PURGE")
      count.remove(key)
      TriggerResult.FIRE_AND_PURGE
    } else {
      logger.warn(s"task count $count max $max taskId ${t._1.taskId} CONTINUE")
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
