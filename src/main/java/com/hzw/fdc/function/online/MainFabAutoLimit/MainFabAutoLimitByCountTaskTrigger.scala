package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.scalabean.{AutoLimitOneConfig, IndicatorResult}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author ：gdj
 * @date ：Created in 2021/6/27 23:04
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitByCountTaskTrigger extends Trigger[List[(AutoLimitOneConfig, IndicatorResult)], GlobalWindow] {

  //controlPlanId -->count
  val mapCountState = new MapStateDescriptor("autoLimitTaskCount", classOf[String],classOf[Set[String]])

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitByCountTaskTrigger])

  override def onElement(t: List[(AutoLimitOneConfig, IndicatorResult)], l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    val taskCountMap = triggerContext.getPartitionedState(mapCountState)
    val (conf,rs) = t.head
    val key=s"${conf.controlPlanId.toString}"
    val runSet = if (taskCountMap.contains(key)) {
      taskCountMap.get(key) + rs.runId
    } else {
      Set(rs.runId)
    }
    if (runSet.size >= conf.triggerMethodValue.toInt) {
      clear(w, triggerContext)
      return TriggerResult.FIRE_AND_PURGE
    } else {
      taskCountMap.put(key,runSet)
    }
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(w: GlobalWindow, triggerContext: Trigger.TriggerContext): Unit = triggerContext.getPartitionedState(mapCountState).clear()

  override def canMerge: Boolean = true

  override def onMerge(window: GlobalWindow, ctx: Trigger.OnMergeContext): Unit = {
    super.onMerge(window, ctx)
  }
}
