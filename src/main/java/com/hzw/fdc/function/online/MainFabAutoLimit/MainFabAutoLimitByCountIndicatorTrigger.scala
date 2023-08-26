package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.scalabean.{AutoLimitOneConfig, IndicatorResult}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author ：gdj
 * @date ：Created in 2021/6/14 1:58
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
@SerialVersionUID(1L)
class MainFabAutoLimitByCountIndicatorTrigger extends  Trigger[(AutoLimitOneConfig,IndicatorResult),GlobalWindow]{



  val timeMapState = new MapStateDescriptor("autoLimitCount", classOf[String],classOf[Long])


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitByTimeTrigger])


  override def onElement(element: (AutoLimitOneConfig, IndicatorResult), timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val timeMap = ctx.getPartitionedState(timeMapState)

    val key=s"${element._2.runId}|${element._1.controlPlanId}"

    if (timeMap.contains(key)) {
      ctx.deleteProcessingTimeTimer(timeMap.get(key))
    }
    timeMap.put(key,ctx.getCurrentProcessingTime+60000L)
    ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime+60000L)
    TriggerResult.CONTINUE


  }

  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
    val timeMap = ctx.getPartitionedState(timeMapState)
    timeMap.clear()
  }

  override def canMerge: Boolean = true

  override def onMerge(window: GlobalWindow, ctx: Trigger.OnMergeContext): Unit = {
    super.onMerge(window, ctx)
  }

}
