package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.scalabean.{AutoLimitOneConfig, IndicatorResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author ：gdj
 * @date ：Created in 2021/6/11 16:44
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitByTimeTrigger extends  Trigger[(AutoLimitOneConfig,IndicatorResult),TimeWindow]{



  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitByTimeTrigger])

  override def onElement(element: (AutoLimitOneConfig, IndicatorResult), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    //当前数据距离窗口结束的时间
    if (window.maxTimestamp() <= ctx.getCurrentProcessingTime) {
      // if the watermark is already past the window fire immediately

      TriggerResult.FIRE_AND_PURGE
    } else {
      ctx.registerEventTimeTimer(window.maxTimestamp())
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    if (time >= window.maxTimestamp) {
      TriggerResult.FIRE_AND_PURGE
    }
    else {
      TriggerResult.CONTINUE
    }

  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time >= window.maxTimestamp) {
      TriggerResult.FIRE_AND_PURGE
    }
    else {
      TriggerResult.CONTINUE
    }
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    super.onMerge(window, ctx)
  }

}
