package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.util.MainFabConstants
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.slf4j.{Logger, LoggerFactory}



/**
 * @author gdj
 * @create 2021-04-21-10:09
 *
 */
class MainFabWindowTrigger extends Trigger[JsonNode,GlobalWindow]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowTrigger])
  override def onElement(element: JsonNode, timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {


    if(element.get(MainFabConstants.dataType).asText()==MainFabConstants.eventEnd){
      TriggerResult.FIRE_AND_PURGE
    }else{

      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {

  }

}
