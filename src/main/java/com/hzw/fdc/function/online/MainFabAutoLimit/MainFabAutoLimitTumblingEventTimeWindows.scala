package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.scalabean.{AutoLimitOneConfig, IndicatorResult}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Collections

@SerialVersionUID(1L)
class MainFabAutoLimitTumblingEventTimeWindows extends WindowAssigner[(AutoLimitOneConfig, IndicatorResult),TimeWindow]{
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitTumblingEventTimeWindows])

  override def assignWindows(element: (AutoLimitOneConfig, IndicatorResult),
                             timestamp: Long,
                             context: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {


    if (timestamp > Long.MinValue) {

//      val dataTimestamp = element._2.indicatorCreateTime
      val dataTimestamp = System.currentTimeMillis()
      val windowSize = element._1.triggerMethodValue.toLong

      val start = dataTimestamp - (dataTimestamp + windowSize) % windowSize

      Collections.singletonList(new TimeWindow(start, start + windowSize))
    }
    else {
      throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
        + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
        + "'DataStream.assignTimestampsAndWatermarks(...)'?")
    }
  }

  override def getDefaultTrigger(env: StreamExecutionEnvironment): Trigger[(AutoLimitOneConfig, IndicatorResult), TimeWindow] = {
    new MainFabAutoLimitByTimeTrigger
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
     new TimeWindow.Serializer
  }

  override def isEventTime: Boolean = {
    false
  }

//  override def toString: String = "hzwSubFabTumblingEventTimeWindows toString"

  def create(): MainFabAutoLimitTumblingEventTimeWindows ={
    new MainFabAutoLimitTumblingEventTimeWindows
  }


}
