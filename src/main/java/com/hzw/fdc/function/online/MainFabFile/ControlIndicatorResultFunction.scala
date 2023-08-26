package com.hzw.fdc.function.online.MainFabFile


import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{FdcData, IndicatorResultFileScala, RunData}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


class ControlIndicatorResultFunction extends RichCoFlatMapFunction[IndicatorResultFileScala, JsonNode, IndicatorResultFileScala]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[ControlIndicatorResultFunction])

  // {"runId": IndicatorFileResult}
  private var indicatorState: MapState[String, IndicatorResultFileScala] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val indicatorStateDescription: MapStateDescriptor[String, IndicatorResultFileScala] = new MapStateDescriptor[String,
      IndicatorResultFileScala]("indicatorFileResultState", TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[IndicatorResultFileScala]))

    indicatorState = getRuntimeContext.getMapState(indicatorStateDescription)
  }

  override def flatMap1(in1: IndicatorResultFileScala, collector: Collector[IndicatorResultFileScala]): Unit = {
    try{
      indicatorState.put(in1.runId, in1)
    }catch {
      case e: Exception => logger.warn(s"indicator flatMap1 error $e :${in1}")
    }
  }

  override def flatMap2(in2: JsonNode, collector: Collector[IndicatorResultFileScala]): Unit = {
    try{
      val rundata = toBean[FdcData[RunData]](in2).datas
      val runId = rundata.runId
      if(indicatorState.contains(runId)){
        val indicatorResult = indicatorState.get(runId)
        indicatorResult.copy(runEndTime = rundata.runEndTime.get)
        collector.collect(indicatorResult)
        indicatorState.remove(runId)
      }

    }catch {
      case e: Exception => logger.warn(s"indicator flatMap2 error $e :${in2}")
    }
  }
}

