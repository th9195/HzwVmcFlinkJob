package com.hzw.fdc.function.online.MainFabWindow


import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.PublicFunction.AddStep
import com.hzw.fdc.json.JsonUtil
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{ErrorCode, MainFabRawData}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


/**
 *   功能：
 *      1：给rawData结构体数据打stepId标签
 *      2： 如果没有stepId的情况下,使用上一个rawData的stepId; 如果是eventStart后的第一个没有stepId就给赋值1
 */
class MainFabWindowAddStepProcessFunction(isCacheFailSensor :Boolean) extends  KeyedProcessFunction[String,JsonNode,Option[JsonNode]]{
  val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowAddStepProcessFunction])


  //存traceId 和 上一个stepid
  var ptRawDataStateMap: MapState[String, Long] = _


  override def open(parameters: Configuration): Unit = {
    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()


    val ptRawDataStateMapDescription = new MapStateDescriptor[String, Long](s"ptRawDataState_${isCacheFailSensor.toString}",
      TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Long]))
    ptRawDataStateMapDescription.enableTimeToLive(ttlConfig)
    ptRawDataStateMap = getRuntimeContext.getMapState(ptRawDataStateMapDescription)
  }


  override def processElement(value: JsonNode, ctx: KeyedProcessFunction[String, JsonNode, Option[JsonNode]]#Context,
                              out: Collector[Option[JsonNode]]): Unit = {
    try {
      val dataType: String = value.get(MainFabConstants.dataType).asText()
      if (dataType == MainFabConstants.rawData) {
        val rawData: Option[MainFabRawData] = AddStep.addStep(value, logger, isCacheFailSensor, ptRawDataStateMap)
        if (rawData.nonEmpty) {
          out.collect(Option(JsonUtil.beanToJsonNode[MainFabRawData](rawData.get)))
        } else {
          None
        }
      } else {
        out.collect(Option(value))
        if(dataType == MainFabConstants.eventEnd){
          ptRawDataStateMap.clear()
          close()
        }
      }
    } catch {
      case exception: Exception =>logger.warn(ErrorCode("002003A001C", System.currentTimeMillis(),
        Map("desc" -> "addStepId数据处理异常", "rawData" -> value), exception.toString).toJson)
        out.collect(Option(value))
    }
  }


  override def close(): Unit = {
    super.close()
  }
}