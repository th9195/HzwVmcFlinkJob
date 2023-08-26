package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{ErrorCode, FdcData, MESMessage, MainFabRawData, RunData, RunEventData, toolMessage}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


class MainFabMESBloomFilterFunction extends KeyedProcessFunction[String, JsonNode, JsonNode] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabMESBloomFilterFunction])

  private var toolMessageBloomFilter: (Long, BloomFilter[String]) = _
  private var MESMessageBloomFilter: (Long, BloomFilter[String]) = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)


//    val toolMessageBloomFilterDescription = new
//        ValueStateDescriptor[(Long, BloomFilter[String])]("bloomFilterState", TypeInformation.of(classOf[(Long, BloomFilter[String])]))
//    toolMessageBloomFilter = getRuntimeContext.getState(toolMessageBloomFilterDescription)
//    val MESMessageBloomFilterDescription = new
//        ValueStateDescriptor[(Long, BloomFilter[String])]("bloomFilterState", TypeInformation.of(classOf[(Long, BloomFilter[String])]))
//    MESMessageBloomFilter = getRuntimeContext.getState(MESMessageBloomFilterDescription)
//

  }

  override def processElement(data: JsonNode, readOnlyContext: KeyedProcessFunction[String, JsonNode,
    JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try {


      //布隆过滤器一小时清理一次 为了DAC功能
      val nowTime = System.currentTimeMillis()
      if (toolMessageBloomFilter == null) {
        toolMessageBloomFilter = (System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 1000000))
      } else if (nowTime - toolMessageBloomFilter._1 >= 1 * 60 * 60 * 1000) {
        toolMessageBloomFilter = (System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 1000000))
      }

      if (MESMessageBloomFilter == null) {
        MESMessageBloomFilter = (System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 1000000))
      } else if (nowTime - MESMessageBloomFilter._1 >= 1 * 60 * 60 * 1000) {
        MESMessageBloomFilter = (System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 1000000))
      }

      if(data.findPath(MainFabConstants.dataType).asText().equals(MainFabConstants.toolMessage)){
        val myToolMessage = toBean[FdcData[toolMessage]](data).datas
        val sensorList = myToolMessage.sensors.filter(Sensor => {
          val mightData = s"${myToolMessage.locationName}|${myToolMessage.moduleName}|${myToolMessage.toolName}|${myToolMessage.chamberName}|${myToolMessage.recipeNames}|$Sensor"
          val isRepeat = toolMessageBloomFilter._2.mightContain(mightData)
          if (!isRepeat) {
            toolMessageBloomFilter._2.put(mightData)
          }
          !isRepeat
        })

        if (sensorList.nonEmpty) {
          val node = beanToJsonNode[toolMessage](toolMessage(locationName = myToolMessage.locationName,
            moduleName = myToolMessage.moduleName,
            toolName = myToolMessage.toolName,
            chamberName = myToolMessage.chamberName,
            sensors = sensorList,
            recipeNames = myToolMessage.recipeNames
          ))
          collector.collect(node)
        }
      }else if(data.findPath(MainFabConstants.dataType).asText().equals(MainFabConstants.MESMessage)){
        val myMESMessage = toBean[FdcData[MESMessage]](data)
        if (!MESMessageBloomFilter._2.mightContain(myMESMessage.datas.toJson)) {
          MESMessageBloomFilter._2.put(myMESMessage.datas.toJson)
          val node = beanToJsonNode[MESMessage](myMESMessage.datas)
          collector.collect(node)
        }
      }
    } catch {
      case exception: Exception => logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(), Map("MainFabRawDataListSize" -> "", "RunEventEnd" -> ""), exception.toString).toJson)

    }
  }

  override def close(): Unit = {
    super.close()
  }
}

