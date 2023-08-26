package com.hzw.fdc.function.online.MainFabRouter

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{ErrorCode, FdcData, MainFabPTRawData, MainFabRawData, RunEventData, UpDataMessage}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

/**
 * @author ：gdj
 * @date ：Created in 2021/11/20 15:58
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabRouterKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRouterKeyedBroadcastProcessFunction])
  //存储当前系统运行版本
  var upDataVersion: UpDataMessage = _
  //存储每个run start 所在的版本，run 以eventStart 版本为准，key =traceId v= run 当前版本
  private val eventStartMap = new concurrent.TrieMap[String, String]()

  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    logger.warn(s"Router ProjectConfig.JOB_VERSION: ${ProjectConfig.JOB_VERSION}")
    upDataVersion = UpDataMessage("update", ProjectConfig.JOB_VERSION, 0L)
  }

  override def processElement(value: JsonNode,
                              readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                              collector: Collector[JsonNode]): Unit = {

    try {
      value.path(MainFabConstants.dataType).asText() match {

        case MainFabConstants.eventStart => {
          //解析
          val runStart = toBean[RunEventData](value)
          if (upDataVersion == null) {
            upDataVersion = (UpDataMessage(
              "update",
              ProjectConfig.JOB_VERSION,
              0L
            ))
          }
          val version = upDataVersion.version
          //添加版本。
          runStart.setDataVersion(version)
          //记录当前run版本。
          eventStartMap.put(runStart.traceId, version)

          val node = beanToJsonNode[RunEventData](runStart)
          //输出
          collector.collect(node)

        }

        case MainFabConstants.eventEnd => {
          //解析
          val runEnd = toBean[RunEventData](value)

          if (eventStartMap.contains(runEnd.traceId)) {
            val version = eventStartMap(runEnd.traceId)
            //添加版本
            runEnd.setDataVersion(version)
            //end 删除当前run
            eventStartMap.remove(runEnd.traceId)
          }else{
            runEnd.setDataVersion(upDataVersion.version)
          }

          val node = beanToJsonNode[RunEventData](runEnd)
          //输出
          collector.collect(node)
        }

        case MainFabConstants.rawData => {
          //解析
          val rawData = toBean[MainFabPTRawData](value)

          if (eventStartMap.contains(rawData.traceId)) {
            val version = eventStartMap(rawData.traceId)
            //添加版本
            rawData.setDataVersion(version)
          }else{
            rawData.setDataVersion(upDataVersion.version)
          }

          val node = beanToJsonNode[MainFabPTRawData](rawData)
          //输出
          collector.collect(node)
        }

        case _ => {
          //输出
          collector.collect(value)
        }


      }
    } catch {
      case e:Exception => logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("data" -> value),
        ExceptionInfo.getExceptionInfo(e) ).toJson)
    }
  }

  override def processBroadcastElement(in2: JsonNode,
                                       context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context,
                                       collector: Collector[JsonNode]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()
      if(dataType == "update") {
        val message = toBean[FdcData[UpDataMessage]](in2)
        logger.warn(s"UpDataMessage: $message")
        upDataVersion = message.datas
      }
    } catch {
      case e:Exception => logger.warn(ErrorCode("002003b009C", System.currentTimeMillis(), Map("data" -> in2),
        ExceptionInfo.getExceptionInfo(e) ).toJson)
    }
  }
}
