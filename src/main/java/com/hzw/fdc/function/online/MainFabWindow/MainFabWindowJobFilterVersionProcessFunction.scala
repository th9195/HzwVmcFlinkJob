package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}



/**
 * @author ：gdj
 * @date ：2021/11/27 16:01
 * @param $params
 * @return $returns
 */
class MainFabWindowJobFilterVersionProcessFunction extends KeyedProcessFunction[String,JsonNode,JsonNode] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowJobFilterVersionProcessFunction])

  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)
  }

  override def processElement(i: JsonNode,
                              context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,
                              collector: Collector[JsonNode]): Unit = {
    try {
      val dataType = i.findPath(MainFabConstants.dataType).asText()
      if (dataType.equals(MainFabConstants.rawData)
        || dataType.equals(MainFabConstants.eventStart)
        || dataType.equals(MainFabConstants.eventEnd)) {

        val dataVersion = i.findPath(MainFabConstants.dataVersion).asText()

        //数据版本 == job版本 写入raw data库
        if (dataVersion == ProjectConfig.JOB_VERSION) {
          collector.collect(i)

        } else {
          logger.warn(s"debug dataVersion $dataVersion  ProjectConfig.JOB_VERSION ${ProjectConfig.JOB_VERSION}")

        }
      }
    }catch {
      case ex: Exception => logger.warn(s"MainFabWindowJobFilterVersionProcessFunction error: $ex")
    }
  }
}
