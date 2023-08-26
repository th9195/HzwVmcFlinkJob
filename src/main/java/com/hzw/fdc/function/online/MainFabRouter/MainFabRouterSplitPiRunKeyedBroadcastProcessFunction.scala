package com.hzw.fdc.function.online.MainFabRouter


import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{ConfigData, ErrorCode, FdcData, PiRunTool}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 *  分流机台到piRun环境
 */
class MainFabRouterSplitPiRunKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,
  JsonNode, JsonNode, JsonNode] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRouterSplitPiRunKeyedBroadcastProcessFunction])

  lazy val piRunOutput = new OutputTag[JsonNode]("piRunOutput")

  //分流到pirun环境的所有 tool
  val toolConfigSet: mutable.Set[String] = mutable.Set()

  // {traceId: "PIRUN"}
  private var traceIdMapState: MapState[String, String] = _

  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    val toolConfigList = InitPiRunToolOracle.initOracleConfig()
    toolConfigList.foreach(elem => toolConfigSet.add(elem))

    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    val traceIdMapStateDescription: MapStateDescriptor[String, String] = new
        MapStateDescriptor[String, String]("traceIdMapStateDescription", TypeInformation.of(classOf[String]),
          TypeInformation.of(classOf[String]))

    // 设置过期时间
    traceIdMapStateDescription.enableTimeToLive(ttlConfig)
    traceIdMapState = getRuntimeContext.getMapState(traceIdMapStateDescription)
  }

  /**
   *  数据流, pirun分流
   */
  override def processElement(value: JsonNode,
                              readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
                                JsonNode]#ReadOnlyContext,
                              collector: Collector[JsonNode]): Unit = {
    try {
      val toolName = value.findPath(MainFabConstants.toolName).asText()
      val traceId = value.findPath(MainFabConstants.traceId).asText()
      value.path(MainFabConstants.dataType).asText() match {

        case MainFabConstants.eventStart =>
          if(toolConfigSet.contains(toolName)){
            traceIdMapState.put(traceId, "PIRUN")
            readOnlyContext.output(piRunOutput, value)
          }else{
            collector.collect(value)
          }
        case MainFabConstants.rawData =>
          if(traceIdMapState.contains(traceId) && traceIdMapState.get(traceId) == "PIRUN"){
            readOnlyContext.output(piRunOutput, value)
          }else{
            collector.collect(value)
          }

        case MainFabConstants.eventEnd =>
          if(traceIdMapState.contains(traceId) && traceIdMapState.get(traceId) == "PIRUN"){
            readOnlyContext.output(piRunOutput, value)
          }else{
            collector.collect(value)
          }
          traceIdMapState.clear()
          close()
        case _ =>
          //输出
          collector.collect(value)
      }
    } catch {
      case e:Exception => logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map("data" -> value,
        "info" -> "分流到pirun环境报错"),ExceptionInfo.getExceptionInfo(e) ).toJson)
        collector.collect(value)
    }
  }

  override def processBroadcastElement(in2: JsonNode,
                                       context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context,
                                       collector: Collector[JsonNode]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()
      if(dataType == "pirunTool") {
        val message = toBean[ConfigData[PiRunTool]](in2)
        if(message.status){
          message.datas.tools.foreach(elem => toolConfigSet.add(elem))
        }else if ( !message.status){
          message.datas.tools.foreach(elem => toolConfigSet.remove(elem))
        }
      }else if(dataType == "allPirunTool"){
        val message = toBean[ConfigData[PiRunTool]](in2)
        toolConfigSet.clear()
        message.datas.tools.foreach(elem => toolConfigSet.add(elem))
      }
    } catch {
      case e:Exception => logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(),
        Map("data" -> in2, "info" -> "topic获取pirun tool配置报错"),ExceptionInfo.getExceptionInfo(e) ).toJson)
    }
  }

  override def close(): Unit = {
    super.close()
  }
}

