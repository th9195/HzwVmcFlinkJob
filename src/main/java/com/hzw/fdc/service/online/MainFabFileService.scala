package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabFileDao
import com.hzw.fdc.function.PublicFunction.MainFabCycleWindowKeyedProcessFunction
import com.hzw.fdc.function.online.MainFabAlarm.MainFabIndicator0DownTimeKeyProcessFunction
import com.hzw.fdc.function.online.MainFabFile._
import com.hzw.fdc.json.JsonUtil.fromMap
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer


class MainFabFileService extends TService {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabFileService])

  private val Dao = new MainFabFileDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = Dao

  /**
   * 处理数据
   *
   * @return
   */
  override def analyses(): Any = {

    val rarTraceSourceStream = getDatas()

    //读取context配置
    val contextConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_FILE_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)


    //注册广播变量
    val config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))


    val ConfigBroadcastDataStream = contextConfigDataStream.broadcast(config)

    // 根据eventStart 和 eventEnd 截取整个run的所有rawData信息
    val rawDataStream = rarTraceSourceStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .connect(ConfigBroadcastDataStream)
      .process(new MainFabRawDataKeyedBroadcastProcessFunction)
      .name("RawDataKeyedBroadcast")
      .uid("RawDataKeyedBroadcast")


    // 写rawData文件
    rawDataStream
      .addSink(new MainFabRawTraceFileSink).name("rawdata sink").uid("rawdata sink")


    //###############################################################################
    /**
     *  indicator result 写文件
     */
    val indicatorSourceStream = getIndicatorData()
      .keyBy(data => {
        val runId = data.findPath("runId").asText()
        s"$runId"
      })
      .process(new MainFabIndicator0DownTimeKeyProcessFunction())
      .name("MainFabIndicator0DownTimeKeyProcessFunction")
      .uid("MainFabIndicator0DownTimeKeyProcessFunction")
      .keyBy(
        data => {
          val toolName = data.findPath(MainFabConstants.toolName).asText()
          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
          s"$toolName|$chamberName"
        }
      ).process(new MainFabCycleWindowKeyedProcessFunction())
      .name("MainFabCycleWindowKeyedProcessFunction")
      .uid("MainFabCycleWindowKeyedProcessFunction")

    val runDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_FILE_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID)


    // 组装获取一个run的所有indicator信息
    val IndicatorStream: DataStream[IndicatorResultFileScala] = indicatorSourceStream.union(runDataStream)
      .keyBy(data => {
        val runId = data.findPath("runId").asText()
        s"$runId"
      })
      .process(new MainFabIndicatorKeyedProcessFunction)
      .name("MainFabIndicatorKeyedProcessFunction")
      .uid("MainFabIndicatorKeyedProcessFunction")


    // 写indicator文件
    IndicatorStream
      .addSink(new MainFabIndicatorFileSink)
      .name("indicator sink").uid("indicator sink")

  }


  /**
   * 获取rawtrace数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_FILE_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RAWTRACE_FILE_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RAWTRACE_FILE_KAFKA_SOURCE_UID)
  }

  /**
   * 获取Indicator数据
   */
  def getIndicatorData(): DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_FILE_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_INDICATOR_FILE_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_INDICATOR_FILE_KAFKA_SOURCE_UID)
  }

}
