package com.hzw.fdc.service.bi_report

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabWriteDataFromKafkaToDgDao
import com.hzw.fdc.function.PublicFunction.{FdcKafkaSchema, MainFabCycleWindowKeyedProcessFunction}
import com.hzw.fdc.function.online.MainFabAlarm.{IndicatorTimeOutKeyedBroadcastProcessFunction, MainFabIndicator0DownTimeKeyProcessFunction}
import com.hzw.fdc.json.JsonUtil
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Package com.hzw.fdc.service.bi_report
  * @author wanghb
  * @date 2023-04-29 3:19
  * @desc
  * @version V1.0
  */
class MainFabWriteDataFromKafkaToDgService extends TService{
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWriteDataFromKafkaToDgService])

  private val Dao = new MainFabWriteDataFromKafkaToDgDao

  /**
    * 获取
    *
    * @return
    */
  override def getDao(): TDao = Dao

  /**
    * 分析
    *
    * @return
    */
  override def analyses(): Any = {

    //runData 数据流
    val RunDataDS: DataStream[String] = getDatas()

    //indicator 数据流
    val sourceDS: DataStream[JsonNode] = getIndicatorDatas()

    //1.开始处理runData
    RunDataDS
      .map(JsonUtil.toJsonNode(_: String))
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_MAINFAB_BI_RUNDATA_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_BI_RUNDATA_TOPIC
          // 随机分区
          , x => s"${x.findPath(MainFabConstants.traceId).asText()}"
        )
        , ProjectConfig.getDGKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("bi report rundata to dg")
      .uid("bi report rundata to dg")

    //2.indicator原始数据
    sourceDS
      .filter(_.findPath(MainFabConstants.dataType).asText() == "AlarmLevelRule")
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_RESULT_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_RESULT_TOPIC
          // 随机分区
          , x => s"${x.findPath(MainFabConstants.toolName).asText()}|${x.findPath(MainFabConstants.indicatorId).asText()}"
        )
        , ProjectConfig.getDGKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("bi report indicator to dg")
      .uid("bi report indicator to dg")

    //3.indicator error数据(只发错误的,后续在dg端再union indicator原始数据)
    val indicatorEtled: DataStream[JsonNode] = indicatorDataDSEtled(sourceDS)
    val runData: DataStream[JsonNode] = RunDataDS.map(JsonUtil.toJsonNode(_: String))
    val indicatorErrorData: DataStream[IndicatorErrorReportData] = calcIndicatorError(indicatorEtled,runData,getFdcConfig)

    indicatorErrorData.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_ERROR_TOPIC,
      new FdcKafkaSchema[IndicatorErrorReportData](ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_ERROR_TOPIC
        , x => s"${x.runId}"
      )
      , ProjectConfig.getDGKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("bi report indicatorError to dg")
      .uid("bi report indicatorError to dg")

  }


  /**
    * 整理原始的来自kafka的indicator数据,供后续使用
    */

  def indicatorDataDSEtled(sourceDS: DataStream[JsonNode]) :DataStream[JsonNode] = {
    val filterAlarmDataDS: DataStream[JsonNode] = sourceDS.filter(data => {
      val dataType = data.findPath("dataType").asText()

      dataType == "AlarmLevelRule"
    })
      .keyBy(data => {
        val runId = data.findPath("runId").asText()
        s"$runId"
      })
      .process(new MainFabIndicator0DownTimeKeyProcessFunction())
      .name("MainFab 0DownTimeKeyProcessFunction -- dg")
      .uid("MainFab 0DownTimeKeyProcessFunction -- dg")
      .keyBy(
        data => {
          val toolName = data.findPath(MainFabConstants.toolName).asText()
          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
          s"$toolName|$chamberName"
        }
      ).process(new MainFabCycleWindowKeyedProcessFunction())

    filterAlarmDataDS
  }

  /**
    * 广播fdc配置数据
    */

  def getFdcConfig():BroadcastStream[JsonNode] = {
    //读取context配置
    val contextConfigDataStream: DataStream[JsonNode] = getContextConfig()

    //读Indicator配置
    val indicatorConfigDataStream: DataStream[JsonNode] = getIndicatorConfig()


    //indicator配置数据 维表流
    val indicatorConfig = new MapStateDescriptor[String, JsonNode](
      "timeout_write_indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val ConfigDataStream = contextConfigDataStream.union(indicatorConfigDataStream)

    //广播配置流
    val indicatorConfigBroadcastStream = ConfigDataStream.broadcast(indicatorConfig)

    indicatorConfigBroadcastStream
  }

  /**
    * 处理indicatorError的数据逻辑
    */

  def calcIndicatorError(indicatorDataEtled: DataStream[JsonNode], RunDataDS: DataStream[JsonNode], indicatorConfigBroadcastStream: BroadcastStream[JsonNode]): DataStream[IndicatorErrorReportData] ={
    val indicatorAndRunDataStream = indicatorDataEtled.union(RunDataDS)

    // 	FAB7-8 indicator多长时间没有算出来/多长时间没有收到rawdata要做告警
    val indicatorOutTimeStream: DataStream[IndicatorErrorReportData] = indicatorAndRunDataStream
      .keyBy(
        data => {
          val runId = data.findPath("runId").asText()
          s"$runId"
        })
      .connect(indicatorConfigBroadcastStream)
      .process(new IndicatorTimeOutKeyedBroadcastProcessFunction)
      .filter(x => x.indicatorValue.isEmpty)

    indicatorOutTimeStream
  }

  /**
    * 获取rundata原始json数据
    */
  override protected def getDatas(): DataStream[String] = {
    getDao().getKafkaOriginalSource(
      ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_DATA_FROM_KAFKA_TO_DG_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)

  }


  /**
    * 获取Indicator和IndicatorRule数据
    */
  def getIndicatorDatas(): DataStream[JsonNode] = {

    getDao().getKafkaSource[JsonNode](
      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_DATA_FROM_KAFKA_TO_DG_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_INDICATOR_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_INDICATOR_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)

  }

  /**
    * 获取context配置数据
    */
  def getContextConfig(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_DATA_FROM_KAFKA_TO_DG_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)
  }

  /**
    * 获取indicator配置数据
    */
  def getIndicatorConfig(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_DATA_FROM_KAFKA_TO_DG_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
  }

}

