package com.hzw.fdc.service.offline

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabOfflineAutoLimitDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.offline.MainFabOfflineAutoLimit.{MainfabOfflineAutolimitCalcKeyedPorcessFunction, MainfabOfflineAutolimitCreateTaskKeyedPorcessFunction, MainfabOfflineAutolimitParseIndicatorKeyedPorcessFunction}
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
/**
 * MainFabOfflineAutoLimitService
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/10 13:52
 * @since 1.0.0
 * @update 2022/8/10 13:52
 * */
class MainFabOfflineAutoLimitService  extends TService {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineAutoLimitService])

  lazy val outputTagByCount = new OutputTag[(AutoLimitOneConfig, IndicatorResult)]("ByCount")
  lazy val outputAutoLimitTask = new OutputTag[AutoLimitTask]("AutoLimitTask")
  private val Dao = new MainFabOfflineAutoLimitDao

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

    // todo 1- 实时获取save时 的offlineAutoLimit 配置信息
    val autoLimitConfigStream: DataStream[JsonNode] = getAutoLimitConfigStream()

    // todo 2- 过滤
    val filterOfflineAutoLimitConfigStream = autoLimitConfigStream.filter(elem => {
      val dataType = elem.findPath("dataType").asText()
      val status = elem.findPath("status").asText().toBoolean
      if (dataType == "offlineAutoLimitSettings" && status) {

        val controlPlanId = elem.findPath("controlPlanId").asText()
        val controlPlanVersion = elem.findPath("version").asText()
        val currentTimestamp = System.currentTimeMillis()

        logger.warn(s"---------------------get offlineAutoLimitSettings : ${elem}")

        true
      } else {
        false
      }
    })

    // todo 3- 生成 OfflineCreateTask
    val offlineAutoLimitCreateTask = filterOfflineAutoLimitConfigStream
      .keyBy(elem => {
        val controlPlanId = elem.findPath("controlPlanId").asText()
        val controlPlanVersion = elem.findPath("version").asText()
        val endTimestamp = elem.findPath("endTimestamp").asText()
        controlPlanId + controlPlanVersion + endTimestamp
      })
      .process(new MainfabOfflineAutolimitCreateTaskKeyedPorcessFunction())

    // todo 4- sink offlineAutoLimitCreateTask to kafka
    offlineAutoLimitCreateTask
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC
          //按照taskId分区
          , (e: JsonNode) => e.findPath("taskId").asText()
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        .name("offline auto limit createTask")
        .uid("offline auto limit createTask")


    // todo 5- 解析配置信息封装以 indicatorId为单位的配置信息 (目的: 可以在下一步 根据 IndicatorId 分布式计算)
    val offlineAutoLimitIndicatorConfig: DataStream[JsonNode] = filterOfflineAutoLimitConfigStream
      .keyBy(elem => {
        val controlPlanId = elem.findPath("controlPlanId").asText()
        val controlPlanVersion = elem.findPath("version").asText()
        val endTimestamp = elem.findPath("endTimestamp").asText()
        controlPlanId + controlPlanVersion + endTimestamp
      })
      .process(new MainfabOfflineAutolimitParseIndicatorKeyedPorcessFunction())

    offlineAutoLimitIndicatorConfig.print(s"----本次所有计算AutoLimit 的Indicator----")

    // todo 6- 以IndicatorId keyBy 分布式计算每个IndicatorId 的多个AutoLimit
    val offlineAutoLimitResult = offlineAutoLimitIndicatorConfig
      .keyBy((elem: JsonNode) => {
        val offlineAutoLimitIndicatorConfig = toBean[OfflineAutoLimitIndicatorConfig](elem)
        val controlPlanId = offlineAutoLimitIndicatorConfig.controlPlanId
        val version = offlineAutoLimitIndicatorConfig.version
        val endTimestamp = offlineAutoLimitIndicatorConfig.endTimestamp
        val indicatorId = offlineAutoLimitIndicatorConfig.indicatorId
        controlPlanId + version + endTimestamp + indicatorId
      }).process(new MainfabOfflineAutolimitCalcKeyedPorcessFunction())


    //todo 7- sink offlineAutoLimitResult to kafka
    offlineAutoLimitResult
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC
          //按照taskId分区
          , (e: JsonNode) => e.findPath("taskId").asText()
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        .name("offline auto limit result")
        .uid("offline auto limit result")
  }

  /**
   * 获取配置的增量数据
   */
  def getAutoLimitConfigStream(): DataStream[JsonNode] = {

    getDao().getKafkaSource(ProjectConfig.KAFKA_AUTO_LIMIT_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_AUTOLIMIT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID)

  }


  /**
   * 获取Alarm配置以获取limit condition信息
   * @return
   */
  def getAlarmConfigStream(): DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_ALARM_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_AUTOLIMIT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONDITION_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONDITION_CONFIG_KAFKA_SOURCE_UID)
  }

}
