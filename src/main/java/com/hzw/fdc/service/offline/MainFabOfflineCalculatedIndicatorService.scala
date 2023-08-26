package com.hzw.fdc.service.offline

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabCalculatedIndicatorDao
import com.hzw.fdc.function.MainFabSynchronizationPolicy.HbaseConfigSource
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.offline.MainFabOfflineIndicator.{IndicatorCommFunction, MainFabOfflineCalculatedProcessFunction}
import com.hzw.fdc.scalabean.{CheckConfig, FdcData, OfflineIndicatorResult}
import com.hzw.fdc.util.{FlinkStreamEnv, MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}
import com.hzw.fdc.json.MarshallableImplicits._

/**
 * @author gdj
 *  2020-09-07-11:48
 *
 */
class MainFabOfflineCalculatedIndicatorService extends TService with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineCalculatedIndicatorService])

  /**
   * 获取
   */
  override def getDao(): TDao = new MainFabCalculatedIndicatorDao

  /**
   * 分析
   */
  override def analyses(): Any = {

    // 数据流
    val indicatorDataStream: DataStream[FdcData[OfflineIndicatorResult]] = getDatas()


    val fdcindicatorDataStream  = indicatorDataStream
      .keyBy(_.datas.taskId)
      .process(new MainFabOfflineCalculatedProcessFunction)
      .name("FdcCalculatedIndicator")

    val indicatorsink: DataStream[(OfflineIndicatorResult, Boolean, Boolean, Boolean)] = fdcindicatorDataStream
      .flatMap(indicator => indicator)

    writeKafka(indicatorsink)

//    /**
//     *  author：yuxiang
//     *  time： 2022.05.31
//     *  功能：同步策略操作
//     *   ==========================同步策略操作 ==========================
//     *   返回给后台，hbase读取的配置，和增量从kafka topic 读取的配置
//     */
//
//    val env = FlinkStreamEnv.get()
//    val HbaseConfigSteam: DataStream[CheckConfig] = env.addSource(new HbaseConfigSource()).map(x => {
//      CheckConfig(
//        checkType="hbase",
//        message=x
//      )
//    })
//      .name("HbaseConfigSteam")
//      .uid("HbaseConfigSteam")
//
//    //读取context配置
//    val contextConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_CHECK_CONTEXT_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_CHECK_CONTEXT_CONFIG_KAFKA_SOURCE_UID)
//
//    //读window配置
//    val windowConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_WINDOW_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_CEHCK_WINDOW_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_CEHCK_WINDOW_CONFIG_KAFKA_SOURCE_UID)
//
//    //读Indicator配置
//    val indicatorConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_CHECK_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_CHECK_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
//
//    // alarm 配置
//    val alarmConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_ALARM_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID)
//
//    // virtual sensor 配置
//    val virtualSensorConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_VIRTUAL_SENSOR_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_CHECK_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_CHECK_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID)
//
//    // autolimt配置
//    val autolimitConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_AUTO_LIMIT_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_CHECK_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_CHECK_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID)
//
//    val ConfigDataStream = windowConfigDataStream.union(indicatorConfigDataStream,
//      contextConfigDataStream, alarmConfigDataStream, virtualSensorConfigDataStream, autolimitConfigDataStream)
//
//    val ConfigDataStream1 = ConfigDataStream.map(elem => {
//      CheckConfig(checkType = "topic", message = elem)
//    })
//
//    /**
//     *  将配置信息返回给后台
//     */
//    ConfigDataStream1.union(HbaseConfigSteam).addSink(new FlinkKafkaProducer(
//      ProjectConfig.KAFKA_MAINFAB_CHECK_CONFIG_TOPIC,
//      new FdcKafkaSchema[CheckConfig](ProjectConfig.KAFKA_MAINFAB_CHECK_CONFIG_TOPIC,
//        (element: CheckConfig) => element.checkType)
//      , ProjectConfig.getKafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
//    )).name("Kafka Sink To MainFabSynchronizationPolicy").uid("MainFabSynchronizationPolicy_sink")
  }



  /**
   * 获取Indicator数据
   */
  override protected def getDatas(): DataStream[FdcData[OfflineIndicatorResult]] = {

    getDao().getKafkaSource[FdcData[OfflineIndicatorResult]](
      ProjectConfig.KAFKA_OFFLINE_CALCULATED_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CALCULATED_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CALCULATED_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
  }
}

