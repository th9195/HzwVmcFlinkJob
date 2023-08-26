package com.hzw.fdc.function.offline.MainFabOfflineIndicator


import java.util.Properties
import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorResult, kafkaOfflineIndicatorResult}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala._
import com.hzw.fdc.common.TDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.service.offline.MainFabOfflineDriftService
import com.hzw.fdc.util.ProjectConfig

trait IndicatorCommFunction extends TDao {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineDriftService])

  def parseOption[A](a: Option[A]): A = {
    a.get
  }

  def Try[A](a: => A): Option[A] = {
    try Some(a)
    catch { case e: Exception => None}
  }

  /**
   *  写入kafka
   */
  def writeKafka(indicatorOutputStream: DataStream[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]): Unit ={
    val indicatorJson: DataStream[FdcData[OfflineIndicatorResult]] = indicatorOutputStream
      .map(i => {
      FdcData("offlineIndicator", i._1)
    })

    val driftIndicatorStream: DataStream[FdcData[OfflineIndicatorResult]] = indicatorOutputStream
      .filter(_._2 ==true)
      .map(i => {
        FdcData("offlineIndicator", i._1)
      })

    val calculatedIndicatorStream: DataStream[FdcData[OfflineIndicatorResult]] = indicatorOutputStream
      .filter(_._3 ==true)
      .map(i => {
        FdcData("offlineIndicator", i._1)
      })

    val logisticIndicatorStream: DataStream[FdcData[OfflineIndicatorResult]] = indicatorOutputStream
      .filter(_._4 ==true)
      .map(i => {
        FdcData("offlineIndicator", i._1)
      })



    indicatorJson.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[OfflineIndicatorResult]](ProjectConfig.KAFKA_OFFLINE_INDICATOR_TOPIC,
        (element: FdcData[OfflineIndicatorResult]) => element.datas.taskId.toString + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To Indicator").uid("IndicatorJob_sink")

    driftIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_DRIFT_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[OfflineIndicatorResult]](ProjectConfig.KAFKA_OFFLINE_DRIFT_INDICATOR_TOPIC,
        (element: FdcData[OfflineIndicatorResult]) => element.datas.taskId.toString + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To driftIndicatorStream").uid("IndicatorJob_drift_sink")

    calculatedIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_CALCULATED_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[OfflineIndicatorResult]](ProjectConfig.KAFKA_OFFLINE_CALCULATED_INDICATOR_TOPIC,
        (element: FdcData[OfflineIndicatorResult]) => element.datas.taskId.toString + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To calculatedIndicatorStream").uid("IndicatorJob_calculated_sink")

    logisticIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_LOGISTIC_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[OfflineIndicatorResult]](ProjectConfig.KAFKA_OFFLINE_LOGISTIC_INDICATOR_TOPIC,
        (element: FdcData[OfflineIndicatorResult]) => element.datas.taskId.toString + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To  logisticIndicatorStream").uid("IndicatorJob_logistic_sink")
  }
}


