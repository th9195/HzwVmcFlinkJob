package com.hzw.fdc.service.offline

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabOfflineResultDao
import com.hzw.fdc.function.offline.MainFabOfflineIndicator.IndicatorCommFunction
import com.hzw.fdc.function.offline.MainfabOfflineResult.{FdcOfflineHbaseSink, FdcOfflineResultProcessFunction}
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MainFabOfflineResultService extends TService with IndicatorCommFunction{

  private val dao = new MainFabOfflineResultDao
  lazy val OfflineIndicatorElemOutput = new OutputTag[OfflineIndicatorElem]("OfflineIndicatorElem")


  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineResultService])

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {
    // 数据流
    val indicatorDataStream: DataStream[JsonNode] = getDatas().union(ConfigOutputStream())


    val kafkaSink: DataStream[OfflineResultScala] = indicatorDataStream
      .keyBy(x => {
        val batchId = x.findPath("batchId").asText()
        batchId
      })
      .process(new FdcOfflineResultProcessFunction)
      .name("FdcOfflineResultStream")

    val OfflineIndicatorElemStream: DataStream[OfflineIndicatorElem] = kafkaSink.getSideOutput(OfflineIndicatorElemOutput)


    OfflineIndicatorElemStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[OfflineIndicatorElem](ProjectConfig.KAFKA_OFFLINE_INDICATOR_RESULT_TOPIC
        //按照tool分区
        , (e: OfflineIndicatorElem) => e.taskId.toString
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaOfflineIndicatorSink)
      .uid(MainFabConstants.KafkaOfflineIndicatorSink)


    kafkaSink.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[OfflineResultScala](ProjectConfig.KAFKA_OFFLINE_INDICATOR_RESULT_TOPIC
        //按照tool分区
        , (e: OfflineResultScala) => e.taskId.toString
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaOfflineResultSink)
      .uid(MainFabConstants.KafkaOfflineResultSink)
  }

  /**
   * 获取Indicator数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {

    getDao().getKafkaSource[JsonNode](
      ProjectConfig.KAFKA_OFFLINE_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_RESULT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_OFFLINE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_OFFLINE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
  }

  /**
   * 获取task数据
   */
  def ConfigOutputStream(): DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_OFFLINE_TASK_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_RESULT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_OFFLINE_RESULT_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_OFFLINE_RESULT_KAFKA_SOURCE_UID)
  }
}

