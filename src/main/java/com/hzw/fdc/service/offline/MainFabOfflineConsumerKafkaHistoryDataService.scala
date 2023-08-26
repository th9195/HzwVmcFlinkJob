package com.hzw.fdc.service.offline

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao. MainFabOfflineConsumerKafkaHistoryDataDao
import com.hzw.fdc.function.offline.MainFabOfflineConsumerKafkaHistoryData._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks._
/**
 * MainFabOfflineConsumerKafkaHistoryDataService
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/24 10:25
 * @since 1.0.0
 * @update 2022/11/24 10:25
 * */
class MainFabOfflineConsumerKafkaHistoryDataService extends TService {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineConsumerKafkaHistoryDataService])

  private val Dao = new MainFabOfflineConsumerKafkaHistoryDataDao
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

    // todo 获取数据源
    val sourceDataStream: DataStream[JsonNode] = getDatas()

    val resultDataStream: DataStream[JsonNode] = sourceDataStream.keyBy(elem => {
      val traceId: String = elem.findPath("traceId").asText()
      val runId: String = elem.findPath("runId").asText()
      traceId + runId
    }).process(new MainFabOfflineKafkaHistoryDataFilter())


    // todo 实时写入指定的目录
//    resultDataStream.addSink(new MainFabOfflineKafkaHistoryDataFileSink)

  }

  override def getDatas() = {

    getDao().getKafkaJsonSourceByTimestamp(
      ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_GROUPID,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CONSUMER_KAFKA_HISTORY_DATA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CONSUMER_KAFKA_HISTORY_DATA_SOURCE_UID,
      ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP)

  }


}
