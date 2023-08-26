package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabWriteRedisDataDao
import com.hzw.fdc.util.redisUtils.RedisSinkScala
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author gdj
 * @create 2020-06-15-19:53
 *
 */
class MainFabWriteRedisDataService extends TService {
  private val dao = new MainFabWriteRedisDataDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWriteRedisDataService])


  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {
    val dataSource: DataStream[JsonNode] = getDatas()

    val sinkRedisData = dataSource.filter(elem => {
      val dataVersion = elem.findPath("dataVersion").asText()
      val configVersion = elem.findPath("configVersion").asText()
      if(null != dataVersion && null != configVersion && dataVersion == configVersion){
        true
      }else{
        false
      }
    })

    sinkRedisData.addSink(new RedisSinkScala())
      .name("Write Data to Redis")
      .uid("Write Data to Redis")

  }


  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_REDIS_DATA_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_WRITE_REDIS_DATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_WRITE_REDIS_DATA_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_WRITE_REDIS_DATA_JOB_FROM_TIMESTAMP)
    }else {
      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_REDIS_DATA_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_WRITE_REDIS_DATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_WRITE_REDIS_DATA_KAFKA_SOURCE_UID)
    }

  }


}
