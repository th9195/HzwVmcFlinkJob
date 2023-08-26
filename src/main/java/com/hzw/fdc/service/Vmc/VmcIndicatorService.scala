package com.hzw.fdc.service.Vmc

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.Vmc.VmcIndicatorDao
import com.hzw.fdc.function.online.MainFabAlarm._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowService
 */
class VmcIndicatorService extends TService {
  private val dao = new VmcIndicatorDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcIndicatorService])


  lazy val AlarmSwitchEventOutput = new OutputTag[List[AlarmSwitchEventConfig]]("AlarmSwitchEvent")

  lazy val ewmaCacheOutput = new OutputTag[JsonNode]("AlarmEwmaCache")

  // ewmaRetargetResult 数据侧道输出
  lazy val retargetResultOutput = new OutputTag[JsonNode]("EwmaRetargetResult")

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

    //indicator 数据流
    val filterAlarmDataDS: DataStream[JsonNode] = getDatas()


    //alarm配置数据 维表流
    val alarm_config = new MapStateDescriptor[String, JsonNode](
      "alarm_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    //广播配置流
    val alarmConfigBroadcastStream: BroadcastStream[JsonNode] = ConfigOutputStream.broadcast(alarm_config)


  }




  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_ALARM_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_ALARM_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_ALARM_JOB_FROM_TIMESTAMP)
    }else {
      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_ALARM_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_ALARM_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
    }

  }


  /**
   * 获取配置的增量数据
   *
   * @return DataStream[JsonNode]
   */
  def ConfigOutputStream: DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_ALARM_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID)
  }

  /**
   * 获取Indicator配置数据 ConfigData[IndicatorConfig]
   */
  def getIndicatorConfig(): DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
  }



}
