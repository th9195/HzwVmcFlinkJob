package com.hzw.fdc.service.offline

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabLogisticIndicatorDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.offline.MainFabOfflineIndicator.IndicatorCommFunction
import com.hzw.fdc.function.offline.MainFabOfflineLogisticIndicator.{MainFabOfflineCollectLogisticIndicatorProcessFunction}
import com.hzw.fdc.function.online.MainFabLogisticIndicator.{MainFabCollectLogisticIndicatorProcessFunction, MainFabLogisticIndicatorProcessFunction}
import com.hzw.fdc.scalabean.{FdcData, IndicatorConfig, IndicatorResult, OfflineIndicatorResult}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer


/**
 * @author ：gdj
 * @date ：Created in 2021/7/21 11:25
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineLogisticIndicatorService extends TService with IndicatorCommFunction{
  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = new MainFabLogisticIndicatorDao

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any ={
    // 数据流
    val indicatorDataStream: DataStream[FdcData[OfflineIndicatorResult]] = getDatas()

    val logisticIndicatorResultDataStream=indicatorDataStream
      .keyBy(x=>x.datas.taskId)
      .process(new MainFabOfflineCollectLogisticIndicatorProcessFunction)
      .name("MainFabCollectLogisticIndicatorProcessFunction")

    writeKafka(logisticIndicatorResultDataStream)
  }


  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[FdcData[OfflineIndicatorResult]] = {

    getDao().getKafkaSource[FdcData[OfflineIndicatorResult]](
      ProjectConfig.KAFKA_OFFLINE_LOGISTIC_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_LOGISTIC_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID)

  }






}


