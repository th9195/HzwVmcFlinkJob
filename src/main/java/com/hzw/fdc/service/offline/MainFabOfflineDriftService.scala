package com.hzw.fdc.service.offline

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabOfflineDriftDao
import com.hzw.fdc.function.offline.MainFabOfflineIndicator._
import com.hzw.fdc.function.online.MainFabIndicator.FdcAbsProcessFunction
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 *  离线的custom计算
 *
 */
class MainFabOfflineDriftService extends TService with IndicatorCommFunction {

  private val dao = new MainFabOfflineDriftDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineDriftService])

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {
    //indicator 数据流
    val indicatorDataStream: DataStream[FdcData[OfflineIndicatorResult]] = getDatas()


    val fdcDriftIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
      .keyBy(x => {x.datas.taskId.toString + x.datas.indicatorId.toString})
      .process(new FdcOfflineDriftProcessFunction)
      .name("FdcOfflineDriftIndicator")


    // 利用历史值计算平均数
    val fdcMovAvgIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
      .keyBy(x => {x.datas.taskId.toString + x.datas.indicatorId.toString})
      .process(new FdcOfflineMovAvgProcessFunction)
      .name("FdcOfflineMovAvgIndicator")


    // 移动加权平均数
    val fdcEWMAIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
      .keyBy(x => {x.datas.taskId.toString + x.datas.indicatorId.toString})
      .process(new FdcOfflineEWMAProcessFunction)
      .name("FdcOfflineEWMAIndicator")

    // 求和的平均值
    val fdcAvgIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
      .keyBy(x => {x.datas.taskId.toString + x.datas.indicatorId.toString})
      .process(new FdcOfflineAvgProcessFunction)
      .name("FdcOfflineAvgIndicator")


    // Drift(Percent)计算逻辑
    val fdcDriftPercentIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
      .keyBy(x => {x.datas.taskId.toString + x.datas.indicatorId.toString})
      .process(new FdcOfflineDriftPercentProcessFunction)
      .name("FdcOfflineDriftPercentIndicator")

    // 离线LinearFit indicator计算逻辑
    val FdcOfflineLinearFitIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
      .keyBy(x => {x.datas.taskId.toString + x.datas.indicatorId.toString})
      .process(new FdcOfflineLinearFitProcessFunction)
      .name("FdcOfflineLinearFitIndicator")

    val fdcAbsIndicator: DataStream[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] =
      indicatorDataStream
        .keyBy(x => x.datas.taskId.toString + x.datas.indicatorId.toString)
        .process(new FdcOfflineAbsProcessFunction)
        .name("FdcOfflineAbsIndicator")

    val indicatorSink = fdcDriftIndicator.union(fdcMovAvgIndicator, fdcEWMAIndicator, fdcAvgIndicator,
      fdcDriftPercentIndicator, FdcOfflineLinearFitIndicator, fdcAbsIndicator).flatMap(indicator => indicator)

    writeKafka(indicatorSink)
  }


  /**
   * 获取Indicator数据
   */
  override protected def getDatas(): DataStream[FdcData[OfflineIndicatorResult]] = {

    getDao().getKafkaSource[FdcData[OfflineIndicatorResult]](
      ProjectConfig.KAFKA_OFFLINE_DRIFT_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_DRIFT_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_OFFLINE_DRIFT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_OFFLINE_DRIFT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
  }
}

