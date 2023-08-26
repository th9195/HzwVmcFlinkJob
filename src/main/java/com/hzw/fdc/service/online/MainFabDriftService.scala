package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabDriftDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabIndicator._
import com.hzw.fdc.scalabean.{ConfigData, FdcData, IndicatorConfig, IndicatorResult}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-08-29-14:19
 *
 */
class MainFabDriftService extends TService with IndicatorCommFunction {

  private val dao = new MainFabDriftDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabDriftService])

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {
    // 数据流
    val indicatorDataStream: DataStream[FdcData[IndicatorResult]] = getDatas()

    // 维表流
    val indicator_config = new MapStateDescriptor[String, ConfigData[IndicatorConfig]](
      "indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[ConfigData[IndicatorConfig]] {}))

    val indicatorConfigBroadcastStream = getIndicatorConfig().broadcast(indicator_config)


    val fdcDriftIndicator: DataStream[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] = indicatorDataStream
      .keyBy(_.datas.indicatorId)
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcDriftConfigBroadcastProcessFunction)
      .name("FdcDriftIndicator")

    val driftAdvancedIndicatorCacheData = fdcDriftIndicator.getSideOutput(advancedIndicatorCacheDataOutput)


    // 利用历史值计算平均数
    val fdcMovAvgIndicator: DataStream[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] = indicatorDataStream
      .keyBy(_.datas.indicatorId)
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcMovAvgProcessFunction)
      .name("FdcMovAvgIndicator")

    val movAvgAdvancedIndicatorCacheData = fdcMovAvgIndicator.getSideOutput(advancedIndicatorCacheDataOutput)


    // 移动加权平均数
    val fdcEWMAIndicator: DataStream[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] = indicatorDataStream
      .keyBy(_.datas.indicatorId)
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcEWMAProcessFunction)
      .name("FdcEWMAIndicator")

    // 求和的平均值
    val fdcAvgIndicator: DataStream[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] = indicatorDataStream
      .keyBy(_.datas.indicatorId)
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcAvgProcessFunction)
      .name("FdcAvgIndicator")

    val avgAdvancedIndicatorCacheData = fdcAvgIndicator.getSideOutput(advancedIndicatorCacheDataOutput)

    // Drift(Percent)计算逻辑
    val fdcDriftPercentIndicator: DataStream[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] = indicatorDataStream
      .keyBy(_.datas.indicatorId)
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcDriftPercentProcessFunction)
      .name("FdcDriftPercentIndicator")

    val driftPercentAdvancedIndicatorCacheData = fdcDriftPercentIndicator.getSideOutput(advancedIndicatorCacheDataOutput)

    val fdcAbsIndicator: DataStream[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] = indicatorDataStream
      .keyBy(_.datas.indicatorId)
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcAbsProcessFunction)
      .name("FdcAbsIndicator")
    
    val indicatorSink = fdcDriftIndicator.union(fdcMovAvgIndicator, fdcEWMAIndicator, fdcAvgIndicator, fdcDriftPercentIndicator,
      fdcAbsIndicator).flatMap(indicator => indicator).rebalance

    writeKafka(indicatorSink)

    // todo sink to mainfab_indicator_result_topic
    val advancedIndicatorCacheData = driftAdvancedIndicatorCacheData.union(movAvgAdvancedIndicatorCacheData,avgAdvancedIndicatorCacheData,driftPercentAdvancedIndicatorCacheData)

    advancedIndicatorCacheData.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC, (element: JsonNode) => {
        element.findPath("baseIndicatorId").asText()
      })
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("advancedIndicatorCacheData sink to kafka")
      .uid("advancedIndicatorCacheData sink to kafka")

  }


  /**
   * 获取Indicator数据
   */
  override protected def getDatas(): DataStream[FdcData[IndicatorResult]] = {

    getDao().getKafkaSource[FdcData[IndicatorResult]](
      ProjectConfig.KAFKA_DRIFT_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_DRIFT_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_DRIFT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_DRIFT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
  }

  /**
   * 获取Indicator配置数据
   */
  def getIndicatorConfig(): DataStream[ConfigData[IndicatorConfig]] = {

    getDao().getKafkaSource[ConfigData[IndicatorConfig]](
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_DRIFT_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_DRIFT_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_DRIFT_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
  }
}

