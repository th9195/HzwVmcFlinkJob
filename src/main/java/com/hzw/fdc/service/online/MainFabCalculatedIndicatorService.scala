package com.hzw.fdc.service.online

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabCalculatedIndicatorDao
import com.hzw.fdc.function.online.MainFabIndicator.{FdcCalculatedProcessFunction, IndicatorCommFunction}
import com.hzw.fdc.scalabean.{ConfigData, FdcData, IndicatorConfig, IndicatorResult}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 * @author gdj
 *  2020-09-07-11:48
 *
 */
class MainFabCalculatedIndicatorService extends TService with IndicatorCommFunction {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabCalculatedIndicatorService])

  /**
   * 获取
   */
  override def getDao(): TDao = new MainFabCalculatedIndicatorDao

  /**
   * 分析
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


    val fdcindicatorDataStream  = indicatorDataStream
      .keyBy(elem => {
        s"${elem.datas.runId}|${elem.datas.controlPlanId}"
      })
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcCalculatedProcessFunction)
      .name("FdcCalculatedIndicator")

    val indicatorsink: DataStream[(IndicatorResult, Boolean, Boolean, Boolean)] = fdcindicatorDataStream
      .flatMap(indicator => indicator)

    writeKafka(indicatorsink)
  }

  /**
   * 获取Indicator数据
   */
  override protected def getDatas(): DataStream[FdcData[IndicatorResult]] = {

    getDao().getKafkaSource[FdcData[IndicatorResult]](
      ProjectConfig.KAFKA_CALCULATED_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_CALCULATED_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CALCULATED_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CALCULATED_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
  }

  /**
   * 获取Indicator配置数据
   */
  def getIndicatorConfig(): DataStream[ConfigData[IndicatorConfig]] = {

    getDao().getKafkaSource[ConfigData[IndicatorConfig]](
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_CALCULATED_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CALCULATED_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CALCULATED_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
  }
}

