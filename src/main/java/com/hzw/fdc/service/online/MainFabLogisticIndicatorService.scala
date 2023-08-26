package com.hzw.fdc.service.online

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabLogisticIndicatorDao
import com.hzw.fdc.function.online.MainFabLogisticIndicator.{MainFabCollectLogisticIndicatorProcessFunction, MainFabLogisticIndicatorProcessFunction}
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.scalabean.{ConfigData, FdcData, IndicatorConfig, IndicatorResult}
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
class MainFabLogisticIndicatorService extends TService{
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
    val indicatorDataStream: DataStream[FdcData[IndicatorResult]] = getDatas()


    //读indicator配置
    val configDataStream = getDao().getKafkaSource[ConfigData[IndicatorConfig]](
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_LOGISTIC_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)


    // 维表流
    val indicator_config = new MapStateDescriptor[String, ConfigData[IndicatorConfig]](
      "indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[ConfigData[IndicatorConfig]] {}))


    val indicatorConfigBroadcastStream = configDataStream.broadcast(indicator_config)


    val logisticIndicatorDataStream:DataStream[(IndicatorConfig, IndicatorResult)]=
      indicatorDataStream
      .keyBy(_.datas.runId)
      .connect(indicatorConfigBroadcastStream)
      .process(new MainFabLogisticIndicatorProcessFunction)
      .name("MainFabLogisticIndicatorProcessFunction")


    val logisticIndicatorResultDataStream=logisticIndicatorDataStream
      .keyBy(x=>x._1.indicatorId)
      .process(new MainFabCollectLogisticIndicatorProcessFunction)
      .name("MainFabCollectLogisticIndicatorProcessFunction")




    writeKafka(logisticIndicatorResultDataStream)


  }


  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[FdcData[IndicatorResult]] = {

    getDao().getKafkaSource[FdcData[IndicatorResult]](
      ProjectConfig.KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_LOGISTIC_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID)

  }


  /**
   *  写入kafka
   */
  def writeKafka(indicatorOutputStream: DataStream[(FdcData[IndicatorResult], Boolean, Boolean, Boolean)]): Unit ={
    val indicatorJson: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream.map(i => {
      i._1
    })

    val driftIndicatorStream: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream
      .filter(_._2 ==true).map(i => {
      i._1
    })


    val calculatedIndicatorStream: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream
      .filter(_._3 ==true).map(i => {
      i._1
    })


    val logisticIndicatorStream: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream
      .filter(_._4 ==true).map(i => {
      i._1
    })


    indicatorJson.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC, element=> element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To Indicator").uid("IndicatorJob_sink")

    driftIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_DRIFT_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_DRIFT_INDICATOR_TOPIC, element => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To driftIndicatorStream").uid("IndicatorJob_drift_sink")

    calculatedIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_CALCULATED_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_CALCULATED_INDICATOR_TOPIC, element => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To calculatedIndicatorStream").uid("IndicatorJob_calculated_sink")

    logisticIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC, element => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaLogisticIndicatorSink)
      .uid(MainFabConstants.KafkaLogisticIndicatorSink)
  }




}


