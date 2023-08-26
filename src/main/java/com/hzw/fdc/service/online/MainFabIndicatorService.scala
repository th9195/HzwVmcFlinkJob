package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabIndicatorDao
import com.hzw.fdc.function.online.MainFabIndicator.{FdcIndicatorConfigBroadcastProcessFunction, FdcIndicatorProcessFunction, IndicatorAlgorithm, IndicatorCommFunction}
import com.hzw.fdc.scalabean.ALGO.ALGO
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-06-28-18:26
 *
 */
class MainFabIndicatorService extends TService with IndicatorCommFunction {


  lazy val RawDataOutput = new OutputTag[FdcData[IndicatorResult]]("fdcWindowDatas")
  lazy val configOutput = new OutputTag[FdcData[IndicatorResult]]("config")

  private val dao = new MainFabIndicatorDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabIndicatorService])

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {

    // 数据流
    val KafkaRawDataStream: DataStream[JsonNode] = getDatas()

    // 维表流
    val indicator_config = new MapStateDescriptor[String, JsonNode](
      "indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))


    val indicatorConfigBroadcastStream: BroadcastStream[JsonNode] = getIndicatorConfig().broadcast(indicator_config)

    /**
     *  组装配置和rawDataList
     */
    val outputStream: DataStream[(ListBuffer[(ALGO, IndicatorConfig)], ListBuffer[RawData])] = KafkaRawDataStream
      .process(new ProcessFunction[JsonNode,Option[fdcWindowData]]{
          override def processElement(value: JsonNode, ctx: ProcessFunction[JsonNode, Option[fdcWindowData]]#Context, out: Collector[Option[fdcWindowData]]) = {
            val res = FdcIndicatorProcessFunction.parseWindow(value)
            if(res != null){
              try {
                out.collect(Option(res))
              } catch {
                case e: Exception => logger.warn(s"input data --> $value  ExceptionInfo:${ExceptionInfo.getExceptionInfo(e)} ")
              }
            }
          }
        }
      )
      .filter(_.nonEmpty)
      .map(_.get)
      .keyBy(data => {
          if(data.datas.windowDatasList.nonEmpty){
            s"${data.datas.toolName}|${data.datas.chamberName}|${data.datas.windowDatasList.head.sensorAlias}"
          }else{
            s"${data.datas.toolName}|${data.datas.chamberName}|"
          }
        }
      )
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcIndicatorConfigBroadcastProcessFunction)
      .name("FdcGenerateIndicator")
      .uid("FdcGenerateIndicator")

    /**
     *  基础的一些算法计算，比如avg，sum，slop
     */
    val indicatorOutputStream: DataStream[(IndicatorResult, Boolean, Boolean, Boolean)] = outputStream
      .flatMap(datas =>{
        val flatMapData = new ListBuffer[(ALGO, IndicatorConfig, ListBuffer[RawData])]
        for (data1<- datas._1) {
          flatMapData.append((data1._1, data1._2, datas._2))
        }
        flatMapData
      })
      .map(elem => {
        val res = FdcIndicatorProcessFunction.FdcIndicatorMath(elem)
        if(res == null){
          None
        }else {
          Try(res)
        }
      })
      .filter(_.nonEmpty)
      .map(x => {parseOption(x)})
      .name("indicator math")
      .uid("indicator math")

    /**
     * 写入kafka sink
     */
    writeKafka(indicatorOutputStream)
  }


  /**
   * 获取fdcWindowData数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_INDICATOR_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_INDICATOR_JOB_FROM_TIMESTAMP)
    }else {
      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_INDICATOR_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID)
    }
  }

  /**
   * 获取Indicator配置数据
   */
  def getIndicatorConfig(): DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
  }
}
