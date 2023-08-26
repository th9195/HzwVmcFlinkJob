package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabProcessEndWindowDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabWindow._
import com.hzw.fdc.scalabean.{FdcData, IndicatorResult, MainFabLogInfo, fdcWindowData}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

/**
 * MainFabProcessEndWindowService
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/5 11:21
 * @since 1.0.0
 * @update 2022/11/5 11:21
 * */
class MainFabProcessEndWindowService extends TService{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabProcessEndWindowService])

  lazy val dataMissingRatioOutput = new OutputTag[FdcData[IndicatorResult]]("dataMissingRatio")
  lazy val MESOutput = new OutputTag[JsonNode]("toolMessage")
  lazy val debugOutput = new OutputTag[String]("debugTest")
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")

  private val Dao = new MainFabProcessEndWindowDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = Dao

  /**
   * 处理数据
   *
   * @return
   */
  override def analyses(): Any = {

    // todo 1- 读取源数据:  M1 M2 M3 处理后的数据;
    val sourceDataStream = getDatas()

    // todo 2- 读取维表数据
    val contextConfigDataStream = getDimDatas(ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)

    val windowConfigDataStream = getDimDatas(ProjectConfig.KAFKA_WINDOW_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID)

    val indicatorConfigDataStream = getDimDatas(ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)

    val debugConfigStream = getDimDatas(ProjectConfig.KAFKA_DEBUG_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID)

    // 合并 维表数据;
    val dimConfigDataStream = contextConfigDataStream.union(windowConfigDataStream, indicatorConfigDataStream,
      debugConfigStream)

    // 维表数据注册广播变量
    val config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val dimConfigBroadcastDataStream = dimConfigDataStream.broadcast(config)

    // todo 3- 源数据简单清洗 转换 过滤
    val splitWindowDataStream:DataStream[(String, JsonNode, JsonNode)] = sourceDataStream.filter(inputValue =>{
      val dataType = inputValue.get(MainFabConstants.dataType).asText()
      val traceId = inputValue.get(MainFabConstants.traceId).asText()
      if( !StringUtils.isEmpty(traceId) && (dataType == MainFabConstants.eventStart ||
        dataType == MainFabConstants.rawData ||
        dataType == MainFabConstants.eventEnd)){
        true
      }else{
        false
      }
    }).keyBy(elem => {
      elem.findPath(MainFabConstants.traceId).asText()
    }).connect(dimConfigBroadcastDataStream)
      .process(new MainFabProcessEndConfigSplitKeyedBroadcastProcessFunction)
      .name("processEndWindow WindowConfigSplit")
      .uid("processEndWindow WindowConfigSplit")

    val mainFabLogInfo_01 = splitWindowDataStream.getSideOutput(mainFabLogInfoOutput)


    // todo 4- processEnd 划分窗口
    val processEndOutPutDataStream: DataStream[fdcWindowData] = splitWindowDataStream
      .keyBy(_._1)
      .connect(dimConfigBroadcastDataStream)
      .process(new MainFabProcessEndKeyedBroadcastProcessFunction)
      .uid("processEnd calc window")
      .name("processEnd calc window")

    val mainFabLogInfo_02 = processEndOutPutDataStream.getSideOutput(mainFabLogInfoOutput)


    // todo 5- debug log 写入kafka
    val processEndDebugTestStream = processEndOutPutDataStream.getSideOutput(debugOutput)
    processEndDebugTestStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_DEBUG_RESULT_LOG_TOPIC,
      new FdcKafkaSchema[String](ProjectConfig.KAFKA_DEBUG_RESULT_LOG_TOPIC
        // 随机分区
        , (e: String) => Random.nextInt(1000).toString
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("processEnd Window debugTest")
      .uid("processEnd Window debugTest")

    // todo 6- dataMissingRatioIndicator 和 cycleCount 写入 kafka  indicator topic
    val dataMissingRatioStream: DataStream[FdcData[IndicatorResult]] = processEndOutPutDataStream.getSideOutput(dataMissingRatioOutput)
    dataMissingRatioStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
        elem => {
          elem.datas.runId
        }
        //按照tool分区
//         (e: FdcData[IndicatorResult]) =>
//          e.datas.toolName + e.datas.chamberName
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("processEnd Window sink dataMissingRatio to kafka")
      .uid("processEnd Window sink dataMissingRatio to kafka")


    // todo 7- processEnd window 结果数据 sink 到 kafka
    processEndOutPutDataStream
      .process(new ParseWindowProcessFunction())
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
        new FdcKafkaSchema[fdcWindowData](ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
          elem => {
            if(elem.datas.windowDatasList.nonEmpty){
              s"${elem.datas.runId}|${elem.datas.controlWindowId}|${elem.datas.windowDatasList.head.sensorAlias}"
            }else{
              s"${elem.datas.runId}|${elem.datas.controlWindowId}"
            }
          }
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("processEnd Window sink fdcWindowData to kafka")
      //添加uid用于监控
      .uid("processEnd Window sink fdcWindowData to kafka")


    // todo 8 日志信息写入kafka
    val mainFabLogInfos: DataStream[MainFabLogInfo] = mainFabLogInfo_01.union(mainFabLogInfo_02)
    mainFabLogInfos.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC,
      new FdcKafkaSchema[MainFabLogInfo](ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC
        , (mainFabLogInfo: MainFabLogInfo) => mainFabLogInfo.mainFabDebugCode
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("ProcessendWindowJob sink MainFabLogInfo to kafka")
      .uid("ProcessendWindowJob sink MainFabLogInfo to kafka")
  }

  /**
   * 读取源数据:  M1 M2 M3 处理后的实时数据;
   */
  override protected def getDatas(): DataStream[JsonNode] = {

    logger.warn(s"${ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP}")

    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_PROCESSEND_JOB_RAWDATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_PROCESSEND_JOB_RAWDATA_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_JOB_FROM_TIMESTAMP)
    }else{
      getDao.getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_PROCESSEND_JOB_RAWDATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_PROCESSEND_JOB_RAWDATA_KAFKA_SOURCE_UID)
    }

  }


  /**
   * 根据topic + 时间戳 获取实时的维表数据
   * @param topic
   * @return
   */
  def getDimDatas(topic:String,name_uid:String):DataStream[JsonNode] = {

    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao().getKafkaJsonSourceByTimestamp(
        topic,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB,
        MainFabConstants.latest,
        name_uid,
        name_uid,
        ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_JOB_FROM_TIMESTAMP)
    }else{
      getDao().getKafkaJsonSource(
        topic,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB,
        MainFabConstants.latest,
        name_uid,
        name_uid)
    }
  }
}
