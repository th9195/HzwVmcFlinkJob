package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.{MainFabWindowDao, MainFabWindowEndWindowDao}
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabWindow.{MainFabWindowConfigSplitKeyedBroadcastProcessFunction, MainFabWindowEndKeyedBroadcastProcessFunction, ParseWindowProcessFunction}
import com.hzw.fdc.scalabean.{FdcData, IndicatorResult, MainFabLogInfo, fdcWindowData}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Package com.hzw.fdc.service.online
  * @author wanghb
  * @date 2022-11-12 14:42
  * @desc
  * @version V1.0
  */
class MainFabWindowEndWindowService extends TService{
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowEndWindowService])

  lazy val WindowEndOutput = new OutputTag[(String, JsonNode, JsonNode)]("WindowEnd")
  lazy val debugOutput = new OutputTag[String]("debugTest")
  lazy val cycleCountDataOutput = new OutputTag[FdcData[IndicatorResult]]("cycleCountData")
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")

  private val Dao = new MainFabWindowEndWindowDao

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
      MainFabConstants.MAIN_FAB_WINDOWEND_WINDOW_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)

    val windowConfigDataStream = getDimDatas(ProjectConfig.KAFKA_WINDOW_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_WINDOWEND_WINDOW_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID)

    val indicatorConfigDataStream = getDimDatas(ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_WINDOWEND_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)

    val debugConfigStream = getDimDatas(ProjectConfig.KAFKA_DEBUG_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_WINDOWEND_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID)

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

    // todo 3- 拆分window配置
    val windowSplitStream: DataStream[(String, JsonNode, JsonNode)] = sourceDataStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .connect(dimConfigBroadcastDataStream)
      .process(new MainFabWindowConfigSplitKeyedBroadcastProcessFunction)
      .name("WindowConfigSplit")
      .uid("WindowConfigSplit")

    val WindowEndSensorAliasStream = windowSplitStream.getSideOutput(WindowEndOutput)
    val mainFabLogInfo_01 = windowSplitStream.getSideOutput(mainFabLogInfoOutput)

    // todo 4- window end计算
    val windowEndOutPutDataStream = WindowEndSensorAliasStream
      .keyBy(_._1)
      .connect(dimConfigBroadcastDataStream)
      .process(new MainFabWindowEndKeyedBroadcastProcessFunction)
      .uid("windowEnd")
      .name("windowEnd")

    val windowEndDebugTestStream = windowEndOutPutDataStream.getSideOutput(debugOutput)
    val cycleCountDataStream: DataStream[FdcData[IndicatorResult]] = windowEndOutPutDataStream.getSideOutput(cycleCountDataOutput)
    val mainFabLogInfo_02 = windowEndOutPutDataStream.getSideOutput(mainFabLogInfoOutput)

    // todo 5- debug log 写入kafka
    windowEndDebugTestStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_DEBUG_RESULT_LOG_TOPIC,
      new FdcKafkaSchema[String](ProjectConfig.KAFKA_DEBUG_RESULT_LOG_TOPIC
        //按照tool分区
        , (e: String) => ""
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("windowEnd Window debugTest")
      .uid("windowEnd Window debugTest")

    // todo 6- cycleCount 写入 kafka  indicator topic (此处功能等dev1.4测试完成后同步更新到这个版本)
    cycleCountDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC
        //按照tool分区
        , (e: FdcData[IndicatorResult]) =>
          e.datas.toolName + e.datas.chamberName
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("windowEnd Window cycleCount")
      .uid("windowEnd Window cycleCount")

    // todo 7- windowEnd window 结果数据 sink 到 kafka
    windowEndOutPutDataStream
      .process(new ParseWindowProcessFunction())
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
        new FdcKafkaSchema[fdcWindowData](ProjectConfig.KAFKA_WINDOW_DATA_TOPIC
          //按照tool分区
          , (e: fdcWindowData) => {
            if(e.datas.windowDatasList.nonEmpty){
              s"${e.datas.toolName}|${e.datas.chamberName}|${e.datas.windowDatasList.head.sensorAlias}"
            }else{
              s"${e.datas.toolName}|${e.datas.chamberName}|"
            }
          }
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("windowEnd Window sink fdcWindowData to kafka")
      //添加uid用于监控
      .uid("windowEnd Window sink fdcWindowData to kafka")

    // todo 8 日志信息写入kafka
    val mainFabLogInfos: DataStream[MainFabLogInfo] = mainFabLogInfo_01.union(mainFabLogInfo_02)
    mainFabLogInfos.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC,
      new FdcKafkaSchema[MainFabLogInfo](ProjectConfig.KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC
        // 按照tool + chamber分区
        , (mainFabLogInfo: MainFabLogInfo) => mainFabLogInfo.mainFabDebugCode
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("WindowEndWindowJob sink MainFabLogInfo to kafka")
      .uid("WindowEndWindowJob sink MainFabLogInfo to kafka")

  }

  /**
    * 读取源数据:  M1 M2 M3 处理后的实时数据;
    */
  override protected def getDatas(): DataStream[JsonNode] = {

    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_WINDOWEND_JOB_RAWDATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_WINDOWEND_JOB_RAWDATA_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_JOB_FROM_TIMESTAMP)
    }else{
      getDao.getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_WINDOWEND_JOB_RAWDATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_WINDOWEND_JOB_RAWDATA_KAFKA_SOURCE_UID)
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
        ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB,
        MainFabConstants.latest,
        name_uid,
        name_uid,
        ProjectConfig.KAFKA_MAINFAB_WINDOWEND_WINDOW_JOB_FROM_TIMESTAMP)
    }else{
      getDao().getKafkaJsonSource(
        topic,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB,
        MainFabConstants.latest,
        name_uid,
        name_uid)
    }
  }
}
