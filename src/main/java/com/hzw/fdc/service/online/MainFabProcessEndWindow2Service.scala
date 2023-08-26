package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabProcessEndWindow2Dao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabWindow2.{MainFabMatchControlPlanAndWindowsBroadcastProcessFunction, MainFabProcessEndCalcWindowBroadcastProcessFunction}
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{FdcData, IndicatorResult, MainFabDebugInfo, MainFabLogInfo, fdcWindowData}
import org.apache.flink.streaming.api.scala._
import com.hzw.fdc.util.{ProjectConfig, MainFabConstants}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
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
class MainFabProcessEndWindow2Service extends TService{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabProcessEndWindow2Service])


  lazy val specialIndicatorOutput = new OutputTag[FdcData[IndicatorResult]]("specialIndicatorOutput")
  lazy val debugOutput = new OutputTag[String]("debugOutput")
  lazy val matchControlPlanAndWindowGiveUpRunStreamOutput = new OutputTag[JsonNode]("matchControlPlanAndWindowGiveUpRunStream")
  lazy val calcWindowGiveUpRunStreamOutput = new OutputTag[JsonNode]("calcWindowGiveUpRunStream")
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  lazy val mainFabDebugInfoOutput = new OutputTag[MainFabDebugInfo]("mainFabDebugInfo")

  private val Dao = new MainFabProcessEndWindow2Dao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = Dao


  /**
   * 注册广播变量
   * @param dimConfigDataStream
   * @return
   */
  def generaterBroadcastDataStream(dimConfigDataStream: DataStream[JsonNode]): BroadcastStream[JsonNode] = {
    val config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val broadcastDataStream: BroadcastStream[JsonNode] = dimConfigDataStream.broadcast(config)
    broadcastDataStream
  }

  /**
   * 处理数据
   *
   * @return
   */
  override def analyses(): Any = {

    // todo 读取源数据;
    val sourceDataStream = getDatas()

    // todo 读取维表数据
    val controlPlanConfigDataStream = getDimDatas(ProjectConfig.KAFKA_MAINFAB_CONTROLPLAN_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID,
      ProjectConfig.MAINFAB_PARTITION_DEFAULT)

    val windowConfigDataStream = getDimDatas(ProjectConfig.KAFKA_MAINFAB_WINDOW2_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID,
      ProjectConfig.MAINFAB_PARTITION_DEFAULT)

    val indicatorConfigDataStream = getDimDatas(ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      ProjectConfig.MAINFAB_PARTITION_DEFAULT)

    val debugConfigStream = getDimDatas(ProjectConfig.KAFKA_DEBUG_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID)

    // 合并 维表数据  维表数据注册广播变量  -- 匹配controlPlan 和 window;
    val matchControlPlanAndWindowDimConfigDataStream = controlPlanConfigDataStream.union(windowConfigDataStream,debugConfigStream)
    val matchControlPlanAndWindowDimConfigBroadcastDataStream = generaterBroadcastDataStream(matchControlPlanAndWindowDimConfigDataStream)

    // 合并 维表数据  维表数据注册广播变量  -- 划window;
    val calcWindowDimConfigDataStream = windowConfigDataStream.union(indicatorConfigDataStream,
      debugConfigStream)
    val calcWindowDimConfigBroadcastDataStream = generaterBroadcastDataStream(calcWindowDimConfigDataStream)

    // todo 3- 源数据简单清洗 转换 过滤  匹配controlPlan 计算Window
    val runDataMatchedWindowStream = sourceDataStream.filter(inputValue => {
      val dataType = inputValue.get(MainFabConstants.dataType).asText()
      val traceId = inputValue.get(MainFabConstants.traceId).asText()
      if (!StringUtils.isEmpty(traceId) && (dataType == MainFabConstants.eventStart ||
        dataType == MainFabConstants.rawData ||
        dataType == MainFabConstants.eventEnd)) {
        true
      } else {
        false
      }
    }).setParallelism(ProjectConfig.MAINFAB_SOURCE_WINDOW_RAWDATA_PARTITION)
      .keyBy(inputValue => {
      inputValue.get(MainFabConstants.traceId).asText()
    }).connect(matchControlPlanAndWindowDimConfigBroadcastDataStream)
      .process(new MainFabMatchControlPlanAndWindowsBroadcastProcessFunction(MainFabConstants.windowTypeProcessEnd))
      .setParallelism(ProjectConfig.MAINFAB_PROCESSEND_WINDOW_MATCH_CONTROLPLAN_PARTITION)
      .name("ProcessEnd match CP and windows")
      .uid("ProcessEnd match CP and windows")

    // todo 测流 收集匹配controlPlan 和 window 时丢弃的数据
    val matchControlPlanAndWindowGiveUpRunStream = runDataMatchedWindowStream.getSideOutput(matchControlPlanAndWindowGiveUpRunStreamOutput)
    matchControlPlanAndWindowGiveUpRunStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC
        //按照 traceId 分区
        , (elem: JsonNode) => {
          val traceId = elem.get(MainFabConstants.traceId).asText()
          traceId
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).setParallelism(ProjectConfig.MAINFAB_PARTITION_DEFAULT)
      .name("ProcessEnd matchCP giveUp sink")
      //添加uid用于监控
      .uid("ProcessEnd matchCP giveUp sink")

    // todo 划window
    val processEndWindowResultData = runDataMatchedWindowStream.keyBy(inputValue => {
      val traceId = inputValue.get(MainFabConstants.traceId).asText()
      val matchedControlPlanId = inputValue.get("matchedControlPlanId").asText()
      val matchedWindowId = inputValue.get("matchedWindowId").asText()
      val windowPartitionId = inputValue.get("windowPartitionId").asText()
      s"${traceId}|${matchedControlPlanId}|${matchedWindowId}|${windowPartitionId}"
    }).connect(calcWindowDimConfigBroadcastDataStream)
      .process(new MainFabProcessEndCalcWindowBroadcastProcessFunction())
      .setParallelism(ProjectConfig.MAINFAB_PROCESSEND_WINDOW_CALC_WINDOW_PARTITION)
      .name("ProcessEnd calc window")
      .uid("ProcessEnd calc window")

    // todo 收集cycleCount and dataMissingRatio Indicator 的数据
    val specialIndicator = processEndWindowResultData.getSideOutput(specialIndicatorOutput)
    specialIndicator.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
        elem => {
          elem.datas.runId
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).setParallelism(ProjectConfig.MAINFAB_PARTITION_DEFAULT)
      .name("ProcessEnd specialIndicator sink")
      .uid("ProcessEnd specialIndicator sink")

    // todo 划Window 时丢弃的数据 写入kafka
    val calcWindowGiveUpRunStream: DataStream[JsonNode] = processEndWindowResultData.getSideOutput(calcWindowGiveUpRunStreamOutput)
    calcWindowGiveUpRunStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_CALC_WINDOW_GIVEUP_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_CALC_WINDOW_GIVEUP_TOPIC
        //按照 traceId | matchedControlPlanId | matchedWindowId 分区
        , (elem: JsonNode) => {
          val traceId = elem.get(MainFabConstants.traceId).asText()
          val matchedControlPlanId = elem.get("matchedControlPlanId").asText()
          val matchedWindowId = elem.get("matchedWindowId").asText()
          val windowPartitionId = elem.get("windowPartitionId").asText()
          s"${traceId}|${matchedControlPlanId}|${matchedWindowId}|${windowPartitionId}"
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).setParallelism(ProjectConfig.MAINFAB_PARTITION_DEFAULT)
      .name("ProcessEnd calc window giveUp sink")
      //添加uid用于监控
      .uid("ProcessEnd calc window giveUp sink")


    // todo 正常window结果数据 写入kafka
    processEndWindowResultData.addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_WINDOW_DATA_TOPIC
          //按照tool分区
          , (elem: JsonNode) => {
            val windowDataResult = toBean[fdcWindowData](elem)
            if(windowDataResult.datas.windowDatasList.nonEmpty){
              s"${windowDataResult.datas.toolName}|${windowDataResult.datas.chamberName}|${windowDataResult.datas.windowDatasList.head.sensorAlias}"
            }else{
              s"${windowDataResult.datas.toolName}|${windowDataResult.datas.chamberName}|"
            }
          }
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).setParallelism(ProjectConfig.MAINFAB_PARTITION_DEFAULT)
      .name("ProcessEnd fdcWindowData sink")
      //添加uid用于监控
      .uid("ProcessEnd fdcWindowData sink")

    // todo 日志信息 sink to kafka
    val mainFabLogInfo_calcWindow: DataStream[MainFabLogInfo] = processEndWindowResultData.getSideOutput(mainFabLogInfoOutput)
    val mainFabLogInfo_matchControlPlanAndWindow: DataStream[MainFabLogInfo] = runDataMatchedWindowStream.getSideOutput(mainFabLogInfoOutput)
    val mainFabWindowLogInfos = mainFabLogInfo_calcWindow.union(mainFabLogInfo_matchControlPlanAndWindow)
    mainFabWindowLogInfos.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC,
      new FdcKafkaSchema[MainFabLogInfo](ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC
        , (mainFabLogInfo: MainFabLogInfo) => mainFabLogInfo.mainFabDebugCode
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).setParallelism(ProjectConfig.MAINFAB_PARTITION_DEFAULT)
      .name("ProcessEnd MainFabLogInfo sink")
      .uid("ProcessEnd MainFabLogInfo sink")


    // todo Debug 调试日志输出
    val mainFabDebugInfo_match = runDataMatchedWindowStream.getSideOutput(mainFabDebugInfoOutput)
    val mainFabDebugInfo_calcWindow = processEndWindowResultData.getSideOutput(mainFabDebugInfoOutput)
    mainFabDebugInfo_match.union(mainFabDebugInfo_calcWindow).addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC,
      new FdcKafkaSchema[MainFabDebugInfo](ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC
        //按照 traceId 分区
        , (elem: MainFabDebugInfo) => {
          Random.nextInt(1000).toString
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).setParallelism(ProjectConfig.MAINFAB_PARTITION_DEFAULT)
      .name("ProcessEnd DebugInfo sink")
      //添加uid用于监控
      .uid("ProcessEnd DebugInfo sink")
  }

  /**
   * 读取源数据:  M1 M2 M3 处理后的实时数据;
   */
  override protected def getDatas(): DataStream[JsonNode] = {

    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourcePartitionByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB,
        MainFabConstants.latest,
        ProjectConfig.MAINFAB_SOURCE_WINDOW_RAWDATA_PARTITION,
        MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_RAWDATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_RAWDATA_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW2_JOB_FROM_TIMESTAMP)
    }else{
      getDao.getKafkaJsonSourcePartition(
        ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB,
        MainFabConstants.latest,
        ProjectConfig.MAINFAB_SOURCE_WINDOW_RAWDATA_PARTITION,
        MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_RAWDATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_PROCESSEND_WINDOW2_JOB_RAWDATA_KAFKA_SOURCE_UID)
    }

  }


  /**
   * 根据topic + 时间戳 获取实时的维表数据
   * @param topic
   * @return
   */
  def getDimDatas(topic:String,name_uid:String,partition_num:Int=1):DataStream[JsonNode] = {

    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao().getKafkaJsonSourcePartitionByTimestamp(
        topic,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB,
        MainFabConstants.latest,
        partition_num,
        name_uid,
        name_uid,
        ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW2_JOB_FROM_TIMESTAMP)
    }else{
      getDao().getKafkaJsonSourcePartition(
        topic,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB,
        MainFabConstants.latest,
        partition_num,
        name_uid,
        name_uid)
    }
  }
}
