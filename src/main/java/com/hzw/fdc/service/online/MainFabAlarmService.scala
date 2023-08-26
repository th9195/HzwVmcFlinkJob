package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabAlarmDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabAlarm._
import com.hzw.fdc.function.online.MainFabIndicator.IndicatorCommFunction
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, fromJson, toBean}
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.datastream.BroadcastStream



/**
 * @author gdj
 * @create 2020-06-15-19:53
 *
 */
class MainFabAlarmService extends TService with IndicatorCommFunction {
  private val dao = new MainFabAlarmDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAlarmService])


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

//    filterAlarmDataDS.print("--------------------------------")
//    lazy val outputTag = new OutputTag[JsonNode]("alarm_config_output") {}

//    val EWMA_config = new MapStateDescriptor[String, JsonNode](
//      "EWMA_config",
//      //Key类型
//      BasicTypeInfo.STRING_TYPE_INFO,
//      //Value类型
//      TypeInformation.of(new TypeHint[JsonNode] {}))
//
//    val indicatorResultStream = filterAlarmDataDS
//      .keyBy(
//        data => {
//          val toolName = data.findPath(MainFabConstants.toolName).asText()
//          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
//          s"$toolName|$chamberName"
//        }
//      )
//      .connect(ConfigOutputStream.broadcast(EWMA_config))
//      .process(new EWMAConfigProcessFunction())
//      .name("EWMAConfigProcess")

//    val alarmConfigStream = indicatorResultStream.getSideOutput(outputTag)

    //alarm配置数据 维表流
    val alarm_config = new MapStateDescriptor[String, JsonNode](
      "alarm_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    //广播配置流
    val alarmConfigBroadcastStream: BroadcastStream[JsonNode] = ConfigOutputStream.broadcast(alarm_config)


    //数据流和广播流connect
    val alarmAndLimitOutputStream: DataStream[(String, String)] = filterAlarmDataDS
      .keyBy(
        data => {
          val indicatorId = data.findPath(MainFabConstants.indicatorId).asText()
          s"${indicatorId}"
        }
      )
      .connect(alarmConfigBroadcastStream)
      .process(new IndicatorAlarmProcessFunctionNew())
      .name("generateAlarm")
      .uid("generateAlarm")


    val alarmOutputStream:DataStream[(AlarmRuleResult,IndicatorLimitResult)]=alarmAndLimitOutputStream.map(x=>{
      val alarmRuleResult: AlarmRuleResult = fromJson[AlarmRuleResult](x._1)
      val indicatorLimitResult = fromJson[IndicatorLimitResult](x._2)

      val resAlarmRuleResult: AlarmRuleResult = alarmRuleResult.copy(
        unit = indicatorLimitResult.unit,
        switchStatus = indicatorLimitResult.switchStatus,
        alarmLevel = indicatorLimitResult.alarmLevel,
        oocLevel = indicatorLimitResult.oocLevel,
        limit = indicatorLimitResult.limit
      )

      (resAlarmRuleResult, indicatorLimitResult)
    })
    // LinearFit indicator实时计算逻辑
    getLinearFitFunction(alarmOutputStream, filterAlarmDataDS)


    val micStream: DataStream[MicAlarmData] = alarmOutputStream
      .keyBy(key => key._2.toolName + "|" + key._2.chamberName)
      .connect(alarmConfigBroadcastStream)
      .process(new FdcMicProcessFunction())
      .filter(f=>f.dataVersion == ProjectConfig.JOB_VERSION)
      .map(x => {
        MicAlarmData("micAlarm", x)
      })
      .name("micStream")


    // 写入 alarm kafka
    val alarmLevelOutputStream: DataStream[AlarmRuleResult] = alarmOutputStream
      .map(_._1)

    val kafkaSink = alarmLevelOutputStream.map(x => {
      FdcData[AlarmRuleResult]("AlarmLevelRule", x)
    })

    //读window配置
    val windowConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_WINDOW_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_ALARM_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_ALARM_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID)

    val indicatorConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)

    val alarmConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_ALARM_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_ALARM_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID)

    val ConfigDataStream = alarmConfigDataStream.union(windowConfigDataStream, indicatorConfigDataStream)

    //indicator配置数据 维表流
    val indicatorConfig = new MapStateDescriptor[String, JsonNode](
      "run_alarm_indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    //广播配置流
    val indicatorConfigBroadcastStream = ConfigDataStream.broadcast(indicatorConfig)


    val runAlarmStream =  kafkaSink.map(beanToJsonNode[FdcData[AlarmRuleResult]](_))
      .union(micStream.map(beanToJsonNode[MicAlarmData](_)))

    // 按run维度聚合ocap告警发邮件
    val runAlarmSink:DataStream[FdcData[AlarmLevelRule]] = runAlarmStream.keyBy(data => {
      val runId = data.findPath("runId").asText()
      s"$runId"
    })
      .connect(indicatorConfigBroadcastStream)
      .process(new MainFabRunAlarmBroadcastProcessFunction())
      .filter(f=>f.dataVersion == ProjectConfig.JOB_VERSION)   // 过滤0 down time不对的版本
      .map(FdcData[AlarmLevelRule]("AlarmRule", _))

    runAlarmSink.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_ALARM_TOPIC,
      new FdcKafkaSchema[FdcData[AlarmLevelRule]](ProjectConfig.KAFKA_ALARM_TOPIC, x => s"${x.datas.toolName}|${x.datas.chamberName}"
      )
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka alarm rule Sink")
      .uid("AlarmJob_rule_sink")


    kafkaSink.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[AlarmRuleResult]](ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC, x => s"${x.datas.toolName}|${x.datas.indicatorId}"
      )
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink")
      .uid("AlarmJob_sink")



    //获取侧输出流
    val alarmSwitchEventStream = alarmAndLimitOutputStream.getSideOutput(AlarmSwitchEventOutput)
    alarmSwitchEventStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_ALARM_SWITCH_ACK_TOPIC,
      new FdcKafkaSchema[scala.collection.immutable.List[AlarmSwitchEventConfig]](ProjectConfig.KAFKA_ALARM_SWITCH_ACK_TOPIC, x => x.head.toolName),
      ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("switch event Sink")
      .uid("switch_event_sink")


    // todo 侧道输出 AlarmEwmaCacheData
    val alarmEwmaCacheStream: DataStream[JsonNode] = alarmAndLimitOutputStream.getSideOutput(ewmaCacheOutput)

    // todo sink ewmaCache to kafka
    alarmEwmaCacheStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC, (element: JsonNode) => {
        element.findPath("ewmaKey").asText()
      })
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("AlarmEwmaCache Kafka Sink")
      .uid("AlarmJob_AlarmEwmaCache_sink")

    // todo 侧道输出 ewmaRetargetResult
    val ewmaRetargetResultStream: DataStream[JsonNode] = alarmAndLimitOutputStream.getSideOutput(retargetResultOutput)

    // todo sink ewmaRetargetResult to kafka
    ewmaRetargetResultStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_RETARGET_DATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_RETARGET_DATA_TOPIC, (element: JsonNode) => {
        element.findPath("indicatorId").asText()
      })
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("ewmaRetargetResult Kafka Sink")
      .uid("AlarmJob_ewmaRetargetResult_sink")


    micStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[MicAlarmData](ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC, (element: MicAlarmData) => element.datas.toolName)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Mic Kafka Sink")
      .uid("AlarmJob_mic_sink")
  }


  /**
   * LinearFit 实时计算逻辑
   * @param alarmOutputStream
   * @param filterAlarmDataDS
   */
  def getLinearFitFunction(alarmOutputStream: DataStream[(AlarmRuleResult, IndicatorLimitResult)],
                           filterAlarmDataDS: DataStream[JsonNode]): Unit = {



    //indicator配置数据 维表流
    val indicator_config = new MapStateDescriptor[String, ConfigData[IndicatorConfig]](
      "LinearFit_indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[ConfigData[IndicatorConfig]] {}))

    //广播配置流
    val indicatorConfigBroadcastStream = getIndicatorConfig().broadcast(indicator_config)

    val LinearFitStatusStream = alarmOutputStream.filter(x => {
      x._1.algoClass == "linearFit" && x._2.oocLevel != 0
    })
      .map(x => {
        val bean= FdcData("ClearLinearFit", clearLinearFit(algoClass = x._1.algoClass, indicatorId = x._1.indicatorId))
        val node = beanToJsonNode(bean)
        node
      })
      .union(filterAlarmDataDS)


    // LinearFit indicator实时计算逻辑
    val linearFitIndicatorResultList = LinearFitStatusStream
      .keyBy(_.findPath(MainFabConstants.indicatorId).asText())
      .connect(indicatorConfigBroadcastStream)
      .process(new FdcLinearFitIndicatorProcessFunction)

    val indicatorResultStream = linearFitIndicatorResultList.flatMap(indicator => indicator)

    // 写入kafka
    writeKafka(indicatorResultStream)

    // 侧道输出 LinearFitAdvancedIndicatorCacheData
    val linearFitAdvancedIndicatorCacheData = linearFitIndicatorResultList.getSideOutput(advancedIndicatorCacheDataOutput)

    linearFitAdvancedIndicatorCacheData.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_REDIS_DATA_TOPIC, (element: JsonNode) => {
        element.findPath("baseIndicatorId").asText()
      })
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("linearFitAdvancedIndicatorCacheData sink to kafka")
      .uid("linearFitAdvancedIndicatorCacheData sink to kafka")

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


  /**
   * 获取key
   */
  def geneAlarmKey(toolid: String, chamberid: String, indicatorid: String): String = {
    toolid + "#" + chamberid + "#" + indicatorid
  }

  /**
   * 判断字符串为空
   */
  def hasLength(str: String): Boolean = {
    str != null && str != ""
  }
}
