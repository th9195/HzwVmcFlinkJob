package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabAutoLimitDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabAutoLimit._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration


/**
 * @author ：gdj
 * @date ：Created in 2021/6/8 15:53
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitService extends TService {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitService])

  lazy val outputTagByCount = new OutputTag[(AutoLimitOneConfig, IndicatorResult)]("ByCount")
  lazy val outputAutoLimitTask = new OutputTag[AutoLimitTask]("AutoLimitTask")
  private val Dao = new MainFabAutoLimitDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = Dao


  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {

    //Indicator 数据
    val kafkaIndicatorDataStream = getDatas()

    // 维表流
    val autoLimit_config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.autoLimit_config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val autoLimitConfigBroadcastStream: BroadcastStream[JsonNode] = ConfigOutputStream().union(getAlarmConfig).broadcast(autoLimit_config)


    val autoLimitByTimeDataStream: DataStream[(AutoLimitOneConfig, IndicatorResult)] = kafkaIndicatorDataStream
      .keyBy(_.findPath("indicatorId").asText())
      .connect(autoLimitConfigBroadcastStream)
      .process(new MainFabAutoLimitConfigKeyedBroadcastProcessFunction)

    val autoLimitByCountDataStream: DataStream[(AutoLimitOneConfig, IndicatorResult)] = autoLimitByTimeDataStream.getSideOutput(outputTagByCount)

    //todo by count autoLimit
    val autoLimitByCountIndicatorListDataStream: DataStream[List[(AutoLimitOneConfig, IndicatorResult)]] = autoLimitByCountDataStream
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[(AutoLimitOneConfig, IndicatorResult)](Duration.ofMinutes(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[(AutoLimitOneConfig, IndicatorResult)]{
          override def extractTimestamp(elem:(AutoLimitOneConfig, IndicatorResult),recordTimestamp: Long) = System.currentTimeMillis()
        }))
      //将indicator 聚合成以run为单位
      .keyBy(x=>s"${x._2.runId},${x._1.controlPlanId}")
      .window(GlobalWindows.create())
      .trigger(new MainFabAutoLimitByCountIndicatorTrigger)
      .process(new MainFabAutoLimitByCountIndicatorProcessWindowFunction)

    val AutoLimitByCountResultDataStream: DataStream[AutoLimitResult] = autoLimitByCountIndicatorListDataStream
      //count run 数量
      .keyBy(_.head._1.controlPlanId.toString)
      .window(GlobalWindows.create())
      .trigger(new MainFabAutoLimitByCountTaskTrigger)
      .process(new MainFabAutoLimitByCountTaskProcessWindowFunction)

    //todo by time autoLimit
    val AutoLimitByTimeResultDataStream: DataStream[AutoLimitResult] = autoLimitByTimeDataStream
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(AutoLimitOneConfig, IndicatorResult)](Time.seconds(1)) {
          override def extractTimestamp(element: (AutoLimitOneConfig, IndicatorResult)): Long = {
            System.currentTimeMillis()  // 指定事件时间
          }
        })
      .keyBy(x => {x._2.indicatorId.toString + "|" + x._1.specId})
      .window(new MainFabAutoLimitTumblingEventTimeWindows)
      .trigger(new MainFabAutoLimitByTimeTrigger)
      .process(new MainFabAutoLimitWindowProcessWindowFunction)
      .name(MainFabConstants.AutoLimitByTimeWindow)
      .uid(MainFabConstants.AutoLimitByTimeWindow)


    //auto limit 结果
    val AutoLimitResultDataStream: DataStream[AutoLimitResult] = AutoLimitByTimeResultDataStream.union(AutoLimitByCountResultDataStream)
    AutoLimitResultDataStream
      .addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC,
      new FdcKafkaSchema[AutoLimitResult](ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC
        //按照taskId分区
        , (e: AutoLimitResult) => e.taskId
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("auto limit result")
      .uid("auto limit result")



    //task 开始任务
    val AutoLimitTaskByCountDataStream= AutoLimitByCountResultDataStream.getSideOutput(outputAutoLimitTask)

    val AutoLimitTaskDataStream: DataStream[AutoLimitTask] = AutoLimitByTimeResultDataStream.getSideOutput(outputAutoLimitTask).union(AutoLimitTaskByCountDataStream)

    AutoLimitTaskDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC,
      new FdcKafkaSchema[AutoLimitTask](ProjectConfig.KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC
        //按照taskId分区
        , (e: AutoLimitTask) => e.taskId
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("auto limit task")
      .uid("auto limit task")


  }

  /**
   * 获取配置的增量数据
   */
  def ConfigOutputStream(): DataStream[JsonNode] = {

    getDao().getKafkaSource(ProjectConfig.KAFKA_AUTO_LIMIT_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_AUTO_LIMIT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID)
  }


  /**
   * 获取Indicator数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {

    getDao.getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_AUTO_LIMIT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_INDICATOR_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_INDICATOR_KAFKA_SOURCE_UID)
  }

  /**
   * 获取Alarm配置以获取limit condition信息
   * @return
   */
  def getAlarmConfig: DataStream[JsonNode] = {

    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_ALARM_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_AUTO_LIMIT_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONDITION_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_AUTO_LIMIT_CONDITION_CONFIG_KAFKA_SOURCE_UID)
  }
}
