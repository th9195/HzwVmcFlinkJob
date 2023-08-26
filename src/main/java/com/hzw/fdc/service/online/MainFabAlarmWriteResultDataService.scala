package com.hzw.fdc.service.online

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.dao.MainFabFileDao
import com.hzw.fdc.function.online.MainFabAlarm.{AlarmHbaseSink, IndicatorTimeOutKeyedBroadcastProcessFunction, MainFabIndicator0DownTimeKeyProcessFunction}
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.function.PublicFunction.MainFabCycleWindowKeyedProcessFunction
import com.hzw.fdc.function.online.MainFabAlarmHbase.IndicatorWriteCountTrigger
import com.hzw.fdc.util.redisUtils.{RedisSinkJava, RedisSinkScala}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


class MainFabAlarmWriteResultDataService extends TService {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAlarmWriteResultDataService])

  private val Dao = new MainFabFileDao

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
    //indicator 数据流

    // todo 源数据流
    val sourceDS = getDatas()

    // todo advancedIndicator_xxx 数据流
//    val advancedIndicatorCacheDS: DataStream[JsonNode] = sourceDS.filter(data => {
//      val dataType = data.findPath("dataType").asText()
//      dataType.startsWith("advancedIndicator_")
//    })

    // todo ewmaCache 数据流
//    val ewmaCacheDS: DataStream[JsonNode] = sourceDS.filter(data => {
//      val dataType = data.findPath("dataType").asText()
//      dataType == "alarmEwmaCache"
//    })

    // todo indicator 数据流
    val filterAlarmDataDS: DataStream[JsonNode] = sourceDS.filter(data => {
      val dataType = data.findPath("dataType").asText()

      dataType == "AlarmLevelRule"
    })
      .keyBy(data => {
        val runId = data.findPath("runId").asText()
        s"$runId"
      })
      .process(new MainFabIndicator0DownTimeKeyProcessFunction())
      .name("MainFab 0DownTimeKeyProcessFunction")
      .uid("MainFab 0DownTimeKeyProcessFunction")
      .keyBy(
        data => {
          val toolName = data.findPath(MainFabConstants.toolName).asText()
          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
          s"$toolName|$chamberName"
        }
      ).process(new MainFabCycleWindowKeyedProcessFunction())


    /**
     *  indicator入库hbase  //设置翻滚窗口,聚合indicator数据
     */
    val alarmRuleResultStream: DataStream[List[JsonNode]] = filterAlarmDataDS
      .keyBy(
        data => {
          val toolName = data.findPath(MainFabConstants.toolName).asText()
          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
          s"$toolName|$chamberName"
        }
      )
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .trigger(new IndicatorWriteCountTrigger())
      .process(new IndicatorResultProcessWindowFunction())


    // todo sink IndicatorData_table
    alarmRuleResultStream.addSink(new AlarmHbaseSink())
      .name("Hbase indicator Sink")
      .uid("Hbase indicator Sink")


    // todo sink ewmaCacheDS 到Redis
//    ewmaCacheDS.addSink(new RedisSinkScala())
//      .name("Redis ewmaCache Sink")
//      .uid("Redis ewmaCache Sink")

//    advancedIndicatorCacheDS.addSink(new RedisSinkScala())
//      .name("Redis advancedIndicatorCache Sink")
//      .uid("Redis advancedIndicatorCache Sink")

//    // 异步IO 写入hbase
//    AsyncDataStream
//      .unorderedWait(filterAlarmDataDS,
//      new AlarmHbaseSink(),
//        12000,
//      TimeUnit.MILLISECONDS,
//      100)
//      .name("Async IO Hbase Sink")
  }

  /**
   *  聚合在一起，批量写入
   */
  class IndicatorResultProcessWindowFunction extends ProcessWindowFunction[JsonNode, List[JsonNode], String,
    TimeWindow] {

    def process(key: String, context: Context, input: Iterable[JsonNode],
                out: Collector[List[JsonNode]]): Unit = {
      try {
        if(input.nonEmpty){
          out.collect(input.toList)
        }
      }catch {
        case ex: Exception => logger.warn(s"IndicatorResultProcessWindowFunction error: ${ex.toString}")
      }
    }
  }


  /**
   * 获取Indicator和IndicatorRule数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_INDICATOR_RESULT_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_INDICATOR_HBASE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_INDICATOR_HBASE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_ALARM_WRITE_RESULT_JOB_FROM_TIMESTAMP)
    }else {
      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_INDICATOR_RESULT_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_INDICATOR_HBASE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_INDICATOR_HBASE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
    }
  }


}