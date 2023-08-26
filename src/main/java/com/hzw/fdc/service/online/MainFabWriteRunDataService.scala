package com.hzw.fdc.service.online

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabWriteRunDataDao
import com.hzw.fdc.function.online.MainFabWindow.{MainFabMESBloomFilterFunction, WindowEndRunDataHbaseSink}
import com.hzw.fdc.scalabean.{FdcData, MainFabRawData, RunData}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.PublicFunction.{FdcJsonNodeSchema, FdcKafkaSchema}
import com.hzw.fdc.json.JsonUtil.toBean
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

class MainFabWriteRunDataService extends TService {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWriteRunDataService])

  private val Dao = new MainFabWriteRunDataDao

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
    //runData 数据流
    val RunDataDS: DataStream[JsonNode] = getDatas()

//    // 设置翻滚窗口，聚合indicator数据
//    val alarmRuleResultStream: DataStream[List[JsonNode]] = RunDataDS
//      .keyBy(
//        data => {
//          val toolName = data.findPath(MainFabConstants.toolName).asText()
//          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
//          s"$toolName|$chamberName"
//        })
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
//      .process(new RunDataProcessWindowFunction())
    RunDataDS.addSink(new WindowEndRunDataHbaseSink(ProjectConfig.HBASE_MAINFAB_RUNDATA_TABLE))
      .name("Hbase RunData Sink")
      .uid("Hbase RunData Sink")

//    // 异步IO 写入hbase
//    AsyncDataStream.unorderedWait(
//      RunDataDS,
//      new WindowEndRunDataHbaseSink(ProjectConfig.HBASE_MAINFAB_RUNDATA_TABLE),
//      12000,
//      TimeUnit.MILLISECONDS,
//      100)
//      .name("Hbase Sink")


//    /**
//      *   runData写入hdfs(供后续bi报表使用)
//      */
//    val hdfsSink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(ProjectConfig.HDFS_RUNDATA_DIR), new SimpleStringEncoder[String]("UTF-8"))
//      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
//      .withRollingPolicy(
//        OnCheckpointRollingPolicy.build()
//      )
//      .build()
//    RunDataDS.map(_.toString).addSink(hdfsSink).name("hdfs Sink").setParallelism(ProjectConfig.HDFS_SET_PARALLELISM)


    /**
     *   mes信息处理
     */
    val MESDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_MES_FILTER_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_MES_FILTER_JOB_KAFKA_SOURCE_UID
    )

    val BloomFilterMESDataStream= MESDataStream
      .keyBy(x=>{
        val tool=x.findPath(MainFabConstants.toolName).asText()
        val chamber=x.findPath(MainFabConstants.chamberName).asText()
        s"$tool|$chamber"
      })
      .process(new MainFabMESBloomFilterFunction)

    //ToolMessage写到kafka
    BloomFilterMESDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_MES_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_MES_TOPIC,
        x=> {
          val tool = x.findPath(MainFabConstants.toolName).asText()
          s"$tool"
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaMESSink)
      .uid(MainFabConstants.KafkaMESSink)
  }

  /**
   *  聚合在一起，批量写入
   */
  class RunDataProcessWindowFunction extends ProcessWindowFunction[JsonNode, List[JsonNode], String, TimeWindow] {

    def process(key: String, context: Context, input: Iterable[JsonNode], out: Collector[List[JsonNode]]): Unit = {
      out.collect(input.toList)
    }
  }

  /**
   * 获取RunData数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_RUN_DATA_JOB_FROM_TIMESTAMP)
    }else {
      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID)
    }
  }
}
