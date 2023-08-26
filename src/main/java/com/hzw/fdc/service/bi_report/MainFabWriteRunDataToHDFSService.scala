package com.hzw.fdc.service.bi_report

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabWriteRunDataToHDFSDao
import com.hzw.fdc.function.PublicFunction.{FdcKafkaSchema, MainFabCycleWindowKeyedProcessFunction}
import com.hzw.fdc.function.online.MainFabAlarm.{IndicatorTimeOutKeyedBroadcastProcessFunction, MainFabIndicator0DownTimeKeyProcessFunction}
import com.hzw.fdc.json.JsonUtil
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Package com.hzw.fdc.service.bi_report
  * @author wanghb
  * @date 2022-09-01 14:37
  * @desc
  * @version V1.0
  */
class MainFabWriteRunDataToHDFSService extends TService {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWriteRunDataToHDFSService])

    private val Dao = new MainFabWriteRunDataToHDFSDao

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
    //runData 数据流
    val RunDataDS: DataStream[String] = getDatas()

    //runData写入hdfs(供后续bi报表使用)
    val hdfsSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(ProjectConfig.HDFS_RUNDATA_DIR), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build()
      )
      .build()

    RunDataDS
      .filter(x => x != "")
      .map(elem => {
        val dateFormat = new SimpleDateFormat("HH")
        val hour = dateFormat.format(System.currentTimeMillis())
        elem + "\001" + hour
      })
      .addSink(hdfsSink).name("run data hdfs Sink").setParallelism(ProjectConfig.HDFS_SET_RUNDATA_PARALLELISM)

    //indicator 数据流
    // todo 源数据流
    val sourceDS: DataStream[JsonNode] = getIndicatorDatas()

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

    val indicatorDataDs: DataStream[String] = filterAlarmDataDS.map(elem => {
      val alarmRuleResult: ConfigData[AlarmRuleResult] = toBean[ConfigData[AlarmRuleResult]](elem)
      JsonUtil.toJson(ToHiveData(alarmRuleResult.`dataType`, alarmRuleResult.datas))
    })

    val indicatorDataHDFSSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(ProjectConfig.HDFS_INDICATOR_DATA_DIR), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build()
      )
      .build()
    indicatorDataDs
      .filter(x => x != "")
      .map(elem => {
        val dateFormat = new SimpleDateFormat("HH")
        val hour = dateFormat.format(System.currentTimeMillis())
        elem + "\001" + hour
      })
      .addSink(indicatorDataHDFSSink).name("indicator data hdfs sink ").setParallelism(ProjectConfig.HDFS_SET_INDICATOR_DATA_PARALLELISM)



    //日志数据写入hive
/*
    val log: DataStream[JsonNode] = getProcessEndLog.union(getWindowEndLog()).union(getDataTransformLog())
    val logSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(ProjectConfig.HDFS_FLINK_JOB_LOG_DIR), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build()
      )
      .build()

    log.map(x => {
      val info: MainFabLogInfo = toBean[MainFabLogInfo](x)

      s"${info.mainFabDebugCode}\t${info.jobName}\t${info.optionName}\t${info.functionName}\t${info.logTimestamp}\t${info.logTime}\t" +
        s"${info.message}\t"+
          s"${info.paramsInfo.getOrElse("clipWindowResult","")}\t"+
          s"${info.paramsInfo.getOrElse("matchedControlPlanId","")}\t"+
          s"${info.paramsInfo.getOrElse("collectDataCostTime","")}\t"+
          s"${info.paramsInfo.getOrElse("subTaskId","")}\t"+
          s"${info.paramsInfo.getOrElse("controlWindowId","")}\t"+
          s"${info.paramsInfo.getOrElse("setStartTime","")}\t"+
          s"${info.paramsInfo.getOrElse("calcWindowCostTime","")}\t"+
          s"${info.paramsInfo.getOrElse("windowPartitionId","")}\t"+
          s"${info.paramsInfo.getOrElse("setStopTime","")}\t"+
          s"${info.paramsInfo.getOrElse("runId","")}\t"+
          s"${info.paramsInfo.getOrElse("windowType","")}\t"+
          s"${info.dataInfo}\t${info.exception}"
    })
      .addSink(logSink).name("flink job log sink ").setParallelism(ProjectConfig.HDFS_FLINK_JOB_LOG_PARALLELISM)
*/


    //切窗口结果验证(测试用)
/*
    val splitWindow: DataStream[JsonNode] = getSplitWindowResultForTest()
    splitWindow.map(x =>{
      val window: fdcWindowData = toBean[fdcWindowData](x)

      s"${window.datas.runId}\t${window.datas.controlWindowId}\t${window.datas.controlPlanId}\t${window.datas.controlPlanVersion}\t${window.datas.windowStart}" +
        s"${window.datas.windowStartTime}\t${window.datas.windowEnd}\t${window.datas.windowEndTime}\t${window.datas.windowTimeRange}\t${window.datas.windowType}\t${sensorDataList}"
    })
      .addSink(hdfsSinkUtils(ProjectConfig.HDFS_SPLIT_WINDOW_FOR_TEST_DIR)).name("split window for test").setParallelism(ProjectConfig.HDFS_SPLIT_WINDOW_FOR_TEST_PARALLELISM)
*/


    //过渡阶段,先往dg发送3个kafka topic的数据
    //rundata
    /*RunDataDS
      .map(JsonUtil.toJsonNode(_: String))
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_MAINFAB_BI_RUNDATA_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_BI_RUNDATA_TOPIC
          // 随机分区
          , x => s"${x.findPath(MainFabConstants.traceId).asText()}"
        )
        , ProjectConfig.getDGKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("bi report rundata to dg")
      .uid("bi report rundata to dg")

    //indicator原始数据
    sourceDS
      .filter(_.findPath(MainFabConstants.dataType).asText() == "AlarmLevelRule")
      .addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_RESULT_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_RESULT_TOPIC
          // 随机分区
          , x => s"${x.findPath(MainFabConstants.toolName).asText()}|${x.findPath(MainFabConstants.indicatorId).asText()}"
        )
        , ProjectConfig.getDGKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("bi report indicator to dg")
      .uid("bi report indicator to dg")

    //indicator error数据(只发错误的,后续在dg端再union indicator原始数据)
    val indicatorEtled: DataStream[JsonNode] = indicatorDataDSEtled(sourceDS)
    val runData: DataStream[JsonNode] = RunDataDS.map(JsonUtil.toJsonNode(_: String))
    val indicatorErrorData: DataStream[IndicatorErrorReportData] = calcIndicatorError(indicatorEtled,runData,getFdcConfig)

    indicatorErrorData.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_ERROR_TOPIC,
      new FdcKafkaSchema[IndicatorErrorReportData](ProjectConfig.KAFKA_MAINFAB_BI_INDICATOR_ERROR_TOPIC
        , x => s"${x.runId}"
      )
      , ProjectConfig.getDGKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("bi report indicatorError to dg")
      .uid("bi report indicatorError to dg")*/

  }

  /**
    * 处理indicatorError的数据逻辑
    */

  def calcIndicatorError(indicatorDataEtled: DataStream[JsonNode], RunDataDS: DataStream[JsonNode], indicatorConfigBroadcastStream: BroadcastStream[JsonNode]): DataStream[IndicatorErrorReportData] ={
    val indicatorAndRunDataStream = indicatorDataEtled.union(RunDataDS)

    // 	FAB7-8 indicator多长时间没有算出来/多长时间没有收到rawdata要做告警
    val indicatorOutTimeStream: DataStream[IndicatorErrorReportData] = indicatorAndRunDataStream
      .keyBy(
        data => {
          val runId = data.findPath("runId").asText()
          s"$runId"
        })
      .connect(indicatorConfigBroadcastStream)
      .process(new IndicatorTimeOutKeyedBroadcastProcessFunction)
      .filter(x => x.indicatorValue.isEmpty)

    indicatorOutTimeStream
  }

  /**
    * 广播fdc配置数据
    */

  def getFdcConfig():BroadcastStream[JsonNode] = {
    //读取context配置
    val contextConfigDataStream: DataStream[JsonNode] = getContextConfig()

    //读Indicator配置
    val indicatorConfigDataStream: DataStream[JsonNode] = getIndicatorConfig()


    //indicator配置数据 维表流
    val indicatorConfig = new MapStateDescriptor[String, JsonNode](
      "timeout_write_indicator_config",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val ConfigDataStream = contextConfigDataStream.union(indicatorConfigDataStream)

    //广播配置流
    val indicatorConfigBroadcastStream = ConfigDataStream.broadcast(indicatorConfig)

    indicatorConfigBroadcastStream
  }

  /**
    * 整理原始的来自kafka的indicator数据,供后续使用
    */

  def indicatorDataDSEtled(sourceDS: DataStream[JsonNode]) :DataStream[JsonNode] = {
    val filterAlarmDataDS: DataStream[JsonNode] = sourceDS.filter(data => {
      val dataType = data.findPath("dataType").asText()

      dataType == "AlarmLevelRule"
    })
      .keyBy(data => {
        val runId = data.findPath("runId").asText()
        s"$runId"
      })
      .process(new MainFabIndicator0DownTimeKeyProcessFunction())
      .name("MainFab 0DownTimeKeyProcessFunction -- dg")
      .uid("MainFab 0DownTimeKeyProcessFunction -- dg")
      .keyBy(
        data => {
          val toolName = data.findPath(MainFabConstants.toolName).asText()
          val chamberName = data.findPath(MainFabConstants.chamberName).asText()
          s"$toolName|$chamberName"
        }
      ).process(new MainFabCycleWindowKeyedProcessFunction())

    filterAlarmDataDS
  }

  /**
    * 写hdfs格式与路径
    */
  def hdfsSinkUtils(url: String): StreamingFileSink[String] ={
    StreamingFileSink
      .forRowFormat(new Path(url), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build()
      )
      .build()
  }

  /**
    * 获取rundata原始json数据
    */
  override protected def getDatas(): DataStream[String] = {
    getDao().getKafkaOriginalSource(
      ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_TO_HDFS_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)

  }

  /**
    * 获取Indicator和IndicatorRule数据
    */
  def getIndicatorDatas(): DataStream[JsonNode] = {

    getDao().getKafkaSource[JsonNode](
      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_INDICATOR_DATA_TO_HDFS_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_INDICATOR_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_INDICATOR_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)

  }

  /**
    * 获取context配置数据
    */
  def getContextConfig(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_CONFIG_DATA_TO_HDFS_JOB_DG,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)
  }

  /**
    * 获取indicator配置数据
    */
  def getIndicatorConfig(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_CONFIG_DATA_TO_HDFS_JOB_DG,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)
  }

  /**
    * 读取日志数据
    */
  def getProcessEndLog(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_TO_HDFS_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_PROCESSEND_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_PROCESSEND_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)
  }

  def getWindowEndLog(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_TO_HDFS_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_WINDOWEND_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_WINDOWEND_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)
  }

  def getDataTransformLog(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_LOGINFO_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_TO_HDFS_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_DATA_TRANSFORM_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_DATA_TRANSFORM_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID)
  }

  /**
    * 读取切窗口结果(测试用)
    */
  def getSplitWindowResultForTest(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_TO_HDFS_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_GET_SPLIT_WINDOW_FOR_TEST_SOURCE_UID,
      MainFabConstants.MAIN_FAB_GET_SPLIT_WINDOW_FOR_TEST_SOURCE_UID)
  }


}
