package com.hzw.fdc.service.online

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabWriteRunDataDao
import com.hzw.fdc.function.online.MainFabAlarmHistory.{AlarmHistoryHbaseSink, AlarmStatisticBuildFunction, AlarmStatisticFunction, AlarmStatisticHbaseSink, AlarmStatisticOneMinMergeFunction}
import com.hzw.fdc.json.JsonUtil.{toBean, toJsonNode}
import com.hzw.fdc.scalabean.{AlarmRuleResult, AlarmStatistic, FdcData, HiveReportData}
import com.hzw.fdc.util.{FlinkStreamEnv, MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util.Date

import com.hzw.fdc.function.PublicFunction.MainFabCycleWindowKeyedProcessFunction
import com.hzw.fdc.function.online.MainFabAlarm.{IndicatorTimeOutKeyedBroadcastProcessFunction, MainFabIndicator0DownTimeKeyProcessFunction}
import com.hzw.fdc.function.online.MainFabAlarmHbase.IndicatorWriteCountTrigger
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


class MainFabWriteAlarmHistoryService extends TService {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWriteAlarmHistoryService])

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

    //indicator 数据流
    val alarmDatas: DataStream[JsonNode] = getAlarmDatas()
      .keyBy(data => {
        val runId = data.findPath("runId").asText()
        s"$runId"
      })
      .process(new MainFabIndicator0DownTimeKeyProcessFunction())
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
    val alarmRuleResultStream: DataStream[List[JsonNode]] = alarmDatas
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

    alarmRuleResultStream
      .addSink(new AlarmHistoryHbaseSink())
      .name("History Hbase Sink")
      .uid("AlarmHistory_sink")

//    // 异步IO 写入hbase
//    AsyncDataStream.unorderedWait(
//      alarmDatas,
//      new AlarmHistoryHbaseSink(),
//      25000,
//      TimeUnit.MILLISECONDS,
//      100)
//      .name("History Hbase Sink").uid("AlarmHistory_sink")

//    alarmDatas.map(new AlarmStatisticBuildFunction()).filter(x => x.toolName.nonEmpty).name("alarm statistic")
//      .keyBy(a => a.toolName)
//      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
//      .process(new AlarmStatisticOneMinMergeFunction)
//      .keyBy(a => a._2)
//      .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(1)))
//      .process(new AlarmStatisticFunction())
//      .name("Statistic time window process")
//      .addSink(new AlarmStatisticHbaseSink())
//      .name("Statistic Hbase Sink")
//      .uid("Alarm statistic to Hbase")

//    //indicator 数据流
//    val IndicatorDS: DataStream[JsonNode] = alarmDatas.filter(data => {
//      val dataType = data.findPath("dataType").asText()
//
//      dataType == "AlarmLevelRule"
//    })

//    val runDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID)

//    val indicatorAndRunDataStream = IndicatorDS.union(runDataStream)

//    //读取context配置
//    val contextConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)
//
//    //读Indicator配置
//    val indicatorConfigDataStream = getDao().getKafkaJsonSource(
//      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)


//    //indicator配置数据 维表流
//    val indicatorConfig = new MapStateDescriptor[String, JsonNode](
//      "timeout_write_indicator_config",
//      //Key类型
//      BasicTypeInfo.STRING_TYPE_INFO,
//      //Value类型
//      TypeInformation.of(new TypeHint[JsonNode] {}))
//
//    val ConfigDataStream = contextConfigDataStream.union(indicatorConfigDataStream)
//
//
//    //广播配置流
//    val indicatorConfigBroadcastStream = ConfigDataStream.broadcast(indicatorConfig)


//    // 	FAB7-8 indicator多长时间没有算出来/多长时间没有收到rawdata要做告警
//    val indicatorOutTimeStream = indicatorAndRunDataStream
//      .keyBy(
//        data => {
//          val runId = data.findPath("runId").asText()
//          s"$runId"
//        })
//      .connect(indicatorConfigBroadcastStream)
//      .process(new IndicatorTimeOutKeyedBroadcastProcessFunction)


//    val hdfsIndicatorErrorReportSink1: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(ProjectConfig.HDFS_INDICATOR_ERROR_REPORT_DIR), new SimpleStringEncoder[String]("UTF-8"))
//      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
//      .withRollingPolicy(
//        OnCheckpointRollingPolicy.build()
//      )
//      .build()
//    indicatorOutTimeStream
//        .map(elem => {
//          try {
//            val fm = new SimpleDateFormat("yyyy-MM-dd")
//            val time_day = if(elem.runEndTime != 0L)
//              fm.format(new Date(elem.runEndTime))
//            else
//              fm.format(new Date(elem.windowDataCreateTime))
//
//            val time_shift_fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//            val time = if(elem.runEndTime != 0L) {
//              time_shift_fm.format(new Date(elem.runEndTime))
//            }else{
//              time_shift_fm.format(new Date(elem.windowDataCreateTime))
//            }
//
//            s"${elem.runEndTime}\t${elem.runId}\t${elem.toolGroupName}\t${elem.chamberGroupName}\t${elem.recipeGroupName}" +
//              s"\t${elem.toolName}\t${elem.chamberName}\t${elem.recipeName}\t${elem.controlPlanName}" +
//              s"\t${elem.indicatorName}\t${elem.indicatorValue}\t${time}\t${time_day}"
//          }catch {
//            case ex: Exception => logger.warn("hdfsIndicatorErrorReportSink1 error: " + ex.toString)
//              ""
//          }
//        })
//      .filter(x => x != "")
//      .addSink(hdfsIndicatorErrorReportSink1).name("indicator error report hdfs Sink").setParallelism(ProjectConfig.HDFS_SET_PARALLELISM)

//    //写入hdfs
//    val inputHdfs = IndicatorDS.map(x => parseAlarmLevelRule(x)).filter(x => x != "")
//    val hdfsSink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(ProjectConfig.HDFS_DIR), new SimpleStringEncoder[String]("UTF-8"))
//      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyyMMddHH"))
//      .withRollingPolicy(
//        OnCheckpointRollingPolicy.build()
//      )
//      .build()
//    inputHdfs.addSink(hdfsSink).name("hdfs Sink").setParallelism(ProjectConfig.HDFS_SET_PARALLELISM)

//    // flink table
//    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
//    val tableEnv = StreamTableEnvironment.create(FlinkStreamEnv.get(), settings)
//
//
//    // 连接hive
//    val hive = new HiveCatalog(ProjectConfig.HIVE_NAME, ProjectConfig.HIVE_DATABASE, ProjectConfig.HIVE_CONF_DIR,
//      ProjectConfig.HIVE_VERSION)
//    tableEnv.registerCatalog(ProjectConfig.HIVE_NAME, hive)
//
//    // set the HiveCatalog as the current catalog of the session
//    tableEnv.useCatalog(ProjectConfig.HIVE_NAME)
//
//    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
//
//    //  解析
//    val input = IndicatorDS
//      .map(x => parseAlarmLevelRuleToHiveData(x)).filter(x => x != null)
//
//
//    tableEnv.createTemporaryView("HiveReportData", input)
//
//    //        val tableSql =
//    //          """
//    //            | create table IF NOT EXISTS alarm_report_data
//    //            | (
//    //            | Location STRING, LOCATION_ID STRING,
//    //            | Module STRING, MODULE_ID STRING,
//    //            | ToolGroup STRING, TOOL_GROUP_ID STRING,
//    //            | Controlplan STRING, CONTROL_PLAN_ID STRING,
//    //            | Tool STRING, TOOL_ID STRING,
//    //            | INDICATOR_NAME STRING, RUN_ID STRING,
//    //            | CHAMBER_ID STRING, YEAR STRING, MONTH STRING, DAY STRING, TIME STRING,
//    //            | TRIGGER_ALARM_COUNT INT,
//    //            | TRIGGER_RULE_COUNT INT,
//    //            | TRIGGER_OCAP_COUNT INT,
//    //            | CREATE_TIME STRING
//    //            | )
//    //            | PARTITIONED BY ( dt STRING, hr STRING) row format delimited fields terminated by '\t' collection items terminated by '\n'
//    //            | STORED AS parquet TBLPROPERTIES
//    //            | (
//    //            | 'partition.time-extractor.timestamp-pattern'='dt $hr:00:00',
//    //            | 'sink.partition-commit.trigger'='process-time',
//    //            | 'sink.partition-commit.delay'='0s',
//    //            | 'sink.partition-commit.policy.kind'='metastore,success-file',
//    //            | 'auto-compaction'='true',
//    //            | 'compaction.file-size'='256MB'
//    //            | )
//    //            |""".stripMargin
//    //
//    //        tableEnv.executeSql(tableSql)
//
//
//    val insertSql =
//      """
//        | insert into alarm_report_data
//        | SELECT
//        |  locationName, locationId, moduleName, moduleId,
//        |  toolGroupName, toolGroupId, controlPlanName, controlPlnId,
//        |  toolName, toolName, indicatorName, runId, chamberName,
//        |  DATE_FORMAT(windowEndTime, 'yyyy'),
//        |  DATE_FORMAT(windowEndTime, 'MM'),
//        |  DATE_FORMAT(windowEndTime, 'dd'),
//        |  DATE_FORMAT(windowEndTime, 'yyyy.MM.dd'),
//        |  triggerAlarmCount,
//        |  triggerRuleCount,
//        |  triggerOcapCount,
//        |  createTime,
//        |  DATE_FORMAT(windowEndTime, 'yyyy.MM.dd'),
//        |  DATE_FORMAT(windowEndTime, 'HH')
//        | FROM HiveReportData
//        |""".stripMargin
//
//    tableEnv.executeSql(insertSql)
  }

  /**
   * 解析alram data数据写入hdfs， 以便hive读取
   */
  def parseAlarmLevelRule(elem: JsonNode): String = {
    try {
      val alarmRuleResult = toBean[FdcData[AlarmRuleResult]](elem)
      val rule = alarmRuleResult.datas

      var TRIGGER_RULE_COUNT = 0
      var TRIGGER_ALARM_COUNT = 0
      var TRIGGER_OCAP_COUNT = 0
      for (r <- rule.RULE) {
        if (r.rule == 1) {
          TRIGGER_ALARM_COUNT = 1
        }
        if (r.alarmInfo.map(_.action).nonEmpty) {
          TRIGGER_OCAP_COUNT = 1
        }
        TRIGGER_RULE_COUNT = 1
      }

      def tranTimeToString(tm: String): String = {
        val fm = new SimpleDateFormat("yyyy.MM.dd")
        val tim = fm.format(new Date(tm.toLong))
        tim
      }

      val TIME = tranTimeToString(rule.windowEndTime.toString)
      val time_arr = TIME.split("\\.")
      val YEAR = time_arr(0)
      val MONTH = time_arr(1)
      val DAY = time_arr(2)

      def parseCreateTime(tm: String): String = {
        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val tim = fm.format(new Date(tm.toLong))
        tim
      }

      val create_time = parseCreateTime(rule.indicatorCreateTime.toString)

      rule.locationName + "\t" + rule.locationId + "\t" + rule.moduleName + "\t" + rule.moduleId + "\t" +
        rule.toolGroupName + "\t" + rule.toolGroupId + "\t" + rule.toolName + "\t" + rule.toolName +
        " \t " + rule.controlPlanName + "\t" + rule.controlPlnId + "\t" + rule.indicatorName + "\t" + rule.runId + "\t" +
        rule.chamberName + "\t" + YEAR + "\t" + MONTH + "\t" + DAY + "\t" + TIME + "\t" + TRIGGER_ALARM_COUNT + "\t" +
        TRIGGER_RULE_COUNT + "\t" + TRIGGER_OCAP_COUNT + "\t" + create_time + "\t" + TIME
    } catch {
      case ex: Exception => logger.error(s"parseAlarmLevelRule ERROR : $ex\t rule: " + elem)
        ""
    }
  }

  /**
   * 解析alram data数据写入hdfs， 以便hive读取
   */
  def parseAlarmLevelRuleToHiveData(elem: JsonNode): HiveReportData = {
    try {

      val alarmRuleResult = toBean[FdcData[AlarmRuleResult]](elem)
      val rule = alarmRuleResult.datas

      var TRIGGER_RULE_COUNT = 0
      var TRIGGER_ALARM_COUNT = 0
      var TRIGGER_OCAP_COUNT = 0
      for (r <- rule.RULE) {
        if (r.rule == 1) {
          TRIGGER_ALARM_COUNT = 1
        }
        if (r.alarmInfo.map(_.action).nonEmpty) {
          TRIGGER_OCAP_COUNT = 1
        }
        TRIGGER_RULE_COUNT = 1
      }

      def parseCreateTime(tm: String): String = {
        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val tim = fm.format(new Date(tm.toLong))
        tim
      }

      val createTime = parseCreateTime(System.currentTimeMillis().toString)

      HiveReportData(
        rule.locationId.toString,
        rule.locationName,
        rule.moduleId.toString,
        rule.moduleName,
        rule.toolGroupId.toString,
        rule.toolGroupName,
        rule.controlPlnId.toString,
        rule.controlPlanName,
        rule.toolName,
        rule.indicatorName,
        rule.runId,
        rule.chamberName,
        TRIGGER_ALARM_COUNT,
        TRIGGER_RULE_COUNT,
        TRIGGER_OCAP_COUNT,
        new Timestamp(rule.windowEndTime),
        createTime
      )
    } catch {
      case ex: Exception => logger.error(s"parseAlarmLevelRule ERROR : $ex\t rule: " + elem)
        null
    }
  }


//  /**
//   * 获取alarm数据
//   */
//  override protected def getDatas(): DataStream[JsonNode] = {
//
//    getDao().getKafkaSource[JsonNode](
//      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
//      ProjectConfig.KAFKA_QUORUM,
//      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB,
//      MainFabConstants.latest,
//      MainFabConstants.MAIN_FAB_HIVE_REPORT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID,
//      MainFabConstants.MAIN_FAB_HIVE_REPORT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID)
//
//  }

  def getAlarmDatas(): DataStream[JsonNode] = {

    getDao().getKafkaSource[JsonNode](
      ProjectConfig.KAFKA_INDICATOR_RESULT_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_WRITE_ALARM_HISTORY_JOB_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_WRITE_ALARM_HISTORY_JOB_KAFKA_SOURCE_UID)

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
}
