package com.hzw.fdc.util

import com.hzw.fdc.util.DateUtils.DateTimeUtil
import org.apache.flink.api.java.utils.ParameterTool

import java.util.Properties

/**
 * FdcProjectConfig
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/14 9:39
 * @since 1.0.0
 * @update 2022/12/14 9:39
 * */
object FdcProjectConfig {

  //#Hbase配置  表名命名 需要带_table 后缀
  var HBASE_ZOOKEEPER_QUORUM: String = "fdc01,fdc02,fdc03,fdc04,fdc05"
  var HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT: String = "2181"
  var HBASE_MAINFAB_RUNDATA_TABLE: String = "mainfab_rundata_table"
  var HBASE_INDICATOR_DATA_TABLE = "indicator_data_table"
  var HBASE_OFFLINE_INDICATOR_DATA_TABLE = "mainfab_offline_indicatordata_table"
  var HBASE_ALARM_HISTROY_TABLE = "mainfab_alarm_history"
  var HBASE_ALARM_STATISTIC_TABLE = "mainfab_alarm_statistic"
  var HBASE_FULL_SYNC_TIME_TABLE = "flink_full_sync_time"
  var HBASE_SYNC_WINDOW_TABLE = "flink_sync_window_"
  var HBASE_SYNC_RULE_TABLE = "flink_sync_rule_"
  var HBASE_SYNC_MIC_ALARM_TABLE = "flink_sync_mic_alarm_config_"
  var HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE = "flink_sync_chamber_alarm_switch_"
  var HBASE_SYNC_VIRTUAL_SENSOR_TABLE = "flink_sync_virtual_sensor_config_"
  var HBASE_SYNC_AUTO_LIMIT_TABLE = "flink_sync_auto_limit_"
  var HBASE_SYNC_INDICATOR_TABLE = "flink_sync_indicator_"
  var HBASE_SYNC_CONTEXT_TABLE = "flink_sync_context_"
  var HBASE_SYNC_LOG_TABLE = "flink_sync_log"
  var HBASE_SYNC_LONG_RUN_CONTROL_PLAN_TABLE = "flink_sync_long_run_control_plan_"
  var HBASE_SYNC_LONG_RUN_WINDOW_TABLE = "flink_sync_long_run_window_"

  // 配置文件的路径
  var FDC_PROPERTIES_FILE_PATH: String = _

  //#KAFKA配置
  var KAFKA_QUORUM: String = "fdc01:9092,fdc02:9092,fdc03:9092"

  //RocksDBStateBackend 地址
  var ROCKSDB_HDFS_PATH = "hdfs://nameservice1/flink-checkpoints"
  var FILESYSTEM_HDFS_PATH= "hdfs://nameservice1/flink-checkpoints-filesystem"

  // 获取kafka 数据是否指定时间戳
  var GET_KAFKA_DATA_BY_TIMESTAMP:Boolean = false

  // 重新获取kafka数据的时间戳
  var KAFKA_CONSUMER_DATA_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // kafka消费者组
  var KAFKA_CONSUMER_GROUP:String = "consumer-group-mainfab-processend-window2-job"

  // 并行度
  var SET_PARALLELISM = 1

  // checkpoint设置
  var CHECKPOINT_ENABLE = true

  // 状态state缓存
  var CHECKPOINT_STATE_BACKEND = "rocksdb"

  // checkpoint 频率
  var CHECKPOINT_INTERVAL = 90000

  // checkpoint 超时时间
  var CHECKPOINT_TIME_OUT = 600000

  // 主流DataSource 数据源kafka topic
  var KAFKA_MAINFAB_DATA_SOURCE_TOPIC = "mainfab_A_window_rawdata_topic"
  var KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC = "mainfab_indicator_topic"
  var KAFKA_WINDOW_DATA_TOPIC = "mainfab_window_data_topic"
  var KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC = "mainfab_match_controlplan_and_window_giveup_topic"  // 新增topic
  var KAFKA_CALC_WINDOW_GIVEUP_TOPIC = "mainfab_calc_window_giveup_topic"  // 新增topic
  var KAFKA_DEBUG_RESULT_LOG_TOPIC = "mainfab_debug_result_log_topic"
  var KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC:String = "mainfab_processend_window_loginfo_topic"
  var KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC = "mainfab_windowend_window_loginfo_topic"

  var KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC:String = "mainfab_processend_window_debuginfo_topic"
  var KAFKA_MAINFAB_WINDOWEND_WINDOW_DEBUGINFO_TOPIC = "mainfab_windowend_window_debuginfo_topic"

  // 维表流 kafka topic
  // ---------------------------------维表流 kafka topic start-------------------------------
  var KAFKA_MAINFAB_LONG_RUN_CONTROLPLAN_CONFIG_TOPIC = "mainfab_long_run_controlplan_config_topic" // 新增topic
  var KAFKA_MAINFAB_LONG_RUN_WINDOW_CONFIG_TOPIC = "mainfab_long_run_window_config_topic" // 新增topic
  var KAFKA_MAINFAB_INDICATOR_CONFIG_TOPIC = "mainfab_indicator_config_topic"
  var KAFKA_DEBUG_CONFIG_TOPIC = "mainfab_debug_config_topic"  // debug使用，不需要写在配置文件

  // ---------------------------------维表流 kafka topic end---------------------------------

  //数据支持长度单位小时
  var RUN_MAX_LENGTH = 26

  //切window的频率
  var CALC_WINDOW_INTERVAL: Long = 180000l
  var CALC_WINDOW_MAX_INTERVAL: Long = 300000l

  /**
   *
   * @param configname
   */
  def fdcConfig(configname: ParameterTool) = {

    // --------------------------------- Hbase  start ---------------------------------
    HBASE_ZOOKEEPER_QUORUM = configname.get("hbase.zookeeper.quorum",HBASE_ZOOKEEPER_QUORUM).trim
    HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = configname.get("hbase.zookeeper.property.clientport",HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT).trim
    HBASE_MAINFAB_RUNDATA_TABLE = configname.get("hbase.rundata.table",HBASE_MAINFAB_RUNDATA_TABLE).trim
    HBASE_INDICATOR_DATA_TABLE = configname.get("hbase.indicator.data.table",HBASE_INDICATOR_DATA_TABLE).trim
    HBASE_OFFLINE_INDICATOR_DATA_TABLE = configname.get("hbase.offline.indicator.data.table",HBASE_OFFLINE_INDICATOR_DATA_TABLE).trim
    HBASE_ALARM_HISTROY_TABLE = configname.get("hbase.alarm.history.table",HBASE_ALARM_HISTROY_TABLE).trim
    HBASE_ALARM_STATISTIC_TABLE = configname.get("hbase.alarm.statistic.table",HBASE_ALARM_STATISTIC_TABLE).trim
    HBASE_FULL_SYNC_TIME_TABLE = configname.get("hbase.full.sync.time.table",HBASE_FULL_SYNC_TIME_TABLE).trim
    HBASE_SYNC_WINDOW_TABLE = configname.get("hbase.sync.window.table",HBASE_SYNC_WINDOW_TABLE).trim
    HBASE_SYNC_RULE_TABLE = configname.get("hbase.sync.rule.table",HBASE_SYNC_RULE_TABLE).trim
    HBASE_SYNC_MIC_ALARM_TABLE = configname.get("hbase.sync.mic.alarm.table",HBASE_SYNC_MIC_ALARM_TABLE).trim
    HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE = configname.get("hbase.sync.chamber.alarm.switch.table",HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE).trim
    HBASE_SYNC_VIRTUAL_SENSOR_TABLE = configname.get("hbase.sync.virtual.sensor.table",HBASE_SYNC_VIRTUAL_SENSOR_TABLE).trim
    HBASE_SYNC_AUTO_LIMIT_TABLE = configname.get("hbase.sync.auto.limit.table",HBASE_SYNC_AUTO_LIMIT_TABLE).trim
    HBASE_SYNC_INDICATOR_TABLE = configname.get("hbase.sync.indicator.table",HBASE_SYNC_INDICATOR_TABLE).trim
    HBASE_SYNC_CONTEXT_TABLE = configname.get("hbase.sync.context.table",HBASE_SYNC_CONTEXT_TABLE).trim
    HBASE_SYNC_LOG_TABLE = configname.get("hbase.sync.log.table",HBASE_SYNC_LOG_TABLE).trim
    HBASE_SYNC_LONG_RUN_CONTROL_PLAN_TABLE = configname.get("hbase.sync.long.run.control.plan.table",HBASE_SYNC_LONG_RUN_CONTROL_PLAN_TABLE).trim
    HBASE_SYNC_LONG_RUN_WINDOW_TABLE = configname.get("hbase.sync.long.run.window.table",HBASE_SYNC_LONG_RUN_WINDOW_TABLE).trim

    // --------------------------------- Hbase  end ---------------------------------

    KAFKA_QUORUM = configname.get("kafka.zookeeper.connect",KAFKA_QUORUM).trim
    ROCKSDB_HDFS_PATH = configname.get("rocksdb.hdfs.path",ROCKSDB_HDFS_PATH).trim
    FILESYSTEM_HDFS_PATH= configname.get("filesystem.hdfs.path", FILESYSTEM_HDFS_PATH).trim
    GET_KAFKA_DATA_BY_TIMESTAMP = configname.get("enable.kafka.set.start.from.timestamp",GET_KAFKA_DATA_BY_TIMESTAMP.toString).trim.toBoolean
    KAFKA_CONSUMER_DATA_FROM_TIMESTAMP = configname.get("kafka.consumer.data.from.timestamp",KAFKA_CONSUMER_DATA_FROM_TIMESTAMP.toString).trim.toLong
    KAFKA_CONSUMER_GROUP = configname.get("kafka.consumer.group",KAFKA_CONSUMER_GROUP).trim
    SET_PARALLELISM = configname.get("set.parallelism",SET_PARALLELISM.toString).trim.toInt
    CHECKPOINT_ENABLE = configname.get("checkpoint.enable",CHECKPOINT_ENABLE.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND = configname.get("checkpoint.state.backend",CHECKPOINT_STATE_BACKEND).trim
    CHECKPOINT_INTERVAL = configname.get("checkpoint.interval",CHECKPOINT_INTERVAL.toString).trim.toInt
    CHECKPOINT_TIME_OUT = configname.get("checkpoint.time.out",CHECKPOINT_TIME_OUT.toString).trim.toInt

    // --------------------------------- 数据流 kafka topic start----------------------------------
    KAFKA_MAINFAB_DATA_SOURCE_TOPIC = configname.get("kafka.mainfab.data.source.topic",KAFKA_MAINFAB_DATA_SOURCE_TOPIC).trim
    KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC = configname.get("kafka.topic.indicator.result",KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC).trim
    KAFKA_WINDOW_DATA_TOPIC = configname.get("kafka.topic.data.window",KAFKA_WINDOW_DATA_TOPIC).trim

    KAFKA_CALC_WINDOW_GIVEUP_TOPIC = configname.get("kafka.calc.window.giveup.topic",KAFKA_CALC_WINDOW_GIVEUP_TOPIC).trim
    KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC = configname.get("kafka.match.controlplan.and.window.giveup.topic",KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC).trim
    KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC = configname.get("kafka.mainfab.processend.window.loginfo.topic",KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC).trim
    KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC = configname.get("kafka.mainfab.windowend.window.loginfo.topic",KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC).trim


    KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC = configname.get("kafka.mainfab.processend.window.debuginfo.topic",KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC).trim
    KAFKA_MAINFAB_WINDOWEND_WINDOW_DEBUGINFO_TOPIC = configname.get("kafka.mainfab.windowend.window.debuginfo.topic",KAFKA_MAINFAB_WINDOWEND_WINDOW_DEBUGINFO_TOPIC).trim



    // ---------------------------------数据流 kafka topic end----------------------------------



    // ---------------------------------维表(配置)流 kafka topic start-------------------------------

    KAFKA_MAINFAB_LONG_RUN_CONTROLPLAN_CONFIG_TOPIC = configname.get("kafka.mainfab.long.run.controlplan.config.topic",KAFKA_MAINFAB_LONG_RUN_CONTROLPLAN_CONFIG_TOPIC).trim
    KAFKA_MAINFAB_LONG_RUN_WINDOW_CONFIG_TOPIC = configname.get("kafka.mainfab.long.run.window.config.topic",KAFKA_MAINFAB_LONG_RUN_WINDOW_CONFIG_TOPIC).trim
    KAFKA_MAINFAB_INDICATOR_CONFIG_TOPIC = configname.get("kafka.topic.indicator.config",KAFKA_MAINFAB_INDICATOR_CONFIG_TOPIC).trim

    // ---------------------------------维表流 kafka topic end---------------------------------

    RUN_MAX_LENGTH=configname.get("run.max.length",RUN_MAX_LENGTH.toString).trim.toInt
    CALC_WINDOW_INTERVAL=configname.get("calc.window.interval",CALC_WINDOW_INTERVAL.toString).trim.toLong
    CALC_WINDOW_MAX_INTERVAL=configname.get("calc.window.max.interval",CALC_WINDOW_MAX_INTERVAL.toString).trim.toLong

  }



  /**
   * kafka生产者配置
   *
   * @return
   */
  def getKafkaProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", KAFKA_QUORUM)
    properties.put("retries", "2147483647")
    properties.put("buffer.memory", "67108864")
    properties.put("batch.size", "131072")
    properties.put("linger.ms", "100")
    properties.put("max.in.flight.requests.per.connection", "1")
    properties.put("retry.backoff.ms", "100")
    properties.put("max.request.size", "100000000")
    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "")
    properties
  }


  /**
   * kafka消费者配置
   *
   * @return mn
   */
  def getKafkaConsumerProperties(ip: String, consumer_group: String, autoOffsetReset: String): Properties = {
    val properties = new java.util.Properties()
    properties.setProperty("bootstrap.servers", ip)
    properties.setProperty("group.id", consumer_group)
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", "5000")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    /**
     * earliest
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
     * latest
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
     * none
     * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */

    properties.setProperty("auto.offset.reset", autoOffsetReset)
    properties
  }

  /**
   * 初始化
   */
  def initConfig(): ParameterTool = {
    var configname: ParameterTool = null
    try {
      configname = ParameterTool.fromPropertiesFile(FDC_PROPERTIES_FILE_PATH)
      fdcConfig(configname)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    configname
  }

  /**
   * 获取变量
   */
  def getConfig(configname: ParameterTool): ParameterTool = {
    fdcConfig(configname)
    configname
  }

}
