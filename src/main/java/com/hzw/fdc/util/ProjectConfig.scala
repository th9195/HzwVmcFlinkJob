package com.hzw.fdc.util

import com.hzw.fdc.util.DateUtils.DateTimeUtil
import org.apache.commons.lang3.StringUtils

import java.util.Properties
import com.hzw.fdc.util.ProjectConfig.KAFKA_QUORUM_DG
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase


/**
 * @author gdj
 * @create 2020-06-06-19:23
 *
 */
object ProjectConfig {
  /**
   * 配置文件 配置名 全小写  用 . 分割
   */
  //允许延迟秒数
  //  var WINDOWS_MAX_OUT_OF_ORDERNESS =2
  //每次批量写入的数量
  var RAW_DATA_WRITE_BATCH_COUNT:Int = 200
  //数据支持长度单位小时
  var RUN_MAX_LENGTH:Int= 26

  //RocksDBStateBackend 地址
  var ROCKSDB_HDFS_PATH = "hdfs://nameservice1/flink-checkpoints"
  var FILESYSTEM_HDFS_PATH= "hdfs://nameservice1/flink-checkpoints-filesystem"

  // ocap聚合后超时时间
  var RUN_ALARM_TIME_OUT = "10000"

  // 离线超时时间
  var OFFLINE_TASK_TIME_OUT = "10000"

  //runData最大超时时间 单位：小时
//  var RUN_DATA_MAX_TIME:Int= 24

  var JOB_VERSION = "none"

  var INDICATOR_BATCH_WRITE_HBASE_NUM = 100
  var  PROCESSEND_DATA_DEPLOY_TIME = 10000

  var WINDOWEND_SENSOR_SPLIT_GROUP_COUNT = 16
  var PROCESSEND_SENSOR_SPLIT_GROUP_COUNT = 16

  //checkpoint enable online AB流
  var CHECKPOINT_ENABLE_WINDOW_JOB = false
  var CHECKPOINT_ENABLE_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_DRIFT_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_CALCULATED_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_LOGISTIC_INDICATOR_JOB = false
  //online phase4新增
  var CHECKPOINT_ENABLE_ROUTER_JOB = false
  var CHECKPOINT_ENABLE_VERSION_TAG_JOB = false
  var CHECKPOINT_ENABLE_ALARM_WRITE_RESULT_DATA_JOB = false
  var CHECKPOINT_ENABLE_WRITE_ALARM_HISTORY_JOB = false
  var CHECKPOINT_ENABLE_WRITE_HIVE_REPORT_JOB = false
  var CHECKPOINT_ENABLE_WRITE_RAW_DATA_JOB = false
  var CHECKPOINT_ENABLE_WRITE_RUN_DATA_JOB = false
  var CHECKPOINT_ENABLE_MES_FILTER_JOB = false
  //online
  var CHECKPOINT_ENABLE_AUTO_LIMIT_JOB = false
  var CHECKPOINT_ENABLE_ALARM_JOB = false
  var CHECKPOINT_ENABLE_FILE_JOB = false

  //checkpoint enable offline
  var CHECKPOINT_ENABLE_OFFLINE_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_WINDOW_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_VIRTUAL_SENSOR_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_RESULT_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_DRIFT_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_LOGISTIC_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_CALCULATED_INDICATOR_JOB = false
  var CHECKPOINT_ENABLE_OFFLINE_AUTOLIMIT_JOB = false


  //checkpoint enable bi_report
  var CHECKPOINT_ENABLE_WRITE_RUN_DATA_TO_HDFS_JOB = false


  //set parallelism online AB流
  var SET_PARALLELISM_WINDOW_JOB = 1
  var SET_PARALLELISM_INDICATOR_JOB = 1
  var SET_PARALLELISM_LOGISTIC_INDICATOR_JOB = 1
  var SET_PARALLELISM_DRIFT_INDICATOR_JOB = 1
  var SET_PARALLELISM_CALCULATED_INDICATOR_JOB = 1
  //online phase4新增
  var SET_PARALLELISM_ROUTER_JOB = 1
  var SET_PARALLELISM_VERSION_TAG_JOB = 1
  var SET_PARALLELISM_ALARM_WRITE_RESULT_DATA_JOB = 1
  var SET_PARALLELISM_WRITE_ALARM_HISTORY_JOB = 1
  var SET_PARALLELISM_WRITE_HIVE_REPORT_JOB = 1
  var SET_PARALLELISM_WRITE_RAW_DATA_JOB = 1
  var SET_PARALLELISM_WRITE_RUN_DATA_JOB = 1
  var SET_PARALLELISM_MES_FILTER_JOB = 1

  //online
  var SET_PARALLELISM_AUTO_LIMIT_JOB = 1
  var SET_PARALLELISM_ALARM_JOB = 1
  var SET_PARALLELISM_FILE_JOB = 1

  //set parallelism offline
  var SET_PARALLELISM_OFFLINE_INDICATOR_JOB = 1
  var SET_PARALLELISM_OFFLINE_WINDOW_JOB = 1
  var SET_PARALLELISM_OFFLINE_VIRTUAL_SENSOR_JOB = 1
  var SET_PARALLELISM_OFFLINE_RESULT_JOB = 1
  var SET_PARALLELISM_OFFLINE_DRIFT_INDICATOR_JOB = 1
  var SET_PARALLELISM_OFFLINE_LOGISTIC_INDICATOR_JOB = 1
  var SET_PARALLELISM_OFFLINE_CALCULATED_INDICATOR_JOB = 1
  var SET_PARALLELISM_OFFLINE_AUTOLIMIT_JOB = 1

  //set parallelism bi_report
  var SET_PARALLELISM_WRITE_RUN_DATA_TO_HDFS_JOB = 1
  var SET_PARALLELISM_WRITE_DATA_FROM_KAFKA_TO_DG_JOB = 1


  //rawData opentsdb地址
  var OPENTSDB_RAWDATA_URL = "http://10.1.10.211:4242/api/query"

  //oracle配置
  var MAIN_FAB_CORE_ORACLE_URL: String = "jdbc:oracle:thin:@10.1.10.208:1522:ORCLCDB"
  var MAIN_FAB_CORE_ORACLE_USER: String = "mainfabdev"
  var MAIN_FAB_CORE_ORACLE_PASSWORD: String = "mainfabdev"

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

  var HBASE_SYNC_EWMA_RETARGET_CONFIG_TABLE = "flink_sync_ewma_retarget_config"
  var HBASE_EWMA_RETARGET_HISTORY_TABLE = "mainfab_ewma_retarget_history"

  //#KAFKA配置
  var KAFKA_QUORUM: String = "fdc01:9092,fdc02:9092,fdc03:9092"
  var KAFKA_QUORUM_DG = "10.22.13.231:9092"

  var PI_RUN_KAFKA_QUORUM: String = "fdc01:9092,fdc02:9092,fdc03:9092"

  //OPENTSDB配置
  var OPENTSDB_CLIENT_URL: String = "http://fdc01"
  var OPENTSDB_CLIENT_PORT: Int = 4242

  //配置文件地址
  var PROPERTIES_FILE_PATH: String = _
  //HDFS report地址
  var HDFS_DIR = "/data/base_alarm_report/"
  var HDFS_INDICATOR_ERROR_REPORT_DIR = "/data/indicator_error_report/"
  var HDFS_SET_PARALLELISM = 3
  var HDFS_RUNDATA_DIR = "/data/base_rundata_detail/"
  var HDFS_SET_RUNDATA_PARALLELISM = 3
  var HDFS_INDICATOR_DATA_DIR = "/data/base_indicator_data_detail/"
  var HDFS_FLINK_JOB_LOG_DIR = "/data/flink_job_log/"
  var HDFS_SPLIT_WINDOW_FOR_TEST_DIR = "/data/split_window_for_test/"
  var HDFS_SET_INDICATOR_DATA_PARALLELISM = 3
  var HDFS_FLINK_JOB_LOG_PARALLELISM = 3
  var HDFS_SPLIT_WINDOW_FOR_TEST_PARALLELISM = 3
  //RAWTRACE  INDICATOR 数据写文件地址
  var RAWTRACE_FILE_PATH = "/rawTrace"
  var INDICATOR_FILE_PATH = "/indicator"
  var FILE_PATH_NUM = 12
  var FS_URL = "hdfs://fdc02.hzw.com:8020"

  // hive 配置
  val HIVE_NAME = "myhive"
  val HIVE_DATABASE = "default"
  val HIVE_CONF_DIR = "/etc/hive/conf"
  val HIVE_VERSION = "1.1.0"

  //  topic命名 需要带_topic 后缀
  //kafka topic config topic
  var KAFKA_CONTEXT_CONFIG_TOPIC: String = "mainfab_context_config_topic"
  var KAFKA_WINDOW_CONFIG_TOPIC: String = "mainfab_window_config_topic"
  var KAFKA_INDICATOR_CONFIG_TOPIC = "mainfab_indicator_config_topic"
  var KAFKA_AUTO_LIMIT_CONFIG_TOPIC = "mainfab_autoLimit_config_topic"
  var KAFKA_VIRTUAL_SENSOR_CONFIG_TOPIC = "mainfab_virtual_sensor_config_topic"
  var KAFKA_ALARM_CONFIG_TOPIC = "mainfab_alarm_config_topic"

  var KAFKA_DEBUG_CONFIG_TOPIC = "mainfab_debug_config_topic"
  var KAFKA_DEBUG_RESULT_LOG_TOPIC = "mainfab_debug_result_log_topic"

  //  for bi
  var KAFKA_MAINFAB_BI_RUNDATA_TOPIC = "mainfab_bi_rundata_topic"
  var KAFKA_MAINFAB_BI_INDICATOR_RESULT_TOPIC = "mainfab_bi_indicator_result_topic"
  var KAFKA_MAINFAB_BI_INDICATOR_ERROR_TOPIC = "mainfab_bi_indicator_error_topic"



  // kafka topic 业务数据
  var KAFKA_MAINFAB_DATA_TOPIC: String = "mainfab_data_topic"
  var KAFKA_MAINFAB_VERSION_TAG_TOPIC: String = "mainfab_version_tag_topic"
  var KAFKA_MAINFAB_PI_RUN_TOPIC: String = ""
  var KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC: String = "mainfab_transform_data_topic"
  var KAFKA_MAINFAB_MES_TOPIC: String = "mainfab_mes_topic"

  var KAFKA_WINDOW_DATA_TOPIC = "mainfab_window_data_topic"
  var KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC = "mainfab_indicator_topic"
  var KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC = "mainfab_logistic_indicator_topic"
  var KAFKA_DRIFT_INDICATOR_TOPIC = "mainfab_drift_indicator_topic"
  var KAFKA_CALCULATED_INDICATOR_TOPIC = "mainfab_calculated_indicator_topic"

  var KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC = "mainfab_autoLimit_result_topic"
  var KAFKA_ALARM_TOPIC = "mainfab_alarm_topic"
  var KAFKA_INDICATOR_RESULT_TOPIC = "mainfab_indicator_result_topic"

  var KAFKA_MAINFAB_RAWDATA_TOPIC = "mainfab_rawdata_topic"
  var KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC = "mainfab_window_rawdata_topic"
  var KAFKA_MAINFAB_RUNDATA_TOPIC = "mainfab_rundata_topic"
  var KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC = "mainfab_toolmessage_topic"

  var KAFKA_ALARM_SWITCH_ACK_TOPIC = "alarm_switch_ack_topic"
  var KAFKA_MAINFAB_CHECK_CONFIG_TOPIC = "mainfab_check_config_topic"

  // kafka topic PM状态数据
  var KAFKA_PM_TOPIC = "mainfab_pm_topic"
  // kafka topic 状态数据
  var KAFKA_UP_DATE_TOPIC = "mainfab_update_topic"


  //kafka topic 离线
  var KAFKA_OFFLINE_TASK_TOPIC: String = "mainfab_offline_task_topic"
  var KAFKA_OFFLINE_WINDOW_DATA_TOPIC: String = "mainfab_offline_window_data_topic"
  var KAFKA_OFFLINE_RESULT_TOPIC: String = "mainfab_offline_result_topic"
  var KAFKA_OFFLINE_INDICATOR_TOPIC: String = "mainfab_offline_indicator_topic"
  var KAFKA_OFFLINE_INDICATOR_RESULT_TOPIC: String = "mainfab_offline_indicator_result_topic"
  var KAFKA_OFFLINE_DRIFT_INDICATOR_TOPIC: String = "mainfab_offline_drift_indicator_topic"
  var KAFKA_OFFLINE_CALCULATED_INDICATOR_TOPIC: String = "mainfab_offline_calculated_indicator_topic"
  var KAFKA_OFFLINE_LOGISTIC_INDICATOR_TOPIC: String = "mainfab_offline_logistic_indicator_topic"


  /**
   * kafka consumer group 按job分类，一个job消费不同topic共用一个consumer group ID
   * consumer group 小写 - 分割
   * 对象名 大写 _ 分割
   */
  //online
  var KAFKA_CONSUMER_GROUP_WINDOW_JOB: String = "consumer-group-mainfab-window-job"
  var KAFKA_CONSUMER_GROUP_INDICATOR_JOB: String = "consumer-group-mainfab-indicator-job"
  var KAFKA_CONSUMER_GROUP_AUTO_LIMIT_JOB: String = "consumer-group-mainfab-auto-limit-job"
  var KAFKA_CONSUMER_GROUP_ALARM_JOB: String = "consumer-group-mainfab-alarm-job"
  var KAFKA_CONSUMER_GROUP_DRIFT_INDICATOR_JOB: String = "consumer-group-mainfab-drift-indicator-job"
  var KAFKA_CONSUMER_GROUP_CALCULATED_INDICATOR_JOB: String = "consumer-group-mainfab-calculated-indicator-job"
  var KAFKA_CONSUMER_GROUP_FILE_JOB: String = "consumer-group-mainfab-file-job"
  var KAFKA_CONSUMER_GROUP_LOGISTIC_INDICATOR_JOB: String = "consumer-group-mainfab-logistic-indicator-job"

  var KAFKA_CONSUMER_GROUP_ROUTER_JOB: String = "consumer-group-mainfab-router-job"
  var KAFKA_CONSUMER_GROUP_VERSION_TAG_JOB: String = "consumer-group-mainfab-version-tag-job"
  var KAFKA_CONSUMER_GROUP_WRITE_INDICATOR_RESULT_JOB: String = "consumer-group-mainfab-write-indicator-result-job"
  var KAFKA_CONSUMER_GROUP_WRITE_HIVE_REPORT_JOB: String = "consumer-group-mainfab-write-hive-report-job"
  var KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_JOB: String = "consumer-group-mainfab-write-run-data-job"
  var KAFKA_CONSUMER_GROUP_WRITE_RAW_DATA_JOB: String = "consumer-group-mainfab-write-raw-data-job"
  var KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB: String = "consumer-group-mainfab-write-alarm-history-job"
  var KAFKA_CONSUMER_GROUP_MES_FILTER_JOB:String="consumer-group-mainfab-mes-filter-job"

  //offline
  var KAFKA_CONSUMER_GROUP_OFFLINE_INDICATOR_JOB: String = "consumer-group-mainfab-offline-indicator-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_WINDOW_JOB: String = "consumer-group-mainfab-offline-window-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_VIRTUAL_SENSOR_JOB: String = "consumer-group-mainfab-offline-virtual-sensor-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_RESULT_JOB: String = "consumer-group-mainfab-offline-result-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_DRIFT_INDICATOR_JOB: String = "consumer-group-mainfab-offline-drift-indicator-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_LOGISTIC_INDICATOR_JOB: String = "consumer-group-mainfab-offline-logistic-indicator-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB: String = "consumer-group-mainfab-offline-calculated-indicator-job"
  var KAFKA_CONSUMER_GROUP_OFFLINE_AUTOLIMIT_JOB: String = "consumer-group-mainfab-offline-autolimit-job"

  //bi_report
  var KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_TO_HDFS_JOB: String = "consumer-group-mainfab-write-run-data-to-hdfs-job"
  var KAFKA_CONSUMER_GROUP_WRITE_INDICATOR_DATA_TO_HDFS_JOB: String = "consumer-group-mainfab-write-indicator-data-to-hdfs-job"
  var KAFKA_CONSUMER_GROUP_WRITE_CONFIG_DATA_TO_HDFS_JOB_DG: String = "consumer-group-mainfab-write-config-data-to-hdfs-job_dg"
  var KAFKA_CONSUMER_GROUP_WRITE_DATA_FROM_KAFKA_TO_DG_JOB: String = "consumer-group-mainfab-write-data-from-kafka-to_dg"



  // AutoLimit ByTime
  var AUTO_LIMIT_BY_TIME_NUM:String = null


  // Redis
  var REDIS_HOST:String = "mainfab01"
  var REDIS_PORT:Int = 6379
  var REDIS_MAX_TOTAL:Int = 8
  var REDIS_MAX_IDLE:Int = 8
  var REDIS_MIN_IDLE:Int = 0
  var REDIS_CONNECTION_TIMEOUT:Int = 5000
  var REDIS_EWMA_CACHE_DATABASE:Int = 0
  var REDIS_CONFIG_CACHE_DATABASE:Int = 1

  var REDIS_MASTER:String = "mymaster"
  var REDIS_SENTINEL_NODES:String = "fdc12:26379,fdc13:26379,fdc14:26379"

  var INIT_EWMA_TARGET_FROM_REDIS = true
  var INIT_ADVANCED_INDICATOR_FROM_REDIS = true


  // Offline AutoLimit
  var OFFLINE_AUTOLIMIT_EVERY_SHARD_TIMES: Long = 1 * 24 * 60 * 60 * 1000l


  // new alarm config 是否清空全部统计计数
  var NEW_ALARM_CONFIG_RESET_ALL_ALARM_COUNT = false
  var ALARM_RULE_3_MAX_N: Long = 100

  var SET_MAX_RESULT_SIZE: Long = 20971520
  var SET_SCAN_CACHE_SIZE: Int = 100


  // 根据时间戳消费某个kafka topic 的历史数据 用于生成环境排查问题
  var CONSUMER_KAFKA_HISTORY_DATA_TOPIC = "mainfab_data_topic"
  var CONSUMER_KAFKA_HISTORY_DATA_GROUPID = "consumer-group-mainfab-kafka-history-data-job-toby"
  var CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP = System.currentTimeMillis()
  var CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO = ""
  var CONSUMER_KAFKA_HISTORY_DATA_FILE_PATH = "/kafkaHistoryData/"
  var SET_PARALLELISM_OFFLINE_CONSUMER_KAFKA_HISTORY_DATA_JOB = 1
  var CHECKPOINT_ENABLE_OFFLINE_CONSUMER_KAFKA_HISTORY_DATA_JOB = false


  /******************************************* Window Job 改造 start start start *************************************************/

  // todo 0- Hbase配置表
  var HBASE_SYNC_CONTROL_PLAN_WINDOW_TABLE = "flink_sync_control_plan_window_"

  // todo 1- 新增 topic
  var KAFKA_MAINFAB_CONTROLPLAN_WINDOW_CONFIG_TOPIC:String = "mainfab_controlplan_window_config_topic"

  var KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_LOGINFO_TOPIC:String = "mainfab_data_transform_window_loginfo_topic"
  var KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC:String = "mainfab_processend_window_loginfo_topic"
  var KAFKA_MAINFAB_WINDOWEND_WINDOW_LOGINFO_TOPIC:String = "mainfab_windowend_window_loginfo_topic"

  // todo 2- DataTransformWindow Job
  // 获取kafka 数据是否指定时间戳
  var GET_KAFKA_DATA_BY_TIMESTAMP:Boolean = false

  //开启指定时间段(指定tool)消费kafka数据过滤  --tool
  var GET_KAFKA_DATA_BY_TIMESTAMP_TOOLS = ""

  //开启指定时间段(指定tool)消费kafka数据过滤  --traceid
  var GET_KAFKA_DATA_BY_TIMESTAMP_ENDTIME = ""


  // DataTransformWindow Job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // DataTransformWindow Job kafka消费者组
  var KAFKA_CONSUMER_GROUP_DATA_TRANSFORM_WINDOW_JOB:String = "consumer-group-mainfab-data-transform-window-job"

  // DataTransformWindow Job并行度
  var SET_PARALLELISM_DATA_TRANSFORM_WINDOW_JOB = 1

  // DataTransformWindow checkpoint设置
  var CHECKPOINT_ENABLE_DATA_TRANSFORM_WINDOW_JOB = true

  // todo processEndWindow Job
  // processEndWindow Job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_PROCESSEND_WINDOW_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // processEndWindow Job kafka消费者组
  var KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB:String = "consumer-group-mainfab-processend-window-job"

  // processEndWindow Job并行度
  var SET_PARALLELISM_PROCESSEND_WINDOW_JOB = 1

  // processEndWindow checkpoint设置
  var CHECKPOINT_ENABLE_PROCESSEND_WINDOW_JOB = false


  // todo WindowEndWindow Job
  // windowEndWindow Job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_WINDOWEND_WINDOW_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // windowEndWindow Job kafka消费者组
  var KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB:String = "consumer-group-mainfab-windowend-window-job"

  // windowEndWindow Job并行度
  var SET_PARALLELISM_WINDOWEND_WINDOW_JOB = 1

  // windowEndWindow checkpoint设置
  var CHECKPOINT_ENABLE_WINDOWEND_WINDOW_JOB = true

  var CHECKPOINT_STATE_BACKEND_ROUTER_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_VERSION_TAG_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_DATA_TRANSFORM_WINDOW_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_INDICATOR_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_ALARM_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_ALARM_HISTORY_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_ALARM_WRITE_RESULT_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_RAW_DATA_JOB = "rocksdb"
  var CHECKPOINT_STATE_BACKEND_RUN_ATA_JOB = "rocksdb"

  // indicator Job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_INDICATOR_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // alarm job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_ALARM_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // alarm write result job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_ALARM_WRITE_RESULT_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // rawdata job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_RAW_DATA_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // rawdata job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_RUN_DATA_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  // router job重新获取kafka数据的时间戳
  var KAFKA_MAINFAB_ROUTER_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  var CHECKPOINT_INTERVAL_WINDOWEND_WINDOW_JOB = 90000
  var CHECKPOINT_INTERVAL_PROCESSEND_WINDOW_JOB = 90000
  var CHECKPOINT_INTERVAL_ALARM_HISTORY_JOB = 60000
  var CHECKPOINT_INTERVAL_ALARM_JOB = 60000
  var CHECKPOINT_INTERVAL_BI_REPORT_TO_HDFS_JOB = 60000
  var CHECKPOINT_INTERVAL_COMMON_JOB = 60000

  var CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW_JOB = 600000
  var CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW_JOB = 600000
  var CHECKPOINT_TIME_OUT_ALARM_HISTORY_JOB = 300000
  var CHECKPOINT_TIME_OUT_ALARM_JOB = 600000
  var CHECKPOINT_TIME_OUT_BI_REPORT_TO_HDFS_JOB = 300000
  var CHECKPOINT_TIME_OUT_COMMON_JOB = 300000

  var SPECIAL_ERROR_CODE = "-105"



  //************************************************ window2.0  START *****************************************/

  var KAFKA_PROPERTIES_RETRIES = "2147483647"
  var KAFKA_PROPERTIES_BUFFER_MEMORY = "67108865"
  var KAFKA_PROPERTIES_BATCH_SIZE = "131073"
  var KAFKA_PROPERTIES_LINGER_MS = "101"
  var KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "2"
  var KAFKA_PROPERTIES_RETRY_BACKOFF_MS = "101"
  var KAFKA_PROPERTIES_MAX_REQUEST_SIZE = "100000001"
  var KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS = "300001"
  var KAFKA_PROPERTIES_COMPRESSION_TYPE = "none"
  var KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_ENABLE = false
  var KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = "30000"


  var HBASE_SYNC_CONTROL_PLAN_TABLE = "flink_sync_control_plan_"
  var HBASE_SYNC_CONTROLPLAN2_TABLE = "flink_sync_controlplan2_"
  var HBASE_SYNC_WINDOW2_TABLE = "flink_sync_window2_"

  var SET_PARALLELISM_PROCESSEND_WINDOW2_JOB = 1
  var CHECKPOINT_ENABLE_PROCESSEND_WINDOW2_JOB = false
  var CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW2_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_PROCESSEND_WINDOW2_JOB = 60000
  var CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW2_JOB = 600000

  var SET_PARALLELISM_WINDOWEND_WINDOW2_JOB = 1
  var CHECKPOINT_ENABLE_WINDOWEND_WINDOW2_JOB = false
  var CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW2_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_WINDOWEND_WINDOW2_JOB = 60000
  var CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW2_JOB = 600000

  var KAFKA_MAINFAB_CONTROLPLAN_CONFIG_TOPIC = "mainfab_controlplan_config_topic"
  var KAFKA_MAINFAB_WINDOW2_CONFIG_TOPIC = "mainfab_window2_config_topic"

  var KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC = "mainfab_match_controlplan_and_window_giveup_topic"
  var KAFKA_CALC_WINDOW_GIVEUP_TOPIC = "mainfab_calc_window_giveup_topic"

  var KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC = "mainfab_processend_window_debuginfo_topic"
  var KAFKA_MAINFAB_WINDOWEND_WINDOW_DEBUGINFO_TOPIC = "mainfab_windowend_window_debuginfo_topic"

  var KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB = "consumer-group-mainfab-processend-window2-job"
  var KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW2_JOB = "consumer-group-mainfab-windowend-window2-job"

  var KAFKA_MAINFAB_PROCESSEND_WINDOW2_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp
  var KAFKA_MAINFAB_WINEDOWEND_WINDOW2_JOB_FROM_TIMESTAMP: Long = DateTimeUtil.getCurrentTimestamp

  //切window的频率
  var CALC_WINDOW_INTERVAL: Long = 180000l
  var CALC_WINDOW_MAX_INTERVAL: Long = 300000l

  // 哪些 errorCode 是不需要切窗口
  var MAINFAB_PASS_ERROR_CODES = "-105"

  // 算子级别自定义并行度

  var MAINFAB_PARTITION_DEFAULT = 16
  var MAINFAB_SOURCE_WINDOW_RAWDATA_PARTITION = 32

  var MAINFAB_PROCESSEND_WINDOW_MATCH_CONTROLPLAN_PARTITION = 32
  var MAINFAB_PROCESSEND_WINDOW_CALC_WINDOW_PARTITION = 32

  var MAINFAB_WINDOWEND_WINDOW_MATCH_CONTROLPLAN_PARTITION = 32
  var MAINFAB_WINDOWEND_WINDOW_CALC_WINDOW_PARTITION = 32

  // windowConfig 按照sensor 来分区: 每个分区的容量大小
  var MAINFAB_WINDOW2_PARTITION_CAPACITY = 10


  //************************************************ window2.0  END *****************************************/

  // 离线切窗口是否指定使用stepid
  var ENABLE_OFFLINE_SPLIT_WINDOW_BY_STEPID = true


  //******************************************* write redis data start ***********************************************/
  var SET_PARALLELISM_WRITE_REDIS_DATA_JOB = 1
  var CHECKPOINT_ENABLE_WRITE_REDIS_DATA_JOB = true
  var CHECKPOINT_STATE_BACKEND_WRITE_REDIS_DATA_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_WRITE_REDIS_DATA_JOB = 60000
  var CHECKPOINT_TIME_OUT_WRITE_REDIS_DATA_JOB = 600000
  var KAFKA_MAINFAB_REDIS_DATA_TOPIC = "mainfab_redis_data_topic"
  var KAFKA_CONSUMER_GROUP_WRITE_REDIS_DATA_JOB = "consumer-group-mainfab-write-redis-data-job"
  var KAFKA_MAINFAB_WRITE_REDIS_DATA_JOB_FROM_TIMESTAMP : Long = DateTimeUtil.getCurrentTimestamp
  var REDIS_DATA_TTL:Int = 30*24*60*60

  var MAINFAB_READ_HBASE_CONFIG_ONCE = true

  var KAFKA_MAINFAB_RETARGET_DATA_TOPIC = "mainfab_retarget_data_topic"
  //******************************************* write redis data end ***********************************************/





  //******************************************************* VMC START ***************************************************
  var SET_PARALLELISM_VMC_ETL_JOB = 1
  var CHECKPOINT_ENABLE_VMC_ETL_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_ETL_JOB = 60000
  var CHECKPOINT_TIME_OUT_VMC_ETL_JOB = 600000

  var SET_PARALLELISM_VMC_WINDOW_JOB = 1
  var CHECKPOINT_ENABLE_VMC_WINDOW_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_WINDOW_JOB = 120000
  var CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB = 600000

  var SET_PARALLELISM_VMC_INDICATOR_JOB = 1
  var CHECKPOINT_ENABLE_VMC_INDICATOR_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB = 60000
  var CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB = 600000


  var KAFKA_CONSUMER_GROUP_VMC_ETL_JOB = "consumer-group-vmc-etl-job"
  var KAFKA_CONSUMER_GROUP_VMC_WINDOW_JOB = "consumer-group-vmc-window-job"
  var KAFKA_CONSUMER_GROUP_VMC_INDICATOR_JOB = "consumer-group-vmc-indicator-job"

  var KAFKA_VMC_DATA_TOPIC = "mainfab_data_topic"
  var KAFKA_VMC_ETL_TOPIC = "vmc_etl_topic"
  var KAFKA_VMC_WINDOW_TOPIC = "vmc_window_topic"
  var KAFKA_VMC_INDICATOR_TOPIC = "vmc_indicator_topic"
  var KAFKA_VMC_CONTROLPLAN_CONFIG_TOPIC = "vmc_controlplan_config_topic"

  //******************************************************* VMC END ***************************************************


  /**
   * 初始化
   */
  def initConfig(): ParameterTool = {
    var configname: ParameterTool = null
    try {
      configname = ParameterTool.fromPropertiesFile(PROPERTIES_FILE_PATH)
      Config(configname)
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
    Config(configname)
    configname
  }

  def Config(configname: ParameterTool): ParameterTool = {

    /**
     * 配置文件 配置名 全小写  用 . 分割
     */
    if(configname.has("checkpoint.state.backend.router.job")){
      CHECKPOINT_STATE_BACKEND_ROUTER_JOB = configname.get("checkpoint.state.backend.router.job").trim
      CHECKPOINT_STATE_BACKEND_VERSION_TAG_JOB = configname.get("checkpoint.state.backend.version.tag.job", "rocksdb").trim
      CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW_JOB = configname.get("checkpoint.state.backend.windowend.window.job").trim
      CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW_JOB = configname.get("checkpoint.state.backend.processend.window.job").trim
      CHECKPOINT_STATE_BACKEND_DATA_TRANSFORM_WINDOW_JOB = configname.get("checkpoint.state.backend.data.transform.window.job").trim
      CHECKPOINT_STATE_BACKEND_INDICATOR_JOB = configname.get("checkpoint.state.backend.indicator.job").trim
      CHECKPOINT_STATE_BACKEND_ALARM_JOB = configname.get("checkpoint.state.backend.alarm.job").trim
      CHECKPOINT_STATE_BACKEND_ALARM_HISTORY_JOB =  configname.get("checkpoint.state.backend.alarm.history.job").trim
      CHECKPOINT_STATE_BACKEND_ALARM_WRITE_RESULT_JOB = configname.get("checkpoint.state.backend.alarm.write.result.job").trim
      CHECKPOINT_STATE_BACKEND_RAW_DATA_JOB = configname.get("checkpoint.state.backend.rawdata.job").trim
      CHECKPOINT_STATE_BACKEND_RUN_ATA_JOB = configname.get("checkpoint.state.backend.rundata.job").trim
    }

    try{
      if(configname.has("checkpoint.interval.alarm.history.job") ){
        CHECKPOINT_INTERVAL_ALARM_HISTORY_JOB = configname.get("checkpoint.interval.alarm.history.job").trim.toInt
      }

      if(configname.has("checkpoint.interval.alarm.job") ){
        CHECKPOINT_INTERVAL_ALARM_JOB = configname.get("checkpoint.interval.alarm.job").trim.toInt
      }

      if(configname.has("checkpoint.interval.bi.report.to.hdfs.job") ){
        CHECKPOINT_INTERVAL_BI_REPORT_TO_HDFS_JOB = configname.get("checkpoint.interval.bi.report.to.hdfs.job").trim.toInt
      }

      if(configname.has("checkpoint.interval.common.job") ){
        CHECKPOINT_INTERVAL_COMMON_JOB = configname.get("checkpoint.interval.common.job").trim.toInt
      }

      if(configname.has("checkpoint.time.out.alarm.history.job") ){
        CHECKPOINT_TIME_OUT_ALARM_HISTORY_JOB = configname.get("checkpoint.time.out.alarm.history.job").trim.toInt
      }

      if(configname.has("checkpoint.time.out.alarm.job") ){
        CHECKPOINT_TIME_OUT_ALARM_JOB = configname.get("checkpoint.time.out.alarm.job").trim.toInt
      }

      if(configname.has("checkpoint.time.out.bi.report.to.hdfs.job") ){
        CHECKPOINT_TIME_OUT_BI_REPORT_TO_HDFS_JOB = configname.get("checkpoint.time.out.bi.report.to.hdfs.job").trim.toInt
      }

      if(configname.has("checkpoint.time.out.common.job") ){
        CHECKPOINT_TIME_OUT_COMMON_JOB = configname.get("checkpoint.time.out.common.job").trim.toInt
      }
    }catch {
      case e: Exception => None
    }

    ROCKSDB_HDFS_PATH = configname.get("rocksdb.hdfs.path")
    if(configname.has("filesystem.hdfs.path")){
      FILESYSTEM_HDFS_PATH= configname.get("filesystem.hdfs.path", FILESYSTEM_HDFS_PATH)
    }

    try{
      RUN_ALARM_TIME_OUT = configname.get("run.alarm.time.out")
      OFFLINE_TASK_TIME_OUT = configname.get("offline.task.time.out")
    }catch {
      case e: Exception => None
    }

    try{
      if(configname.has("indicator.batch.write.hbase.num")) {
        INDICATOR_BATCH_WRITE_HBASE_NUM = configname.getInt("indicator.batch.write.hbase.num")
      }
    }catch {
      case e: Exception => None
    }


    JOB_VERSION = configname.get("job.version")

    RUN_MAX_LENGTH=configname.getInt("job.run.max.length")
    RAW_DATA_WRITE_BATCH_COUNT=configname.getInt("raw.data.write.batch.count")

//    RUN_DATA_MAX_TIME = configname.getInt("job.run.data.max.time")

    //Oracle
    MAIN_FAB_CORE_ORACLE_URL = configname.get("main.fab.core.oracle.url")
    MAIN_FAB_CORE_ORACLE_USER = configname.get("main.fab.core.oracle.user")
    MAIN_FAB_CORE_ORACLE_PASSWORD = configname.get("main.fab.core.oracle.password")

    //Hbase
    HBASE_ZOOKEEPER_QUORUM = configname.get("hbase.zookeeper.quorum")
    HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = configname.get("hbase.zookeeper.property.clientport")
    HBASE_MAINFAB_RUNDATA_TABLE = configname.get("hbase.rundata.table")
    HBASE_INDICATOR_DATA_TABLE = configname.get("hbase.indicator.data.table")
    HBASE_OFFLINE_INDICATOR_DATA_TABLE = configname.get("hbase.offline.indicator.data.table")
    HBASE_ALARM_HISTROY_TABLE = configname.get("hbase.alarm.history.table")
    HBASE_ALARM_STATISTIC_TABLE = configname.get("hbase.alarm.statistic.table")
    HBASE_FULL_SYNC_TIME_TABLE = configname.get("hbase.full.sync.time.table")
    HBASE_SYNC_WINDOW_TABLE = configname.get("hbase.sync.window.table")
    HBASE_SYNC_RULE_TABLE = configname.get("hbase.sync.rule.table")
    HBASE_SYNC_MIC_ALARM_TABLE = configname.get("hbase.sync.mic.alarm.table")
    HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE = configname.get("hbase.sync.chamber.alarm.switch.table")
    HBASE_SYNC_VIRTUAL_SENSOR_TABLE = configname.get("hbase.sync.virtual.sensor.table")
    HBASE_SYNC_AUTO_LIMIT_TABLE = configname.get("hbase.sync.auto.limit.table")
    HBASE_SYNC_INDICATOR_TABLE = configname.get("hbase.sync.indicator.table")
    HBASE_SYNC_CONTEXT_TABLE = configname.get("hbase.sync.context.table")
    HBASE_SYNC_LOG_TABLE = configname.get("hbase.sync.log.table")

    //kafka地址
    KAFKA_QUORUM = configname.get("kafka.zookeeper.connect")
    try {
      if(configname.has("kafka.pi.run.topic")) {
        PI_RUN_KAFKA_QUORUM = configname.get("pi.run.kafka.zookeeper.connect")
        KAFKA_MAINFAB_PI_RUN_TOPIC = configname.get("kafka.pi.run.topic")
      }
    }catch {
      case ex: Exception => None
    }
    KAFKA_QUORUM_DG = configname.get("dg.run.kafka.zookeeper.connect")

    //opentsdb rawData地址
    OPENTSDB_CLIENT_URL = configname.get("opentsdb.url")
    OPENTSDB_CLIENT_PORT = configname.getInt("opentsdb.port")
    //online topic
    KAFKA_CONTEXT_CONFIG_TOPIC = configname.get("kafka.topic.config.context")
    KAFKA_WINDOW_CONFIG_TOPIC = configname.get("kafka.topic.config.window")
    KAFKA_MAINFAB_DATA_TOPIC = configname.get("kafka.topic.main.fab.data")
    KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC = configname.get("kafka.topic.main.fab.transform.data")
    KAFKA_MAINFAB_MES_TOPIC = configname.get("kafka.topic.mes")
    KAFKA_INDICATOR_CONFIG_TOPIC = configname.get("kafka.topic.indicator.config")

    KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC=configname.get("kafka.topic.tool.message")

    KAFKA_MAINFAB_CHECK_CONFIG_TOPIC = configname.get("kafka.topic.check.config")

    KAFKA_AUTO_LIMIT_CONFIG_TOPIC = configname.get("kafka.topic.auto.limit.config.topic")
    KAFKA_ALARM_TOPIC = configname.get("kafka.alarm.topic")
    KAFKA_INDICATOR_RESULT_TOPIC = configname.get("kafka.indicator.result.topic")
    //    KAFKA_MAINFAB_INDICATOR_FILE_TOPIC = configname.get("kafka.indicator.file.topic")
    //KAFKA_MAINFAB_RAWTRACE_FILE_TOPIC = configname.get("kafka.raw.trace.file.topic")
    KAFKA_ALARM_CONFIG_TOPIC = configname.get("kafka.alarm.config.topic")
    KAFKA_ALARM_SWITCH_ACK_TOPIC = configname.get("kafka.alarm.switch.ack.topic")
    KAFKA_MAINFAB_AUTO_LIMIT_RESULT_TOPIC = configname.get("kafka.topic.auto.limit.result.indicator")
    KAFKA_VIRTUAL_SENSOR_CONFIG_TOPIC = configname.get("kafka.topic.virtual.sensor.config")
    KAFKA_PM_TOPIC = configname.get("kafka.topic.pm")
    //AB流
    KAFKA_UP_DATE_TOPIC=configname.get("kafka.topic.update")
    KAFKA_MAINFAB_RAWDATA_TOPIC = configname.get("kafka.mainfab.raw.data.topic",KAFKA_MAINFAB_RAWDATA_TOPIC).trim
    KAFKA_MAINFAB_RUNDATA_TOPIC = configname.get("kafka.mainfab.run.data.topic",KAFKA_MAINFAB_RUNDATA_TOPIC).trim
    KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC = configname.get("kafka.topic.window.rawdata")
    KAFKA_WINDOW_DATA_TOPIC = configname.get("kafka.topic.data.window")
    KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC = configname.get("kafka.topic.indicator.result")
    KAFKA_DRIFT_INDICATOR_TOPIC = configname.get("kafka.topic.drift.indicator")
    KAFKA_CALCULATED_INDICATOR_TOPIC = configname.get("kafka.topic.calculated.indicator")
    KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC = configname.get("kafka.topic.logistic.indicator")

    //offline topic
    KAFKA_OFFLINE_WINDOW_DATA_TOPIC = configname.get("kafka.topic.offline.window")
    KAFKA_OFFLINE_TASK_TOPIC = configname.get("kafka.topic.offline.task.topic")
    KAFKA_OFFLINE_RESULT_TOPIC = configname.get("kafka.topic.offline.result.topic")
    KAFKA_OFFLINE_INDICATOR_TOPIC = configname.get("kafka.offline.indicator.topic")
    KAFKA_OFFLINE_DRIFT_INDICATOR_TOPIC = configname.get("kafka.offline.drift.indicator.topic")
    KAFKA_OFFLINE_CALCULATED_INDICATOR_TOPIC = configname.get("kafka.offline.calculated.indicator.topic")
    KAFKA_OFFLINE_LOGISTIC_INDICATOR_TOPIC = configname.get("kafka.offline.logistic.indicator.topic")

    //bi
    KAFKA_MAINFAB_BI_RUNDATA_TOPIC = configname.get("kafka.topic.bi.rundata.topic")
    KAFKA_MAINFAB_BI_INDICATOR_RESULT_TOPIC = configname.get("kafka.topic.bi.indicator.result.topic")
    KAFKA_MAINFAB_BI_INDICATOR_ERROR_TOPIC = configname.get("kafka.topic.bi.indicator.error.topic")


    //离线job读OpenTSDB地址
    OPENTSDB_RAWDATA_URL = configname.get("opentsdb.rawdata.url")



    //kafka consumer group 按job分类，一个job消费不同topic共用一个consumer group ID
    //online
    KAFKA_CONSUMER_GROUP_WINDOW_JOB = configname.get("kafka.consumer.group.window.job")
    KAFKA_CONSUMER_GROUP_INDICATOR_JOB = configname.get("kafka.consumer.group.indicator.job")
    KAFKA_CONSUMER_GROUP_DRIFT_INDICATOR_JOB = configname.get("kafka.consumer.group.drift.indicator.job")
    KAFKA_CONSUMER_GROUP_CALCULATED_INDICATOR_JOB = configname.get("kafka.consumer.group.calculated.indicator.job")
    KAFKA_CONSUMER_GROUP_AUTO_LIMIT_JOB = configname.get("kafka.consumer.group.auto.limit.job")
    KAFKA_CONSUMER_GROUP_ALARM_JOB = configname.get("kafka.consumer.group.alarm.job")
    KAFKA_CONSUMER_GROUP_FILE_JOB = configname.get("kafka.consumer.group.file.job")
    KAFKA_CONSUMER_GROUP_LOGISTIC_INDICATOR_JOB = configname.get("kafka.consumer.group.logistic.indicator.job")
    //
    KAFKA_CONSUMER_GROUP_WRITE_INDICATOR_RESULT_JOB = configname.get("consumer.group.mainfab.write.indicator.result.job")
    KAFKA_CONSUMER_GROUP_WRITE_HIVE_REPORT_JOB = configname.get("consumer.group.mainfab.write.hive.report.job")
    KAFKA_CONSUMER_GROUP_WRITE_RUN_DATA_JOB = configname.get("consumer.group.mainfab.write.run.data.job")
    KAFKA_CONSUMER_GROUP_WRITE_RAW_DATA_JOB = configname.get("consumer.group.mainfab.write.raw.data.job")
    KAFKA_CONSUMER_GROUP_WRITE_ALARM_HISTORY_JOB = configname.get("consumer.group.mainfab.write.alarm.history.job")
    KAFKA_CONSUMER_GROUP_ROUTER_JOB = configname.get("consumer.group.mainfab.router.job")
    KAFKA_CONSUMER_GROUP_VERSION_TAG_JOB = configname.get("consumer.group.mainfab.version.tag.job")
    KAFKA_CONSUMER_GROUP_MES_FILTER_JOB=configname.get("consumer.group.mainfab.mes.filter.job")

    //offline
    KAFKA_CONSUMER_GROUP_OFFLINE_WINDOW_JOB = configname.get("kafka.consumer.group.offline.window.job")
    KAFKA_CONSUMER_GROUP_OFFLINE_VIRTUAL_SENSOR_JOB = configname.get("kafka.consumer.group.offline.virtual.sensor.job")
    KAFKA_CONSUMER_GROUP_OFFLINE_RESULT_JOB = configname.get("kafka.consumer.group.offline.result.job")
    KAFKA_CONSUMER_GROUP_OFFLINE_DRIFT_INDICATOR_JOB = configname.get("kafka.consumer.group.offline.drift.indicator.job")
    KAFKA_CONSUMER_GROUP_OFFLINE_LOGISTIC_INDICATOR_JOB = configname.get("kafka.consumer.group.offline.logistic.indicator.job")
    KAFKA_CONSUMER_GROUP_OFFLINE_INDICATOR_JOB = configname.get("kafka.consumer.group.offline.indicator.job")
    KAFKA_CONSUMER_GROUP_OFFLINE_CALCULATED_INDICATOR_JOB = configname.get("kafka.consumer.group.offline.calculated.indicator.job")

    //bi
    KAFKA_CONSUMER_GROUP_WRITE_DATA_FROM_KAFKA_TO_DG_JOB = configname.get("kafka.consumer.group.bi.write.data.to.dg.job")

    if(null != configname.get("kafka.consumer.group.offline.autolimit.job")){
      KAFKA_CONSUMER_GROUP_OFFLINE_AUTOLIMIT_JOB = configname.get("kafka.consumer.group.offline.autolimit.job").trim()
    }


    //checkpoint enable online
    CHECKPOINT_ENABLE_WINDOW_JOB = configname.getBoolean("checkpoint.enable.window.job")
    CHECKPOINT_ENABLE_DRIFT_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.drift.indicator.job")
    CHECKPOINT_ENABLE_CALCULATED_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.calculated.indicator.job")
    CHECKPOINT_ENABLE_LOGISTIC_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.logistic.indicator.job")
    CHECKPOINT_ENABLE_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.indicator.job")
    //
    //online phase4新增
    CHECKPOINT_ENABLE_ROUTER_JOB = configname.getBoolean("checkpoint.enable.router.job")
    CHECKPOINT_ENABLE_VERSION_TAG_JOB = configname.getBoolean("checkpoint.enable.version.tag.job", true)
    CHECKPOINT_ENABLE_ALARM_WRITE_RESULT_DATA_JOB = configname.getBoolean("checkpoint.enable.alarm.write.result.job")
    CHECKPOINT_ENABLE_WRITE_ALARM_HISTORY_JOB = configname.getBoolean("checkpoint.enable.write.alarm.history.job")
    CHECKPOINT_ENABLE_WRITE_HIVE_REPORT_JOB = configname.getBoolean("checkpoint.enable.write.hive.report.job")
    CHECKPOINT_ENABLE_WRITE_RAW_DATA_JOB = configname.getBoolean("checkpoint.enable.raw.data.job")
    CHECKPOINT_ENABLE_WRITE_RUN_DATA_JOB = configname.getBoolean("checkpoint.enable.run.data.job")
    CHECKPOINT_ENABLE_MES_FILTER_JOB= configname.getBoolean("checkpoint.enable.mes.filter.job")

    CHECKPOINT_ENABLE_AUTO_LIMIT_JOB = configname.getBoolean("checkpoint.enable.auto.limit.job")
    CHECKPOINT_ENABLE_ALARM_JOB = configname.getBoolean("checkpoint.enable.alarm.job")
    CHECKPOINT_ENABLE_FILE_JOB = configname.getBoolean("checkpoint.enable.file.job")
    //checkpoint enable offline
    CHECKPOINT_ENABLE_OFFLINE_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.offline_indicator.job")
    CHECKPOINT_ENABLE_OFFLINE_WINDOW_JOB = configname.getBoolean("checkpoint.enable.offline.window.job")
    CHECKPOINT_ENABLE_OFFLINE_VIRTUAL_SENSOR_JOB = configname.getBoolean("checkpoint.enable.offline.virtual.sensor.job")
    CHECKPOINT_ENABLE_OFFLINE_RESULT_JOB = configname.getBoolean("checkpoint.enable.offline.result.job")
    CHECKPOINT_ENABLE_OFFLINE_DRIFT_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.offline.drift.indicator.job")
    CHECKPOINT_ENABLE_OFFLINE_LOGISTIC_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.offline.logistic.indicator.job")
    CHECKPOINT_ENABLE_OFFLINE_CALCULATED_INDICATOR_JOB = configname.getBoolean("checkpoint.enable.offline.calculated_indicator.job")

    if(null != configname.get("checkpoint.enable.offline.autolimit.job")){
      CHECKPOINT_ENABLE_OFFLINE_AUTOLIMIT_JOB = configname.get("checkpoint.enable.offline.autolimit.job").trim.toBoolean
    }

    //checkpoint enable bi_report
    CHECKPOINT_ENABLE_WRITE_RUN_DATA_TO_HDFS_JOB = configname.getBoolean("checkpoint.enable.run.data.to.hdfs.job")

    //set parallelism online
    SET_PARALLELISM_WINDOW_JOB = configname.getInt("set.parallelism.window.job")
    SET_PARALLELISM_INDICATOR_JOB = configname.getInt("set.parallelism.indicator.job")
    SET_PARALLELISM_DRIFT_INDICATOR_JOB = configname.getInt("set.parallelism.drift.indicator.job")
    SET_PARALLELISM_CALCULATED_INDICATOR_JOB = configname.getInt("set.parallelism.calculated.indicator.job")
    SET_PARALLELISM_LOGISTIC_INDICATOR_JOB = configname.getInt("set.parallelism.logistic.indicator.job")
    //online phase4新增
    SET_PARALLELISM_ROUTER_JOB = configname.getInt("set.parallelism.router.job")
    SET_PARALLELISM_VERSION_TAG_JOB = configname.getInt("set.parallelism.version.tag.job", 3)
    SET_PARALLELISM_ALARM_WRITE_RESULT_DATA_JOB = configname.getInt("set.parallelism.alarm.write.result.job")
    SET_PARALLELISM_WRITE_ALARM_HISTORY_JOB = configname.getInt("set.parallelism.write.alarm.history.job")
    SET_PARALLELISM_WRITE_HIVE_REPORT_JOB = configname.getInt("set.parallelism.write.hive.report.job")
    SET_PARALLELISM_WRITE_RAW_DATA_JOB = configname.getInt("set.parallelism.raw.data.job")
    SET_PARALLELISM_WRITE_RUN_DATA_JOB = configname.getInt("set.parallelism.run.data.job")
    SET_PARALLELISM_MES_FILTER_JOB= configname.getInt("set.parallelism.mes.filter.job")

    SET_PARALLELISM_AUTO_LIMIT_JOB = configname.getInt("set.parallelism.auto.limit.job")
    SET_PARALLELISM_ALARM_JOB = configname.getInt("set.parallelism.alarm.job")
    SET_PARALLELISM_FILE_JOB = configname.getInt("set.parallelism.file.job")



    //set parallelism offline
    SET_PARALLELISM_OFFLINE_INDICATOR_JOB = configname.getInt("set.parallelism.offline.indicator.job")
    SET_PARALLELISM_OFFLINE_WINDOW_JOB = configname.getInt("set.parallelism.offline.window.job")
    SET_PARALLELISM_OFFLINE_VIRTUAL_SENSOR_JOB = configname.getInt("set.parallelism.offline.virtual.sensor.job")
    SET_PARALLELISM_OFFLINE_RESULT_JOB = configname.getInt("set.parallelism.offline.result.job")
    SET_PARALLELISM_OFFLINE_DRIFT_INDICATOR_JOB = configname.getInt("set.parallelism.offline.drift.indicator.job")
    SET_PARALLELISM_OFFLINE_LOGISTIC_INDICATOR_JOB = configname.getInt("set.parallelism.offline.logistic.indicator.job")
    SET_PARALLELISM_OFFLINE_CALCULATED_INDICATOR_JOB = configname.getInt("set.parallelism.offline.calculated.indicator.job")


    SET_PARALLELISM_WRITE_RUN_DATA_TO_HDFS_JOB = configname.getInt("set.parallelism.run.data.to.hdfs.job")
    if(configname.has("set.parallelism.write.data.from.kafka.to.dg.job")) {
      SET_PARALLELISM_WRITE_DATA_FROM_KAFKA_TO_DG_JOB = configname.getInt("set.parallelism.write.data.from.kafka.to.dg.job")
    }

    if( null != configname.get("set.parallelism.offline.autolimit.job")){
      SET_PARALLELISM_OFFLINE_AUTOLIMIT_JOB = configname.get("set.parallelism.offline.autolimit.job").trim.toInt
    }


    //hdfs
    HDFS_DIR = configname.get("hdfs.dir")
    if(configname.has("hdfs.indicator.error.report.dir")){
      HDFS_INDICATOR_ERROR_REPORT_DIR = configname.get("hdfs.indicator.error.report.dir")
    }

    HDFS_RUNDATA_DIR = configname.get("hdfs.rundata.dir")
    HDFS_SET_PARALLELISM = configname.getInt("hdfs.parallelism")
    HDFS_SET_RUNDATA_PARALLELISM = configname.getInt("hdfs.rundata.parallelism")
    HDFS_INDICATOR_DATA_DIR = configname.get("hdfs.indicator.data.dir")
    HDFS_FLINK_JOB_LOG_DIR = configname.get("hdfs.flink.job.log.dir")
    HDFS_SPLIT_WINDOW_FOR_TEST_DIR = configname.get("hdfs.split.window.for.test.dir")
    HDFS_SET_INDICATOR_DATA_PARALLELISM = configname.getInt("hdfs.indicator.data.parallelism")
    //HDFS_FLINK_JOB_LOG_PARALLELISM = configname.getInt("hdfs.flink.job.log.parallelism")

    RAWTRACE_FILE_PATH = configname.get("rawtrace.file.path")
    INDICATOR_FILE_PATH = configname.get("indicator.file.path")
    FS_URL = configname.get("fs.url")

    AUTO_LIMIT_BY_TIME_NUM = configname.get("auto_limit_by_time_num")


    // redis 相关配置信息
    REDIS_HOST = configname.get("redis_host")
    REDIS_PORT = configname.get("redis_port").toInt
    REDIS_MAX_TOTAL = configname.get("redis_maxTotal").toInt
    REDIS_MAX_IDLE = configname.get("redis_maxIdle").toInt
    REDIS_MIN_IDLE = configname.get("redis_minIdle").toInt
    REDIS_CONNECTION_TIMEOUT = configname.get("redis_connectionTimeout").toInt
    REDIS_EWMA_CACHE_DATABASE = configname.get("redis_ewma_cache_database").toInt
    REDIS_CONFIG_CACHE_DATABASE = configname.get("redis_config_cache_database",REDIS_CONFIG_CACHE_DATABASE.toString).trim.toInt

    REDIS_MASTER = configname.get("redis_master",REDIS_MASTER).trim
    REDIS_SENTINEL_NODES = configname.get("redis_sentinel_nodes",REDIS_SENTINEL_NODES).trim

    INIT_EWMA_TARGET_FROM_REDIS = configname.get("init.ewma.target.from.redis",INIT_EWMA_TARGET_FROM_REDIS.toString).trim.toBoolean
    INIT_ADVANCED_INDICATOR_FROM_REDIS = configname.get("init.advanced.indicator.from.redis",INIT_ADVANCED_INDICATOR_FROM_REDIS.toString).trim.toBoolean


    // AutoLimit
    if(null != configname.get("autoLimit_every_shard_times")) {
      OFFLINE_AUTOLIMIT_EVERY_SHARD_TIMES = configname.get("autoLimit_every_shard_times").trim.toLong * 24 * 60 * 60 * 1000l
    }

    // new alarm config 是否清空全部统计计数
    NEW_ALARM_CONFIG_RESET_ALL_ALARM_COUNT = configname.get("new_alarm_config_reset_all_alarm_count",NEW_ALARM_CONFIG_RESET_ALL_ALARM_COUNT.toString).trim.toBoolean

    // Alarm 第三套规则 Rule 3 中 N的最大值
    ALARM_RULE_3_MAX_N = configname.get("alarm_rule_3_max_n",ALARM_RULE_3_MAX_N.toString).trim.toLong

    // 根据时间戳消费某个kafka topic 的历史数据 用于生成环境排查问题
    CONSUMER_KAFKA_HISTORY_DATA_TOPIC = configname.get("consumer.kafka.history.data.topic",CONSUMER_KAFKA_HISTORY_DATA_TOPIC).trim
    CONSUMER_KAFKA_HISTORY_DATA_GROUPID = configname.get("consumer.kafka.history.data.groupid",CONSUMER_KAFKA_HISTORY_DATA_GROUPID).trim
    CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP = configname.get("consumer.kafka.history.data.timestamp",CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP.toString).trim.toLong
    CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO = configname.get("consumer.kafka.history.data.filterinfo",CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO).trim


    /******************************************* Window Job 改造 start start start *************************************************/

    // todo 0- Hbase 配置信息表
    HBASE_SYNC_CONTROL_PLAN_WINDOW_TABLE = configname.get("hbase.sync.control.plan.window.table",HBASE_SYNC_CONTROL_PLAN_WINDOW_TABLE).trim

    // todo 1- 新增 topic
//    KAFKA_MAINFAB_PROCESSEND_WINDOW_TOPIC = configname.get("kafka.mainfab.processend.window.topic",KAFKA_MAINFAB_PROCESSEND_WINDOW_TOPIC).trim
//    KAFKA_MAINFAB_WINDOWEND_WINDOW_TOPIC = configname.get("kafka.mainfab.windowend.window.topic",KAFKA_MAINFAB_WINDOWEND_WINDOW_TOPIC).trim
//    KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_TOPIC = configname.get("kafka.mainfab.data.transform.window.topic",KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_TOPIC).trim

    KAFKA_MAINFAB_CONTROLPLAN_WINDOW_CONFIG_TOPIC = configname.get("kafka.mainfab.controlpaln.window.config.topic",KAFKA_MAINFAB_CONTROLPLAN_WINDOW_CONFIG_TOPIC).trim

    KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_LOGINFO_TOPIC = configname.get("kafka.mainfab.data.transform.window.loginfo.topic",KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_LOGINFO_TOPIC).trim
    KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC = configname.get("kafka.mainfab.processend.window.loginfo.topic",KAFKA_MAINFAB_PROCESSEND_WINDOW_LOGINFO_TOPIC).trim


    // todo 2- DataTransform Job
    // 获取kafka 数据是否指定时间戳
    GET_KAFKA_DATA_BY_TIMESTAMP = configname.get("enable.kafka.set.start.from.timestamp",GET_KAFKA_DATA_BY_TIMESTAMP.toString).trim.toBoolean
    GET_KAFKA_DATA_BY_TIMESTAMP_TOOLS = configname.get("enable.kafka.set.start.from.timestamp.tool",GET_KAFKA_DATA_BY_TIMESTAMP_TOOLS).trim
    GET_KAFKA_DATA_BY_TIMESTAMP_ENDTIME = configname.get("enable.kafka.set.start.from.timestamp.endtime",GET_KAFKA_DATA_BY_TIMESTAMP_ENDTIME).trim

    // DataTransform Job重新获取kafka数据的时间戳
    if((null != configname.get("kafka.mainfab.data.transform.window.job.from.timestamp")) && StringUtils.isNotEmpty(configname.get("kafka.mainfab.data.transform.window.job.from.timestamp").trim) ){

      // (设置时间戳数据单位:毫秒)
      KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.data.transform.window.job.from.timestamp").trim.toLong
    }
    KAFKA_CONSUMER_GROUP_DATA_TRANSFORM_WINDOW_JOB = configname.get("kafka.consumer.group.data.transfrom.window.job",KAFKA_CONSUMER_GROUP_DATA_TRANSFORM_WINDOW_JOB).trim
    SET_PARALLELISM_DATA_TRANSFORM_WINDOW_JOB = configname.get("set.parallelism.data.transform.window.job",SET_PARALLELISM_DATA_TRANSFORM_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_DATA_TRANSFORM_WINDOW_JOB = configname.get("checkpoint.enable.data.transform.window.job",CHECKPOINT_ENABLE_DATA_TRANSFORM_WINDOW_JOB.toString).trim.toBoolean

    // todo 2- ProcessEnd Job
    // ProcessEnd Job重新获取kafka数据的时间戳
    if((null != configname.get("kafka.mainfab.processend.window.job.from.timestamp") && StringUtils.isNotEmpty(configname.get("kafka.mainfab.processend.window.job.from.timestamp").trim) ) ){

      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_PROCESSEND_WINDOW_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.processend.window.job.from.timestamp").trim.toLong
    }
    KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB = configname.get("kafka.consumer.group.processend.window.job",KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW_JOB).trim
    SET_PARALLELISM_PROCESSEND_WINDOW_JOB = configname.get("set.parallelism.processend.window.job",SET_PARALLELISM_PROCESSEND_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_PROCESSEND_WINDOW_JOB = configname.get("checkpoint.enable.processend.window.job",CHECKPOINT_ENABLE_PROCESSEND_WINDOW_JOB.toString).trim.toBoolean



    // todo 3- WindowEnd Job 海浜自行添加



    /******************************************* Window Job 改造 end end end end *************************************************/
    // windowEnd Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.windowend.window.job.from.timestamp") ){

      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_WINDOWEND_WINDOW_JOB_FROM_TIMESTAMP =
        KAFKA_MAINFAB_WINDOWEND_WINDOW_JOB_FROM_TIMESTAMP - configname.get("kafka.mainfab.windowend.window.job.from.timestamp").trim.toLong
    }
    KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB = configname.get("kafka.consumer.group.windowend.window.job",KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW_JOB).trim
    SET_PARALLELISM_WINDOWEND_WINDOW_JOB = configname.get("set.parallelism.windowend.window.job",SET_PARALLELISM_WINDOWEND_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_WINDOWEND_WINDOW_JOB = configname.get("checkpoint.enable.windowend.window.job",CHECKPOINT_ENABLE_WINDOWEND_WINDOW_JOB.toString).trim.toBoolean


    // indicator Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.indicator.job.from.timestamp") ){
      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_INDICATOR_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.indicator.job.from.timestamp").trim.toLong
    }

    // alarm Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.alarm.job.from.timestamp") ){
      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_ALARM_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.alarm.job.from.timestamp").trim.toLong
    }

    // alarm write result Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.alarm.write.result.job.from.timestamp") ){
      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_ALARM_WRITE_RESULT_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.alarm.write.result.job.from.timestamp").trim.toLong
    }

    // rawdata Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.rawdata.job.from.timestamp") ){
      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_RAW_DATA_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.rawdata.job.from.timestamp").trim.toLong
    }

    // rundata Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.rundata.job.from.timestamp") ){
      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_RUN_DATA_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.rundata.job.from.timestamp").trim.toLong
    }

    // router Job重新获取kafka数据的时间戳
    if(null != configname.get("kafka.mainfab.router.job.from.timestamp") ){
      // 使用当前时间戳 - 设置提前多久捞数据(设置数据单位:毫秒)
      KAFKA_MAINFAB_ROUTER_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.router.job.from.timestamp").trim.toLong
    }


    //
    if(configname.has("checkpoint.interval.windowend.window.job") ){
      CHECKPOINT_INTERVAL_WINDOWEND_WINDOW_JOB = configname.get("checkpoint.interval.windowend.window.job").trim.toInt
    }

    if(configname.has("checkpoint.interval.processend.window.job") ){
      CHECKPOINT_INTERVAL_PROCESSEND_WINDOW_JOB = configname.get("checkpoint.interval.processend.window.job").trim.toInt
    }

    if(configname.has("checkpoint.interval.alarm.history.job") ){
      CHECKPOINT_INTERVAL_ALARM_HISTORY_JOB = configname.get("checkpoint.interval.alarm.history.job").trim.toInt
    }

    if(configname.has("checkpoint.interval.bi.report.to.hdfs.job") ){
      CHECKPOINT_INTERVAL_BI_REPORT_TO_HDFS_JOB = configname.get("checkpoint.interval.bi.report.to.hdfs.job").trim.toInt
    }

    if(configname.has("checkpoint.interval.common.job") ){
      CHECKPOINT_INTERVAL_COMMON_JOB = configname.get("checkpoint.interval.common.job").trim.toInt
    }

    if(configname.has("checkpoint.time.out.windowend.window.job") ){
      CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW_JOB = configname.get("checkpoint.time.out.windowend.window.job").trim.toInt
    }

    if(configname.has("checkpoint.time.out.processend.window.job") ){
      CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW_JOB = configname.get("checkpoint.time.out.processend.window.job").trim.toInt
    }

    if(configname.has("checkpoint.time.out.alarm.history.job") ){
      CHECKPOINT_TIME_OUT_ALARM_HISTORY_JOB = configname.get("checkpoint.time.out.alarm.history.job").trim.toInt
    }

    if(configname.has("checkpoint.time.out.bi.report.to.hdfs.job") ){
      CHECKPOINT_TIME_OUT_BI_REPORT_TO_HDFS_JOB = configname.get("checkpoint.time.out.bi.report.to.hdfs.job").trim.toInt
    }

    if(configname.has("checkpoint.time.out.common.job") ){
      CHECKPOINT_TIME_OUT_COMMON_JOB = configname.get("checkpoint.time.out.common.job").trim.toInt
    }

    if(configname.has("windowend.sensor.split.group.count") ){
      WINDOWEND_SENSOR_SPLIT_GROUP_COUNT = configname.get("windowend.sensor.split.group.count").trim.toInt
    }

    if(configname.has("processend.sensor.split.group.count") ){
      PROCESSEND_SENSOR_SPLIT_GROUP_COUNT = configname.get("processend.sensor.split.group.count").trim.toInt
    }

    if(configname.has("special.error.code") ){
      SPECIAL_ERROR_CODE = configname.get("special.error.code")
    }
    if(configname.has("processend.data.deploy.time") ){
      PROCESSEND_DATA_DEPLOY_TIME = configname.get("processend.data.deploy.time").trim.toInt
    }

    if(configname.has("file.path.num") ){
      FILE_PATH_NUM = configname.get("file.path.num").trim.toInt
    }

    KAFKA_MAINFAB_VERSION_TAG_TOPIC = configname.get("kafka.topic.main.fab.version.tag")



    //*****************************************window 2.0 start *******************************************/

    // KAFKA 配置
    KAFKA_PROPERTIES_RETRIES = configname.get("kafka.properties.retries",KAFKA_PROPERTIES_RETRIES).trim
    KAFKA_PROPERTIES_BUFFER_MEMORY = configname.get("kafka.properties.buffer.memory",KAFKA_PROPERTIES_BUFFER_MEMORY).trim
    KAFKA_PROPERTIES_BATCH_SIZE = configname.get("kafka.properties.batch.size",KAFKA_PROPERTIES_BATCH_SIZE).trim
    KAFKA_PROPERTIES_LINGER_MS = configname.get("kafka.properties.linger.ms",KAFKA_PROPERTIES_LINGER_MS).trim
    KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = configname.get("kafka.properties.max.in.flight.requests.per.connection",KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).trim
    KAFKA_PROPERTIES_RETRY_BACKOFF_MS = configname.get("kafka.properties.retry.backoff.ms",KAFKA_PROPERTIES_RETRY_BACKOFF_MS).trim
    KAFKA_PROPERTIES_MAX_REQUEST_SIZE = configname.get("kafka.properties.max.request.size",KAFKA_PROPERTIES_MAX_REQUEST_SIZE).trim
    KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS = configname.get("kafka.properties.transaction.timeout.ms",KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS).trim
    KAFKA_PROPERTIES_COMPRESSION_TYPE = configname.get("kafka.properties.compression.type",KAFKA_PROPERTIES_COMPRESSION_TYPE).trim
    KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_ENABLE = configname.get("kafka.properties.key.partition.discovery.enable",KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_ENABLE.toString).trim.toBoolean
    KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = configname.get("kafka.properties.key.partition.discovery.interval.millis",KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS).trim


    HBASE_SYNC_CONTROL_PLAN_TABLE = configname.get("hbase.sync.control.plan.table",HBASE_SYNC_CONTROL_PLAN_TABLE).trim
    HBASE_SYNC_WINDOW2_TABLE = configname.get("hbase.sync.window2.table",HBASE_SYNC_WINDOW2_TABLE).trim

    SET_PARALLELISM_PROCESSEND_WINDOW2_JOB = configname.get("set.parallelism.processend.window2.job",SET_PARALLELISM_PROCESSEND_WINDOW2_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_PROCESSEND_WINDOW2_JOB = configname.get("checkpoint.enable.processend.window2.job",CHECKPOINT_ENABLE_PROCESSEND_WINDOW2_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW2_JOB = configname.get("checkpoint.state.backend.processend.window2.job",CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW2_JOB).trim
    KAFKA_MAINFAB_PROCESSEND_WINDOW2_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.processend.window2.job.from.timestamp",KAFKA_MAINFAB_PROCESSEND_WINDOW2_JOB_FROM_TIMESTAMP.toString).trim.toLong
    CHECKPOINT_INTERVAL_PROCESSEND_WINDOW2_JOB = configname.get("checkpoint.interval.processend.window2.job",CHECKPOINT_INTERVAL_PROCESSEND_WINDOW2_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW2_JOB = configname.get("checkpoint.time.out.processend.window2.job",CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW2_JOB.toString).trim.toInt

    SET_PARALLELISM_WINDOWEND_WINDOW2_JOB = configname.get("set.parallelism.windowend.window2.job",SET_PARALLELISM_WINDOWEND_WINDOW2_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_WINDOWEND_WINDOW2_JOB = configname.get("checkpoint.enable.windowend.window2.job",CHECKPOINT_ENABLE_WINDOWEND_WINDOW2_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW2_JOB = configname.get("checkpoint.state.backend.windowend.window2.job",CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW2_JOB).trim
    KAFKA_MAINFAB_WINEDOWEND_WINDOW2_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.windowend.window2.job.from.timestamp",KAFKA_MAINFAB_WINEDOWEND_WINDOW2_JOB_FROM_TIMESTAMP.toString).trim.toLong
    CHECKPOINT_INTERVAL_WINDOWEND_WINDOW2_JOB = configname.get("checkpoint.interval.windowend.window2.job",CHECKPOINT_INTERVAL_WINDOWEND_WINDOW2_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW2_JOB = configname.get("checkpoint.time.out.windowend.window2.job",CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW2_JOB.toString).trim.toInt


    KAFKA_MAINFAB_CONTROLPLAN_CONFIG_TOPIC = configname.get("kafka.mainfab.controlplan.config.topic",KAFKA_MAINFAB_CONTROLPLAN_CONFIG_TOPIC).trim
    KAFKA_MAINFAB_WINDOW2_CONFIG_TOPIC = configname.get("kafka.mainfab.window2.config.topic",KAFKA_MAINFAB_WINDOW2_CONFIG_TOPIC).trim
    KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC = configname.get("kafka.mainfab.match.controlplan.and.window.giveup.topic",KAFKA_MATCH_CONTROLPLAN_AND_WINDOW_GIVEUP_TOPIC).trim
    KAFKA_CALC_WINDOW_GIVEUP_TOPIC = configname.get("kafka.mainfab.calc.window.giveup.topic",KAFKA_CALC_WINDOW_GIVEUP_TOPIC).trim
    KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC = configname.get("kafka.mainfab.processend.window.debuginfo.topic",KAFKA_MAINFAB_PROCESSEND_WINDOW_DEBUGINFO_TOPIC).trim
    KAFKA_MAINFAB_WINDOWEND_WINDOW_DEBUGINFO_TOPIC = configname.get("kafka.mainfab.windowend.window.debuginfo.topic",KAFKA_MAINFAB_WINDOWEND_WINDOW_DEBUGINFO_TOPIC).trim
    KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB = configname.get("kafka.mainfab.group.processend.window2.job",KAFKA_CONSUMER_GROUP_PROCESSEND_WINDOW2_JOB).trim
    KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW2_JOB = configname.get("kafka.mainfab.group.windowend.window2.job",KAFKA_CONSUMER_GROUP_WINDOWEND_WINDOW2_JOB).trim

    CALC_WINDOW_INTERVAL=configname.get("calc.window.interval",CALC_WINDOW_INTERVAL.toString).trim.toLong
    CALC_WINDOW_MAX_INTERVAL=configname.get("calc.window.max.interval",CALC_WINDOW_MAX_INTERVAL.toString).trim.toLong

    // 哪些 errorCode 是不需要切窗口
    MAINFAB_PASS_ERROR_CODES = configname.get("mainfab.pass.error.codes",MAINFAB_PASS_ERROR_CODES).trim

    // ProcessEnd 算子级别自定义并行度
    MAINFAB_PARTITION_DEFAULT = configname.get("mainfab.default.partition",MAINFAB_PARTITION_DEFAULT.toString).trim.toInt
    MAINFAB_SOURCE_WINDOW_RAWDATA_PARTITION = configname.get("mainfab.source.window.rawdata.partition",MAINFAB_SOURCE_WINDOW_RAWDATA_PARTITION.toString).trim.toInt
    MAINFAB_PROCESSEND_WINDOW_MATCH_CONTROLPLAN_PARTITION = configname.get("mainfab.processend.window.match.controlplan.partition",MAINFAB_PROCESSEND_WINDOW_MATCH_CONTROLPLAN_PARTITION.toString).trim.toInt
    MAINFAB_PROCESSEND_WINDOW_CALC_WINDOW_PARTITION = configname.get("mainfab.processend.window.calc.window.partition",MAINFAB_PROCESSEND_WINDOW_CALC_WINDOW_PARTITION.toString).trim.toInt

    MAINFAB_WINDOWEND_WINDOW_MATCH_CONTROLPLAN_PARTITION = configname.get("mainfab.windowend.window.match.controlplan.partition",MAINFAB_WINDOWEND_WINDOW_MATCH_CONTROLPLAN_PARTITION.toString).trim.toInt
    MAINFAB_WINDOWEND_WINDOW_CALC_WINDOW_PARTITION = configname.get("mainfab.windowend.window.calc.window.partition",MAINFAB_WINDOWEND_WINDOW_CALC_WINDOW_PARTITION.toString).trim.toInt

    MAINFAB_WINDOW2_PARTITION_CAPACITY = configname.get("mainfab.window2.partition.capacity",MAINFAB_WINDOW2_PARTITION_CAPACITY.toString).trim.toInt

    //*****************************************window 2.0 end *******************************************/

    ENABLE_OFFLINE_SPLIT_WINDOW_BY_STEPID = configname.get("enable.offline.split.window.by.stepid",ENABLE_OFFLINE_SPLIT_WINDOW_BY_STEPID.toString).trim.toBoolean


    //******************************************** write redis data start *************************************************//
    SET_PARALLELISM_WRITE_REDIS_DATA_JOB = configname.get("set.parallelism.write.redis.data.job",SET_PARALLELISM_WRITE_REDIS_DATA_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_WRITE_REDIS_DATA_JOB = configname.get("checkpoint.enable.write.redis.data.job",CHECKPOINT_ENABLE_WRITE_REDIS_DATA_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_WRITE_REDIS_DATA_JOB = configname.get("checkpoint.state.backend.write.redis.data.job",CHECKPOINT_STATE_BACKEND_WRITE_REDIS_DATA_JOB).trim
    CHECKPOINT_INTERVAL_WRITE_REDIS_DATA_JOB = configname.get("checkpoint.interval.write.redis.data.job",CHECKPOINT_INTERVAL_WRITE_REDIS_DATA_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_WRITE_REDIS_DATA_JOB = configname.get("checkpoint.time.out.write.redis.data.job",CHECKPOINT_TIME_OUT_WRITE_REDIS_DATA_JOB.toString).trim.toInt
    KAFKA_MAINFAB_REDIS_DATA_TOPIC = configname.get("kafka.mainfab.redis.data.topic",KAFKA_MAINFAB_REDIS_DATA_TOPIC).trim
    KAFKA_CONSUMER_GROUP_WRITE_REDIS_DATA_JOB = configname.get("kafka.mainfab.group.write.redis.data.job",KAFKA_CONSUMER_GROUP_WRITE_REDIS_DATA_JOB).trim
    KAFKA_MAINFAB_WRITE_REDIS_DATA_JOB_FROM_TIMESTAMP = configname.get("kafka.mainfab.write.redis.data.job.from.timestamp",KAFKA_MAINFAB_WRITE_REDIS_DATA_JOB_FROM_TIMESTAMP.toString).trim.toLong

    REDIS_DATA_TTL = configname.get("redis.data.ttl",REDIS_DATA_TTL.toString).trim.toInt

    MAINFAB_READ_HBASE_CONFIG_ONCE = configname.get("mainfab.read.hbase.config.once",MAINFAB_READ_HBASE_CONFIG_ONCE.toString).trim.toBoolean
    KAFKA_MAINFAB_RETARGET_DATA_TOPIC = configname.get("kafka.mainfab.retarget.data.topic",KAFKA_MAINFAB_RETARGET_DATA_TOPIC).trim
    //******************************************** write redis data end *************************************************//

    SET_MAX_RESULT_SIZE = configname.get("set.max.result.size", SET_MAX_RESULT_SIZE.toString).trim.toLong
    SET_SCAN_CACHE_SIZE = configname.get("set.scan.cache.size", SET_SCAN_CACHE_SIZE.toString).trim.toInt


    //******************************************************* VMC START ***************************************************

    SET_PARALLELISM_VMC_ETL_JOB = configname.get("set.parallelism.vmc.etl.job",SET_PARALLELISM_VMC_ETL_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_ETL_JOB = configname.get("checkpoint.enable.vmc.etl.job",CHECKPOINT_ENABLE_VMC_ETL_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB = configname.get("checkpoint.state.backend.vmc.etl.job",CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB).trim
    CHECKPOINT_INTERVAL_VMC_ETL_JOB = configname.get("checkpoint.interval.vmc.etl.job",CHECKPOINT_INTERVAL_VMC_ETL_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_ETL_JOB = configname.get("checkpoint.time.out.vmc.etl.job",CHECKPOINT_TIME_OUT_VMC_ETL_JOB.toString).trim.toInt

    SET_PARALLELISM_VMC_WINDOW_JOB = configname.get("set.parallelism.vmc.window.job",SET_PARALLELISM_VMC_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_WINDOW_JOB = configname.get("checkpoint.enable.vmc.window.job",CHECKPOINT_ENABLE_VMC_WINDOW_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB = configname.get("checkpoint.state.backend.vmc.window.job",CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB).trim
    CHECKPOINT_INTERVAL_VMC_WINDOW_JOB = configname.get("checkpoint.interval.vmc.window.job",CHECKPOINT_INTERVAL_VMC_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB = configname.get("checkpoint.time.out.vmc.window.job",CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB.toString).trim.toInt


    SET_PARALLELISM_VMC_INDICATOR_JOB = configname.get("set.parallelism.vmc.indicator.job",SET_PARALLELISM_VMC_INDICATOR_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_INDICATOR_JOB = configname.get("checkpoint.enable.vmc.indicator.job",CHECKPOINT_ENABLE_VMC_INDICATOR_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB = configname.get("checkpoint.state.backend.vmc.indicator.job",CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB).trim
    CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB = configname.get("checkpoint.interval.vmc.indicator.job",CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB = configname.get("checkpoint.time.out.vmc.indicator.job",CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB.toString).trim.toInt

    KAFKA_CONSUMER_GROUP_VMC_ETL_JOB = configname.get("consumer.group.vmc.etl.job",KAFKA_CONSUMER_GROUP_VMC_ETL_JOB.toString).trim
    KAFKA_CONSUMER_GROUP_VMC_WINDOW_JOB = configname.get("consumer.group.vmc.window.job",KAFKA_CONSUMER_GROUP_VMC_WINDOW_JOB.toString).trim
    KAFKA_CONSUMER_GROUP_VMC_INDICATOR_JOB = configname.get("consumer.group.vmc.indicator.job",KAFKA_CONSUMER_GROUP_VMC_INDICATOR_JOB.toString).trim


    KAFKA_VMC_DATA_TOPIC = configname.get("kafka.vmc.data.topic",KAFKA_VMC_DATA_TOPIC.toString).trim
    KAFKA_VMC_ETL_TOPIC = configname.get("kafka.vmc.etl.topic",KAFKA_VMC_ETL_TOPIC.toString).trim
    KAFKA_VMC_WINDOW_TOPIC = configname.get("kafka.vmc.window.topic",KAFKA_VMC_WINDOW_TOPIC.toString).trim
    KAFKA_VMC_INDICATOR_TOPIC = configname.get("kafka.vmc.indicator.topic",KAFKA_VMC_INDICATOR_TOPIC.toString).trim
    KAFKA_VMC_CONTROLPLAN_CONFIG_TOPIC = configname.get("kafka.vmc.controlplan.config.topic",KAFKA_VMC_CONTROLPLAN_CONFIG_TOPIC.toString).trim

    //******************************************************* VMC END ***************************************************




    configname
  }


  /**
   * kafka生产者配置
   *
   * @return
   */
  def getKafkaProperties(): Properties = {
    val properties = new Properties()
//    properties.put("bootstrap.servers", KAFKA_QUORUM)
//    properties.put("retries", "2147483647")
//    properties.put("buffer.memory", "67108864")
//    properties.put("batch.size", "131072")
//    properties.put("linger.ms", "100")
//    properties.put("max.in.flight.requests.per.connection", "1")
//    properties.put("retry.backoff.ms", "100")
//    properties.put("max.request.size", "100000000")
//    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "")

    properties.put("bootstrap.servers", KAFKA_QUORUM)
    properties.put("retries", KAFKA_PROPERTIES_RETRIES)
    properties.put("buffer.memory", KAFKA_PROPERTIES_BUFFER_MEMORY)
    properties.put("batch.size", KAFKA_PROPERTIES_BATCH_SIZE)
    properties.put("linger.ms", KAFKA_PROPERTIES_LINGER_MS)
    properties.put("max.in.flight.requests.per.connection", KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
    properties.put("retry.backoff.ms", KAFKA_PROPERTIES_RETRY_BACKOFF_MS)
    properties.put("max.request.size", KAFKA_PROPERTIES_MAX_REQUEST_SIZE)
    properties.setProperty("transaction.timeout.ms", KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS)
//    properties.setProperty("compression.codec", KAFKA_PROPERTIES_COMPRESSION_TYPE)
    properties.put("compression.type", KAFKA_PROPERTIES_COMPRESSION_TYPE)

    properties
  }

  /**
   * kafka生产者配置
   *
   * @return
   */
  def getPiRunKafkaProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", PI_RUN_KAFKA_QUORUM)
//    properties.put("retries", "2147483647")
//    properties.put("buffer.memory", "67108864")
//    properties.put("batch.size", "131072")
//    properties.put("linger.ms", "100")
//    properties.put("max.in.flight.requests.per.connection", "1")
//    properties.put("retry.backoff.ms", "100")
//    properties.put("max.request.size", "100000000")
//    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "")

    properties.put("retries", KAFKA_PROPERTIES_RETRIES)
    properties.put("buffer.memory", KAFKA_PROPERTIES_BUFFER_MEMORY)
    properties.put("batch.size", KAFKA_PROPERTIES_BATCH_SIZE)
    properties.put("linger.ms", KAFKA_PROPERTIES_LINGER_MS)
    properties.put("max.in.flight.requests.per.connection", KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
    properties.put("retry.backoff.ms", KAFKA_PROPERTIES_RETRY_BACKOFF_MS)
    properties.put("max.request.size", KAFKA_PROPERTIES_MAX_REQUEST_SIZE)
    properties.setProperty("transaction.timeout.ms", KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS)
    properties.put("compression.type", KAFKA_PROPERTIES_COMPRESSION_TYPE)
    properties
  }

  /**
    * kafka生产者配置
    *
    * @return
    */
  def getDGKafkaProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", KAFKA_QUORUM_DG)
    properties.put("retries", "2147483647")
    properties.put("buffer.memory", "67108864")
    properties.put("batch.size", "131072")
    properties.put("linger.ms", "100")
    properties.put("max.in.flight.requests.per.connection", "1")
    properties.put("retry.backoff.ms", "100")
    properties.put("max.request.size", "100000000")
    properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "")
    properties.setProperty("compression.type", "gzip")
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


}