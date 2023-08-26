package com.hzw.fdc.util

/**
 * @author gdj
 * @create 2021-04-21-10:28
 *
 */
object MainFabConstants {

  // DEBUG 开关
  var IS_DEBUG = false

  //job name
  //Offline Job 6
  val OfflineCalculatedJob="MainFabOfflineCalculatedIndicatorApplication"
  val OfflineDriftJob="MainFabOfflineDriftApplication"
  val OfflineIndicatorJob="MainFabOfflineIndicatorApplication"
  val OfflineResultJob="MainFabOfflineResultApplication"
  val OfflineWindowJob="MainFabOfflineWindowApplication"
  val OfflineVirtualSensorJob="MainFabOfflineVirtualSensorApplication"
  val OfflineLogisticIndicatorJob="MainFabOfflineLogisticIndicatorApplication"
  val OfflineAutoLimitJob="MainFabOfflineAutoLimitApplication"
  //Oline job 4
  val IndicatorJob="MainFabIndicatorApplication"
  val LogisticIndicatorJob="MainFabLogisticIndicatorApplication"
  val WindowJob="MainFabWindowApplication"
  val CalculatedIndicatorJob="MainFabCalculatedIndicatorApplication"

  // 10
  val RouterJob="MainFabRouterApplication"
  val VersionTagJob="MainFabVersionTagApplication"
  val AlarmWriteResultDataJob="MainFabAlarmWriteResultDataApplication"
  val WriteAlarmHistoryJob="MainFabWriteAlarmHistoryApplication"
  val WriteHiveReportJob="MainFabWriteHiveReportApplication"
  val WriteRawDataJob="MainFabWriteRawDataApplication"
  val WriteRunData="MainFabWriteRunDataApplication"

  val MESFilter="MainFabMESFilterApplication"


  val AlarmJob="MainFabAlarmApplication"
  val AutoLimitJob="MainFabAutoLimitApplication"
  val DriftJob="MainFabDriftApplication"
  val FileJob="MainFabFileApplication"

  //bi_report
  val WriteRunDataToHDFS="MainFabWriteRunDataToHDFSApplication"
  val WriteDataFromKafkaToDg="MainFabWriteDataFromKafkaToDgApplication"
  val OfflineConsumerKafkaHistoryDataJob = "MainFabOfflineConsumerKafkaHistoryDataApplication"

  val ProcessEndWindowJob="MainFabProcessEndWindowApplication"
  val WindowEndWindowJob="MainFabWindowEndWindowApplication"
  val DataTransformWindowJob="MainFabDataTransformWindowApplication"

  val ProcessEndWindow2Job = "MainFabProcessEndWindow2Application"
  val WindowEndWindow2Job = "MainFabWindowEndWindow2Application"
  val WriteRedisDataJob = "MainFabWriteRedisDataApplication"


  //  val MAIN_FAB_INDICATOR_CONFIG_KAFKA_SOURCE_UID ="MainFabIndicatorJob_Config_kafkaSource"
  val MAIN_FAB_ALARM_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabAlarmJob_Indicator_Result_kafkaSource"

  val MAIN_FAB_OFFLINE_CALCULATED_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_Indicator_Result_kafkaSource"

  val MAIN_FAB_OFFLINE_CALCULATED_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedtJob_Indicator_Config_kafkaSource"


  val MAIN_FAB_CALCULATED_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabCalculatedJob_Indicator_Result_kafkaSource"

  val MAIN_FAB_CALCULATED_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabCalculatedtJob_Indicator_Config_kafkaSource"

  val MAIN_FAB_DRIFT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabDriftJob_Indicator_Result_kafkaSource"

  val MAIN_FAB_DRIFT_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabDriftJob_Indicator_Config_kafkaSource"

  val MAIN_FAB_HIVE_REPORT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabWriteHiveReportJob_Write_Hive_kafkaSource"

  val MAIN_FAB_RUN_DATA_JOB_KAFKA_SOURCE_UID = "MainFabWriteRunDataJob_Write_RunData_kafkaSource"
  val MAIN_FAB_WRITE_RAW_DARA_JOB_KAFKA_SOURCE_UID = "MainFabWriteRawDataJob_Write_RawData_kafkaSource"
  val MAIN_FAB_WRITE_ALARM_HISTORY_JOB_KAFKA_SOURCE_UID = "MainFabWriteRawDataJob_Write_AlarmHistory_kafkaSource"
  val MAIN_FAB_MES_FILTER_JOB_KAFKA_SOURCE_UID="MainFabMesFilterJob_kafkaSource"

  val MAIN_FAB_INDICATOR_HBASE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabWriteIndicatorJob_Write_Indicator_Hbase_kafkaSource"

  val MAIN_FAB_OFFLINE_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabOfflineJob_Indicator_Result_kafkaSource"

  val MAIN_FAB_OFFLINE_DRIFT_JOB_INDICATOR_RESULT_KAFKA_SOURCE_UID = "MainFabOfflineDriftJob_Indicator_Result_kafkaSource"

  val MAIN_FAB_OFFLINE_DRIFT_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineDriftJob_Indicator_Config_kafkaSource"

  val MAIN_FAB_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID = "MainFabAlarmJob_Alarm_Config_kafkaSource"
  val MAIN_FAB_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabAlarmJob_Alarm_Indicator_Config_kafkaSource"

  val MAIN_FAB_OFFLINE_WINDOW_TASK_KAFKA_SOURCE_UID = "MainFabOffline_Window_Task_kafkaSource"

  val MAIN_FAB_OFFLINE_VIRTUAL_SENSOR_TASK_KAFKA_SOURCE_UID = "MainFabOffline_Virtual_Sensor_Task_kafkaSource"

  val MAIN_FAB_OFFLINE_VIRTUAL_SENSOR_KAFKA_SOURCE_UID = "MainFabOffline_Virtual_Sensor_kafkaSource"

  val MAIN_FAB_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabIndicatorJob_Indicator_Config_kafkaSource"

  val MAIN_FAB_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID = "MainFabAutoLimitJob_Config_kafkaSource"
  val MAIN_FAB_AUTO_LIMIT_INDICATOR_KAFKA_SOURCE_UID = "MainFabAutoLimitJob_Indicator_kafkaSource"
  val MAIN_FAB_PT_DATA_KAFKA_SOURCE_UID = "MainFabWindowJob_PT_kafkaSource"
  val MAIN_FAB_AUTO_LIMIT_CONDITION_CONFIG_KAFKA_SOURCE_UID = "MainFabAutoLimitJob_LimitCondition_Config_kafkaSource"

  val MAIN_FAB_ROUTER_JOB_PT_DATA_KAFKA_SOURCE_UID = "MainFabRouterJob_PT_kafkaSource"
  val MAIN_FAB_ROUTER_JOB_VERSION_TAG_KAFKA_SOURCE_UID = "MainFabRouterJob_Version_Tag_kafkaSource"

  val MAIN_FAB_ROUTER_JOB_UP_DATA_KAFKA_SOURCE_UID = "MainFabRouterJob_UpData_kafkaSource"

  val MAIN_FAB_PM_KAFKA_SOURCE_UID = "MainFabWindowJob_PM_kafkaSource"


  val MAIN_FAB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowJob_window_Config_kafkaSource"
  val MAIN_FAB_ALARM_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowJob_alarmJob_window_Config_kafkaSource"
  val MAIN_FAB_RUN_ALARM_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabRunAlarmJob_Indicator_Config_kafkaSource"
  val MAIN_FAB_RUN_ALARM_JOB_ALARM_CONFIG_KAFKA_SOURCE_UID = "MainFabRunAlarmJob_Alarm_Config_kafkaSource"
  val MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowJob_Context_Config_kafkaSource"
  val MAIN_FAB_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowJob_Indicator_Config_kafkaSource"
  val MAIN_FAB_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowJob_Virtual_Sensor_Config_kafkaSource"
  val MAIN_FAB_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowJob_Debug_Config_kafkaSource"


  val MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabLogisticIndicatorJob_Indicator_Config_kafkaSource"
  val MAIN_FAB_LOGISTIC_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID = "MainFabLogisticIndicatorJob_Indicator_kafkaSource"

  val MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID = "MainFabBaseIndicatorJob_Indicator_kafkaSource"
  val MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabBaseIndicatorJob_Indicator_Config_kafkaSource"

  val MAIN_FAB_INDICATOR_FILE_KAFKA_SOURCE_UID = "MainFab_Indicator_File_kafkaSource"
  val MAIN_FAB_RAWTRACE_FILE_KAFKA_SOURCE_UID = "MainFab_RawTrace_File_kafkaSource"
  val MAIN_FAB_PT_DATA_FILE_KAFKA_SOURCE_UID = "MainFabWindowJob_PT_File_kafkaSource"
  val MAIN_FAB_OFFLINE_RESULT_KAFKA_SOURCE_UID = "MainFab_Offline_Result_kafkaSource"

  val MAIN_FAB_CHECK_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_Context_Config_kafkaSource"
  val MAIN_FAB_CEHCK_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_window_Config_kafkaSource"
  val MAIN_FAB_CHECK_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_Indicator_Config_kafkaSource"
  val MAIN_FAB_CHECK_ALARM_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_Alarm_Config_kafkaSource"
  val MAIN_FAB_CHECK_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_Virtual_Sensor_Config_kafkaSource"
  val MAIN_FAB_CHECK_AUTO_LIMIT_CONFIG_KAFKA_SOURCE_UID = "MainFabOfflineCalculatedJob_Auto_Limit_Config_kafkaSource"

  //bi_report
  val MAIN_FAB_RUN_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID = "MainFabWriteRunDataJob_Write_RunData_To_HDFS_kafkaSource"
  val MAIN_FAB_INDICATOR_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID = "MainFabWriteRunDataJob_Write_IndicatorData_To_HDFS_kafkaSource"
  val MAIN_FAB_PROCESSEND_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID = "MainFabWriteRunDataJob_Write_ProcessEnd_Log_HDFS_kafkaSource"
  val MAIN_FAB_WINDOWEND_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID = "MainFabWriteRunDataJob_Write_WindowEnd_Log_HDFS_kafkaSource"
  val MAIN_FAB_DATA_TRANSFORM_LOG_DATA_TO_HDFS_JOB_KAFKA_SOURCE_UID = "MainFabWriteRunDataJob_Write_DataTransform_Log_HDFS_kafkaSource"
  val MAIN_FAB_GET_SPLIT_WINDOW_FOR_TEST_SOURCE_UID = "MainFabWriteRunDataJob_Write_Split_Window_Result_For_Test_HDFS_kafkaSource"

  // consumerkafkahistorydata
  val MAIN_FAB_CONSUMER_KAFKA_HISTORY_DATA_SOURCE_UID = "MainFabOfflineConsumerKafkaHistoryData_kafkaSource"


  //DataTransformWindow Job
  val MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_PT_DATA_KAFKA_SOURCE_UID = "MainFabDataTransformWindowJob_pt_data_kafkaSource"
  val MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabDataTransformWindowJob_context_config_kafkaSource"
  val MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabDataTransformWindowJob_window_config_kafkaSource"

  //ProcessEndWindow Job
  val MAIN_FAB_PROCESSEND_JOB_RAWDATA_KAFKA_SOURCE_UID = "MainFabProcessEndWindowJob_rawdata_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindowJob_context_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindowJob_window_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindowJob_indicator_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindowJob_debug_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW_JOB_CONTROLPLAN_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindowJob_controlPlan_window_config_kafkaSource"


  //WindowEndWindow Job
  val MAIN_FAB_WINDOWEND_JOB_RAWDATA_KAFKA_SOURCE_UID = "MainFabWindowEndWindowJob_rawdata_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindowJob_context_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindowJob_window_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindowJob_indicator_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindowJob_debug_config_kafkaSource"

  //window2.0 ProcessEndWindow Job
  val MAIN_FAB_PROCESSEND_WINDOW2_JOB_RAWDATA_KAFKA_SOURCE_UID = "MainFabProcessEndWindow2Job_rawdata_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW2_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindow2Job_context_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW2_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindow2Job_indicator_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW2_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindow2Job_debug_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW2_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindow2Job_controlPlan_config_kafkaSource"
  val MAIN_FAB_PROCESSEND_WINDOW2_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabProcessEndWindow2Job_window_config_kafkaSource"

  //window2.0 WindowEndWindow Job
  val MAIN_FAB_WINDOWEND_WINDOW2_JOB_RAWDATA_KAFKA_SOURCE_UID = "MainFabWindowEndWindow2Job_rawdata_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW2_JOB_CONTEXT_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindow2Job_context_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW2_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindow2Job_indicator_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW2_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindow2Job_debug_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW2_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindow2Job_controlPlan_config_kafkaSource"
  val MAIN_FAB_WINDOWEND_WINDOW2_JOB_WINDOW_CONFIG_KAFKA_SOURCE_UID = "MainFabWindowEndWindow2Job_window_config_kafkaSource"


  val MAIN_FAB_WRITE_REDIS_DATA_KAFKA_SOURCE_UID = "MainFabWriteRedisJob_redis_data_kafkaSource"

  //判断数据类型
  val dataType = "dataType"

  val eventStart = "eventStart"

  val eventEnd = "eventEnd"

  val rawData = "rawData"

  val PM = "PM"

  val indicatorResult = "indicatorResult"

  val micAlarm = "micAlarm"

  val toolName = "toolName"
  val chamberName = "chamberName"
  val timestamp = "timestamp"
  val data = "data"
  val sensorName = "sensorName"
  val sensorAlias = "sensorAlias"
  val sensorValue = "sensorValue"
  val unit = "unit"
  val indicatorId = "indicatorId"

  val traceId = "traceId"

  val context = "context"

  val runwindow = "runwindow"

  val config = "config"

  val indicatorConfig="indicatorconfig"
  val autoLimit_config="autoLimit_config"

  val controlPlanWindowConfig="controlplan_window_config"

  val virtualSensorConfig = "virtualSensorConfig"
  //task name
  val OpenTSDBSink = "RawData To OpenTSDB Sink"
  val KafkaWindowSink = "Kafka Sink To Window"
  val HbaseRunDataSink = "RunData Hbase Sink"
  val KafkaMESSink = "Kafka MES Sink"
  val KafkaToolMessageSink = "Kafka ToolMessage Sink"
  val KafkaOfflineResultSink = "Kafka offline result Sink"
  val KafkaOfflineIndicatorSink = "Kafka offline indicator value Sink"
  val KafkaIndicatorFileSink = "Kafka indicator file Sink"
  val KafkaDataMissingRatioSink = "Kafka DataMissingRatio Sink"
  val KafkaRouterSink = "Kafka Router Sink"
  val KafkaRouterPiRunSink = "Kafka Router PiRun Sink"
  val KafkaRunDataSink = "Kafka RunData Sink"
  val KafkaRawDataSink = "Kafka RawData Sink"

  val KafkaWindowendWindowSink = "windowend window sink to kafka"
  val KafkaProcessendWindowSink = "processend window sink to kafka"
  val KafkaDataTransformWindowSink = "data transform window sink to kafka"


  val KafkaLogisticIndicatorSink = "Kafka LogisticIndicator Sink"

  val WindowTask = "Window Task"
  val AutoLimitByTimeWindow = "AutoLimitByTimeWindow"

  val dataMissingRatio = "dataMissingRatio"
  val cycleCountIndicator = "cycleCount"

  val CycleWindowMaxType = "CycleWindowMax"
  val CycleWindowMinType = "CycleWindowMin"

  val triggerMethodNameByCount = "By Count"
  val triggerMethodNameByTime = "By Time"

  val NotApplicable = "N/A"

  val ProcessEnd = "ProcessEnd"
  val WindowEnd = "WindowEnd"

  val pmStart = "start"
  val pmNone = "none"
  val pmEnd = "end"

  val dataVersion = "dataVersion"

  val notification= "notification"
  val action=  "action"
  val earliest=  "earliest"
  val latest=  "latest"

  val MESMessage="MESMessage"
  val toolMessage="toolMessage"

  val errorCode = "errorCode"


  val controlPlanConfig="controlPlanConfig"
  val controlPlanConfig2="controlPlanConfig2"
  val window2Config="window2Config"

  val windowTypeProcessEnd = "processEnd"
  val windowTypeWindowEnd = "windowEnd"


  val runStartTime = "runStartTime"
  val runEndTime = "runEndTime"
}
