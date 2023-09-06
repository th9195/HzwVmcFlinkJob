package com.hzw.fdc.util

/**
 *
 * @author tanghui
 * @date 2023/8/26 14:57
 * @description VmcConstants
 */
object VmcConstants {
  // DEBUG 开关
  var IS_DEBUG = false

  val CONFIG = "config"

  val DATA_TYPE = "dataType"
  val TRACE_ID = "traceId"
  val TOOL_NAME = "toolName"
  val CHAMBER_NAME = "chamberName"

  val earliest=  "earliest"
  val latest=  "latest"


  val VMC_CONTROLPLAN_CONFIG = "vmcControlPlanConfig"



  val VmcETLJob="VmcETLApplication"
  val VmcWindowJob="VmcWindowApplication"
  val VmcIndicatorJob="VmcIndicatorApplication"

  val VMC_ETL_JOB_KAFKA_SOURCE_UID = "vmc_etl_kafkaSource"
  val VMC_WINDOW_JOB_KAFKA_SOURCE_UID = "vmc_window_kafkaSource"
  val VMC_INDICATOR_JOB_KAFKA_SOURCE_UID = "vmc_indicator_kafkaSource"

  val VMC_ETL_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID = "vmc_etl_job_controlPlan_config_kafkaSource"
  val VMC_WINDOW_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID = "vmc_window_job_controlPlan_config_kafkaSource"
  val VMC_INDICATOR_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID = "vmc_indicator_job_controlPlan_config_kafkaSource"


}
