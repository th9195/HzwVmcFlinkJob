package com.hzw.fdc.util

import com.hzw.fdc.util.FlinkStreamEnv.EnableCheckpoint
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._

/**
 * @author gdj
 * @create 2020-05-25-17:08
 *
 */
object FlinkStreamEnv {
  private val envLocal = new ThreadLocal[StreamExecutionEnvironment]

  /**
   * 环境初始化
   */
  def init(jobName: String) = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment


    // 注册全局变量
    val parameters = ProjectConfig.initConfig()
    env.getConfig.setGlobalJobParameters(parameters)


    jobName match {

      //AB流
      case MainFabConstants.IndicatorJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_INDICATOR_JOB, env)
      }
      case MainFabConstants.LogisticIndicatorJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_LOGISTIC_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_LOGISTIC_INDICATOR_JOB, env)
      }
      case MainFabConstants.WindowJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WINDOW_JOB)
        WindowEnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WINDOW_JOB, env)
      }
      case MainFabConstants.CalculatedIndicatorJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_CALCULATED_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_CALCULATED_INDICATOR_JOB, env)
      }
      case MainFabConstants.DriftJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_DRIFT_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_DRIFT_INDICATOR_JOB, env)
      }


      //在线 phase4新增
      case MainFabConstants.RouterJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_ROUTER_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_ROUTER_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_ROUTER_JOB)
      }
      case MainFabConstants.VersionTagJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_VERSION_TAG_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_VERSION_TAG_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_VERSION_TAG_JOB)
      }
      case MainFabConstants.AlarmWriteResultDataJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_ALARM_WRITE_RESULT_DATA_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_ALARM_WRITE_RESULT_DATA_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_ALARM_WRITE_RESULT_JOB)
      }
      case MainFabConstants.WriteAlarmHistoryJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_ALARM_HISTORY_JOB)
        AlarmHistoryEnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_ALARM_HISTORY_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_ALARM_HISTORY_JOB)
      }
      case MainFabConstants.WriteHiveReportJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_HIVE_REPORT_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_HIVE_REPORT_JOB, env)
      }
      case MainFabConstants.WriteRawDataJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_RAW_DATA_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_RAW_DATA_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_RAW_DATA_JOB)
      }
      case MainFabConstants.WriteRunData => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_RUN_DATA_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_RUN_DATA_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_RUN_ATA_JOB)
      }
      case MainFabConstants.MESFilter => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_MES_FILTER_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_MES_FILTER_JOB, env)
      }

      // phase4之前
      case MainFabConstants.AlarmJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_ALARM_JOB)
        AlarmEnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_ALARM_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_ALARM_JOB)
      }
      case MainFabConstants.AutoLimitJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_AUTO_LIMIT_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_AUTO_LIMIT_JOB, env)
      }
      case MainFabConstants.FileJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_FILE_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_FILE_JOB, env)
      }

      //离线
      case MainFabConstants.OfflineCalculatedJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_CALCULATED_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_CALCULATED_INDICATOR_JOB, env)
      }
      case MainFabConstants.OfflineDriftJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_DRIFT_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_DRIFT_INDICATOR_JOB, env)
      }
      case MainFabConstants.OfflineIndicatorJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_INDICATOR_JOB, env)
      }
      case MainFabConstants.OfflineResultJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_RESULT_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_RESULT_JOB, env)
      }
      case MainFabConstants.OfflineWindowJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_WINDOW_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_WINDOW_JOB, env)
      }
      case MainFabConstants.OfflineVirtualSensorJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_VIRTUAL_SENSOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_WINDOW_JOB, env)
      }
      case MainFabConstants.OfflineLogisticIndicatorJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_LOGISTIC_INDICATOR_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_LOGISTIC_INDICATOR_JOB, env)
      }
      case MainFabConstants.OfflineAutoLimitJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_AUTOLIMIT_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_AUTOLIMIT_JOB, env)
      }

      //bi_report
      case MainFabConstants.WriteRunDataToHDFS =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_RUN_DATA_TO_HDFS_JOB)
        Bi_Report_To_HDFS_EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_RUN_DATA_TO_HDFS_JOB,env)
      }

      case MainFabConstants.WriteDataFromKafkaToDg =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_DATA_FROM_KAFKA_TO_DG_JOB)
        Bi_Report_To_HDFS_EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_RUN_DATA_TO_HDFS_JOB,env)
      }

      case MainFabConstants.OfflineConsumerKafkaHistoryDataJob => {
        env.setParallelism(ProjectConfig.SET_PARALLELISM_OFFLINE_CONSUMER_KAFKA_HISTORY_DATA_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_OFFLINE_CONSUMER_KAFKA_HISTORY_DATA_JOB, env)
      }

      case MainFabConstants.ProcessEndWindowJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_PROCESSEND_WINDOW_JOB)
        ProcessEndWindowEnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_PROCESSEND_WINDOW_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW_JOB)
      }

      case MainFabConstants.WindowEndWindowJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WINDOWEND_WINDOW_JOB)
        WindowEnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WINDOWEND_WINDOW_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW_JOB)
      }

      case MainFabConstants.DataTransformWindowJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_DATA_TRANSFORM_WINDOW_JOB)
        EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_DATA_TRANSFORM_WINDOW_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_DATA_TRANSFORM_WINDOW_JOB)
      }

      case MainFabConstants.ProcessEndWindow2Job =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_PROCESSEND_WINDOW2_JOB)
        ProcessEndWindow2EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_PROCESSEND_WINDOW2_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_PROCESSEND_WINDOW2_JOB)
      }

      case MainFabConstants.WindowEndWindow2Job =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WINDOWEND_WINDOW2_JOB)
        WindowEndWindow2EnableCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WINDOWEND_WINDOW2_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_WINDOWEND_WINDOW2_JOB)
      }
      case MainFabConstants.WriteRedisDataJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_WRITE_REDIS_DATA_JOB)
        WriteRedisDataCheckpoint(ProjectConfig.CHECKPOINT_ENABLE_WRITE_REDIS_DATA_JOB, env, ProjectConfig.CHECKPOINT_STATE_BACKEND_WRITE_REDIS_DATA_JOB)
      }

      case VmcConstants.VmcETLJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_VMC_ETL_JOB)

        setCheckpoint(env,
          isEnable = ProjectConfig.CHECKPOINT_ENABLE_VMC_ETL_JOB,
          stateBackend = ProjectConfig.CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB,
          checkpointInterval = ProjectConfig.CHECKPOINT_INTERVAL_VMC_ETL_JOB,
          checkpointTimeout = ProjectConfig.CHECKPOINT_TIME_OUT_VMC_ETL_JOB)
      }

      case VmcConstants.VmcWindowJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_VMC_WINDOW_JOB)

        setCheckpoint(env,
          isEnable = ProjectConfig.CHECKPOINT_ENABLE_VMC_WINDOW_JOB,
          stateBackend = ProjectConfig.CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB,
          checkpointInterval = ProjectConfig.CHECKPOINT_INTERVAL_VMC_WINDOW_JOB,
          checkpointTimeout = ProjectConfig.CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB)
      }

      case VmcConstants.VmcIndicatorJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_VMC_INDICATOR_JOB)

        setCheckpoint(env,
          isEnable = ProjectConfig.CHECKPOINT_ENABLE_VMC_INDICATOR_JOB,
          stateBackend = ProjectConfig.CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB,
          checkpointInterval = ProjectConfig.CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB,
          checkpointTimeout = ProjectConfig.CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB)
      }



      case _ => FlinkStreamEnv.get().setParallelism(1)
        EnableCheckpoint(true, env)
    }
    envLocal.set(env)
    env
  }

  /**
   * 获取环境
   */
  def get() = {
    val env = envLocal.get()
    env
  }

  def clear(): Unit = {
    envLocal.remove()
  }

  /**
   * 执行环境
   */
  def execute(jobName: String): Unit = {
    get().execute(jobName)
  }

  def WindowEnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_WINDOWEND_WINDOW_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }

  def EnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_COMMON_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_COMMON_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(30), Time.seconds(5)))
  }

  def AlarmEnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_ALARM_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_ALARM_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }

  def AlarmHistoryEnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_ALARM_HISTORY_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_ALARM_HISTORY_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }

  def Bi_Report_To_HDFS_EnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_BI_REPORT_TO_HDFS_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_BI_REPORT_TO_HDFS_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(30), Time.seconds(5)))
  }

  def ProcessEndWindowEnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {
    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_PROCESSEND_WINDOW_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }

  /**
   *  获取checkpoint的存储类型
   */
  def getStateBackendFunction(isEnable:Boolean, env: StreamExecutionEnvironment, stateBackend: String): Unit = {
      if(stateBackend == "rocksdb"){
        env.setStateBackend(new RocksDBStateBackend(ProjectConfig.ROCKSDB_HDFS_PATH, isEnable))
      }else if(stateBackend == "filesystem"){
        env.setStateBackend(new FsStateBackend(ProjectConfig.FILESYSTEM_HDFS_PATH, isEnable))
      }
  }


  /**
   * 设置 processEndWindow2 checkpoint
   * @param isEnable
   * @param env
   * @param stateBackend
   */
  def ProcessEndWindow2EnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {
    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_PROCESSEND_WINDOW2_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_PROCESSEND_WINDOW2_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }


  /**
   * 设置windowEndwindow2 checkpoint
   * @param isEnable
   * @param env
   * @param stateBackend
   */
  def WindowEndWindow2EnableCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_WINDOWEND_WINDOW2_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_WINDOWEND_WINDOW2_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }


  /**
   * 设置windowEndwindow2 checkpoint
   * @param isEnable
   * @param env
   * @param stateBackend
   */
  def WriteRedisDataCheckpoint(isEnable: Boolean, env: StreamExecutionEnvironment, stateBackend: String="rocksdb"): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(ProjectConfig.CHECKPOINT_INTERVAL_WRITE_REDIS_DATA_JOB)
      env.getCheckpointConfig.setCheckpointTimeout(ProjectConfig.CHECKPOINT_TIME_OUT_WRITE_REDIS_DATA_JOB)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2,
      Time.minutes(60), Time.seconds(5)))
  }


  /**
   *
   * @param env
   * @param isEnable                      默认: flase
   * @param stateBackend                  默认: rocksdb
   * @param checkpointInterval            默认: 60000
   * @param checkpointTimeout             默认: 600000
   * @param minPauseBetweenCheckpoints    默认: 5000
   * @param maxConcurrentCheckpoints      默认: 1
   * @param checkpointingMode             默认: CheckpointingMode.AT_LEAST_ONCE
   * @param failureRate                   默认: 2
   * @param failureInterval               默认: 30
   * @param delayInterval                 默认: 5
   */
  def setCheckpoint(env: StreamExecutionEnvironment,
                    isEnable: Boolean = false,
                    stateBackend: String="rocksdb",
                    checkpointInterval:Long=60000,
                    checkpointTimeout:Long=600000,
                    minPauseBetweenCheckpoints:Long = 5000,
                    maxConcurrentCheckpoints:Int = 1,
                    checkpointingMode: CheckpointingMode = CheckpointingMode.AT_LEAST_ONCE,
                    failureRate:Int = 2,
                    failureInterval:Long = 30,
                    delayInterval:Long = 5): Unit = {

    if (isEnable) {
      getStateBackendFunction(isEnable, env, stateBackend)
      env.enableCheckpointing(checkpointInterval)
      env.getCheckpointConfig.setCheckpointTimeout(checkpointTimeout)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints)
      env.getCheckpointConfig.setCheckpointingMode(checkpointingMode)
    }

    // 设置尝试重启的次数, 30分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(failureRate,
      Time.minutes(failureInterval), Time.seconds(delayInterval)))
  }

}
