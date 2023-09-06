package com.hzw.fdc.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
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

      case VmcConstants.VmcAllJob =>{
        env.setParallelism(ProjectConfig.SET_PARALLELISM_VMC_ALL_JOB)

        setCheckpoint(env,
          isEnable = ProjectConfig.CHECKPOINT_ENABLE_VMC_ALL_JOB,
          stateBackend = ProjectConfig.CHECKPOINT_STATE_BACKEND_VMC_ALL_JOB,
          checkpointInterval = ProjectConfig.CHECKPOINT_INTERVAL_VMC_ALL_JOB,
          checkpointTimeout = ProjectConfig.CHECKPOINT_TIME_OUT_VMC_ALL_JOB)
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
