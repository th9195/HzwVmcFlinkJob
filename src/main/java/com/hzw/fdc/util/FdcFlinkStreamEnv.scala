package com.hzw.fdc.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * FdcFlinkStreamEnv
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/14 9:09
 * @since 1.0.0
 * @update 2022/12/14 9:09
 * */
object FdcFlinkStreamEnv {
  private val fdcEnvLocal = new ThreadLocal[StreamExecutionEnvironment]

  def init(jobName: String) = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    // 注册全局变量
    val parameters = FdcProjectConfig.initConfig()
    env.getConfig.setGlobalJobParameters(parameters)

    jobName match {
      case MainFabConstants.ProcessEndWindow2Job =>{
        env.setParallelism(FdcProjectConfig.SET_PARALLELISM)
        enableCheckpoint(env,
          FdcProjectConfig.CHECKPOINT_ENABLE,
          FdcProjectConfig.CHECKPOINT_STATE_BACKEND,
          FdcProjectConfig.CHECKPOINT_INTERVAL,
          FdcProjectConfig.CHECKPOINT_TIME_OUT)
      }

      case MainFabConstants.WindowEndWindow2Job =>{
        env.setParallelism(FdcProjectConfig.SET_PARALLELISM)
        enableCheckpoint(env,
          FdcProjectConfig.CHECKPOINT_ENABLE,
          FdcProjectConfig.CHECKPOINT_STATE_BACKEND,
          FdcProjectConfig.CHECKPOINT_INTERVAL,
          FdcProjectConfig.CHECKPOINT_TIME_OUT)
      }

      case _ => FlinkStreamEnv.get().setParallelism(1)
        enableCheckpoint(env,false)
    }


    fdcEnvLocal.set(env)


    env
  }


  /**
   * 设置checkPoint
   * @param isEnable
   * @param env
   * @param stateBackend
   * @param checkPointIntervalTime
   * @param checkPointTimeOut
   * @param twoCheckPointIntervalTime
   * @param maxCheckPointNum
   * @param checkPointingMode
   * @param restartFailureRate
   * @param restartMinuteTime
   * @param restartIntervalSecondsTime
   */
  def enableCheckpoint(env: StreamExecutionEnvironment,
                       isEnable: Boolean,
                       stateBackend: String="rocksdb",
                       checkPointIntervalTime:Int = 90000,
                       checkPointTimeOut:Int = 90000,
                       twoCheckPointIntervalTime:Int = 10000,
                       maxCheckPointNum:Int = 1,
                       checkPointingMode: CheckpointingMode = CheckpointingMode.AT_LEAST_ONCE,
                       restartFailureRate:Int = 2,
                       restartMinuteTime:Int = 30,
                       restartIntervalSecondsTime:Int = 5): Unit = {
    if (isEnable) {
      setFdcStateBackend(isEnable, env, stateBackend)
      env.enableCheckpointing(checkPointIntervalTime)
      env.getCheckpointConfig.setCheckpointTimeout(checkPointTimeOut)

      // 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(twoCheckPointIntervalTime)

      // 默认情况下，只有一个检查点可以运行
      // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(maxCheckPointNum)
      env.getCheckpointConfig.setCheckpointingMode(checkPointingMode)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // 设置尝试重启的次数, 10分钟内,最大失败次数为2,重启时间间隔5秒
    env.setRestartStrategy(RestartStrategies.failureRateRestart(restartFailureRate,
      Time.minutes(restartMinuteTime), Time.seconds(restartIntervalSecondsTime)))
  }

  /**
   *  获取checkpoint的存储类型
   */
  def setFdcStateBackend(isEnable:Boolean, env: StreamExecutionEnvironment, stateBackend: String): Unit = {
    if(stateBackend == "rocksdb"){
      env.setStateBackend(new RocksDBStateBackend(FdcProjectConfig.ROCKSDB_HDFS_PATH, isEnable))
    }else if(stateBackend == "filesystem"){
      env.setStateBackend(new FsStateBackend(FdcProjectConfig.FILESYSTEM_HDFS_PATH, isEnable))
    }
  }


  /**
   * 获取环境
   */
  def get() = {
    val env = fdcEnvLocal.get()
    env
  }

  def clear(): Unit = {
    fdcEnvLocal.remove()
  }

  /**
   * 执行环境
   */
  def execute(jobName: String): Unit = {
    get().execute(jobName)
  }

}
