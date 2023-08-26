package com.hzw.fdc.common

import com.hzw.fdc.util.FdcFlinkStreamEnv

/**
 * TFdcApplication
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/14 9:07
 * @since 1.0.0
 * @update 2022/12/14 9:07
 * */
trait TFdcApplication {
  def start(op: => Unit, jobName: String): Unit = {
    try {
      // 初始化Flink的运行环境
      FdcFlinkStreamEnv.init(jobName)
      op
      // 执行Flink环境
      FdcFlinkStreamEnv.execute(jobName)
    } catch {
      case e:Exception=> e.printStackTrace()
    } finally {
      FdcFlinkStreamEnv.clear()
    }
  }
}
