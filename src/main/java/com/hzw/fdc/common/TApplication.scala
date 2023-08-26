package com.hzw.fdc.common

import com.hzw.fdc.util.FlinkStreamEnv

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description TApplication
 */
trait TApplication {
  def start(op: => Unit, jobName: String): Unit = {
    try {
      // 初始化Flink的运行环境
      FlinkStreamEnv.init(jobName)
      op
      // 执行Flink环境
      FlinkStreamEnv.execute(jobName)
    } catch {
      case e:Exception=> e.printStackTrace()
    } finally {
      FlinkStreamEnv.clear()
    }
  }
}
