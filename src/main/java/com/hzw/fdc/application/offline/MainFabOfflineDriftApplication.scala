package com.hzw.fdc.application.offline

import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.MainFabOfflineDriftController
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool

/**
 * @author ：gdj
 * @date ：Created in 2021/10/19 13:59
 * @description：
 * @modified By：
 * @version: $
 */
object MainFabOfflineDriftApplication extends App with TApplication {

  //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
  val parameters = ParameterTool.fromArgs(args)
  ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  start({
    val controller = new MainFabOfflineDriftController
    controller.execute()
  },jobName =  "MainFabOfflineDriftApplication")
}
