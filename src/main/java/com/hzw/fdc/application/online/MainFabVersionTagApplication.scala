package com.hzw.fdc.application.online

import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.MainFabVersionTagController
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool

object MainFabVersionTagApplication extends App with TApplication{

  //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
  val parameters = ParameterTool.fromArgs(args)
  ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  start({
    val controller = new MainFabVersionTagController
    controller.execute()

  }, jobName = "MainFabVersionTagApplication")
}
