package com.hzw.fdc.application.online

import com.hzw.fdc.application.online.MainFabProcessEndWindowApplication.{args, start}
import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.{MainFabProcessEndWindowController, MainFabWindowEndWindowController}
import com.hzw.fdc.scalabean.RULE_CONSTANT
import com.hzw.fdc.util.{LocalPropertiesConfig, MainFabConstants, ProjectConfig}
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.java.utils.ParameterTool

/**
  * @Package com.hzw.fdc.application.online
  * @author wanghb
  * @date 2022-11-14 10:33
  * @desc
  * @version V1.0
  */
object MainFabWindowEndWindowApplication extends App with TApplication{


  // todo 1- 获取配置文件的路径
  if (SystemUtils.IS_OS_WINDOWS) {  // 本地调试

    LocalPropertiesConfig.config.getProperty("config_path")

    println(s"config_path == ${ProjectConfig.PROPERTIES_FILE_PATH}")

    MainFabConstants.IS_DEBUG = true

  } else {   // Linux集群环境

    //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
    val parameters = ParameterTool.fromArgs(args)
    ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  }
  start({
    val controller = new MainFabWindowEndWindowController
    controller.execute()

  },jobName =  "MainFabWindowEndWindowApplication")
}
