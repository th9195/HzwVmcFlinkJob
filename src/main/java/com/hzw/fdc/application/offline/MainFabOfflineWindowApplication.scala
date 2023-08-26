package com.hzw.fdc.application.offline

import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.MainFabOfflineWindowController
import com.hzw.fdc.util.{LocalPropertiesConfig, MainFabConstants, ProjectConfig}
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.java.utils.ParameterTool

/**
 * @author ：gdj
 * @date ：Created in 2021/10/19 13:59
 * @description：
 * @modified By：
 * @version: $
 */
object MainFabOfflineWindowApplication extends App with TApplication {

  // todo 1- 获取配置文件的路径
  if (SystemUtils.IS_OS_WINDOWS) {  // 本地调试

    ProjectConfig.PROPERTIES_FILE_PATH = LocalPropertiesConfig.config.getProperty("config_path")

    println(s"config_path == ${ProjectConfig.PROPERTIES_FILE_PATH}")

    MainFabConstants.IS_DEBUG = true

  } else{
    //启动Flink任务时，需要添加参数config_path  举例: flink run FdcETL.jar -config_path /FdcETL.properties
    val parameters = ParameterTool.fromArgs(args)
    ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  }


  start({
    val controller = new MainFabOfflineWindowController
    controller.execute()

  },jobName =  "MainFabOfflineWindowApplication")
}
