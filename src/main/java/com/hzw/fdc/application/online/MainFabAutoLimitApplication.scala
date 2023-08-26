package com.hzw.fdc.application.online

import com.hzw.fdc.application.online.MainFabAlarmApplication.args
import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.MainFabAutoLimitController
import com.hzw.fdc.scalabean.RULE_CONSTANT
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.java.utils.ParameterTool

import java.util.{Locale, ResourceBundle}

/**
 * @author ：gdj
 * @date ：Created in 2021/6/8 15:43
 * @description：job入口
 * @modified By：
 * @version: $version$
 */
object MainFabAutoLimitApplication extends App with TApplication {

  //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
//  val parameters = ParameterTool.fromArgs(args)
//  ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")


  // todo 1- 获取配置文件的路径
  if (SystemUtils.IS_OS_WINDOWS) {  // 本地调试

    val createResourceBundle: ResourceBundle = ResourceBundle.getBundle("application", new Locale("zh", "CN"))
    ProjectConfig.PROPERTIES_FILE_PATH = createResourceBundle.getString("config_path")

    println(s"config_path == ${ProjectConfig.PROPERTIES_FILE_PATH}")

    MainFabConstants.IS_DEBUG = true

  } else {   // Linux集群环境

    //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
    val parameters = ParameterTool.fromArgs(args)
    ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  }


  start({
    val controller = new MainFabAutoLimitController
    controller.execute()

  },jobName =  "MainFabAutoLimitApplication")
}
