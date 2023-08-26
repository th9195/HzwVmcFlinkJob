package com.hzw.fdc.application.online

import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.MainFabWindowEndWindow2Controller
import com.hzw.fdc.util.{LocalPropertiesConfig, MainFabConstants, ProjectConfig}
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.java.utils.ParameterTool

/**
 * MainFabLongRunWindowEndWindowApplication
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/13 14:58
 * @since 1.0.0
 * @update 2022/12/13 14:58
 * */
object MainFabWindowEndWindow2Application extends App with TApplication {


  // todo 1- 获取配置文件的路径
  if (SystemUtils.IS_OS_WINDOWS) {  // 本地调试

    ProjectConfig.PROPERTIES_FILE_PATH = LocalPropertiesConfig.config.getProperty("config_path")

    println(s"config_path == ${ProjectConfig.PROPERTIES_FILE_PATH}")

    MainFabConstants.IS_DEBUG = true

  } else {   // Linux集群环境

    //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
    val parameters = ParameterTool.fromArgs(args)
    ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  }
  start({
    val controller = new MainFabWindowEndWindow2Controller
    controller.execute()

  },jobName =  MainFabConstants.WindowEndWindow2Job)

}
