package com.hzw.fdc.application.Vmc

import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.Vmc.VmcAllController
import com.hzw.fdc.util.{LocalPropertiesConfig, ProjectConfig, VmcConstants}
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.java.utils.ParameterTool

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcIndicatorApplication
 */
object VmcAllApplication extends App with TApplication {

  // todo 1- 获取配置文件的路径
  if (SystemUtils.IS_OS_WINDOWS) {  // 本地调试

    ProjectConfig.PROPERTIES_FILE_PATH = LocalPropertiesConfig.config.getProperty("config_path")

    println(s"config_path == ${ProjectConfig.PROPERTIES_FILE_PATH}")

    VmcConstants.IS_DEBUG = true

  } else {   // Linux集群环境

    //启动Flink任务时，需要添加参数config_path  举例: flink run FdcIndicator.jar -config_path /FdcIndicator.properties
    val parameters = ParameterTool.fromArgs(args)
    ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  }


  start({
    val controller = new VmcAllController
    controller.execute()
  }, jobName = VmcConstants.VmcAllJob)

}
