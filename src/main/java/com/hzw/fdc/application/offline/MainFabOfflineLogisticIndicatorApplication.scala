package com.hzw.fdc.application.offline

import com.hzw.fdc.common.TApplication
import com.hzw.fdc.controller.{MainFabLogisticIndicatorController, MainFabOfflineLogisticIndicatorController}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool

/**
 * @author ：gdj
 * @date ：Created in 2021/7/21 11:18
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
object MainFabOfflineLogisticIndicatorApplication extends App with TApplication {
  //启动Flink任务时，需要添加参数config_path  举例: flink run FdcETL.jar -config_path /FdcETL.properties
  val parameters = ParameterTool.fromArgs(args)
  ProjectConfig.PROPERTIES_FILE_PATH = parameters.get("config_path")

  start({
    val controller = new MainFabOfflineLogisticIndicatorController
    controller.execute()

  }, jobName = "MainFabOfflineLogisticIndicatorApplication")
}
