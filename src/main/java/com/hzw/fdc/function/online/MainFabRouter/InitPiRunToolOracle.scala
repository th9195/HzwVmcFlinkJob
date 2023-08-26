package com.hzw.fdc.function.online.MainFabRouter

import com.hzw.fdc.util.ProjectConfig
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import scala.collection.mutable.ListBuffer

object InitPiRunToolOracle {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRouterSplitPiRunKeyedBroadcastProcessFunction])

//  lazy val OracleConfigList: ListBuffer[String] = initOracleConfig()

  // 初始化Oracle配置
  def initOracleConfig(): ListBuffer[String] = {

    Class.forName("oracle.jdbc.driver.OracleDriver")
    val Conn: Connection = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL,
      ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)

    val toolConfigDataByAll =  ListBuffer[String]()
    logger.warn(s"Oracle indicatorConfig start \t ${ProjectConfig.MAIN_FAB_CORE_ORACLE_URL} \t " +
      s" ${ProjectConfig.MAIN_FAB_CORE_ORACLE_USER} \t ${ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD}!!!")

    try {

      val piRunTool = Conn.prepareStatement("select TOOL_NAME from fdc_conf_tool where PIRUN_STATUS='ENABLED'")
      val piRunToolResult = piRunTool.executeQuery()
      while (piRunToolResult.next()) {
        toolConfigDataByAll.append(
          piRunToolResult.getString("TOOL_NAME")
        )
      }

      logger.warn("Oracle toolPiRunConfigDataByAll SIZE :" + toolConfigDataByAll.size)
    } catch {
      case e: Exception => logger.warn("indicator load error" + e)
    } finally {
      if (Conn != null) {
        Conn.close()
      }
    }
    toolConfigDataByAll
  }
}

