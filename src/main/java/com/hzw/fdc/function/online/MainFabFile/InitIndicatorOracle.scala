//package com.hzw.fdc.function.online.MainFabFile
//
//
//import com.hzw.fdc.scalabean._
//import com.hzw.fdc.util.ProjectConfig
//import org.slf4j.Logger
//
//import java.sql.DriverManager
//import scala.collection.mutable.ListBuffer
//
//
//object InitIndicatorOracle {
//
//  def initOracleConfig(logger: Logger): ListBuffer[ConfigData[IndicatorConfig]] = {
//
//    val indicatorConfigDataByAll = ListBuffer[ConfigData[IndicatorConfig]]()
//
//    Class.forName("oracle.jdbc.driver.OracleDriver")
//    val poList = new ListBuffer[ByPassConditionPO]()
//    val conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL, ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)
//    try {
//      val byPass = conn.prepareStatement(
//        """
//          |select      OBC.*
//          |                    , FCSOB.ID AS SINGLE_OCAP_ID
//          |                    , FCSOB.ALARM_BYPASS_CONDITION_ID
//          |                    , FCABC.CODE
//          |                    , FCABC.NAME
//          |                    , FCABC.HAS_PARAM
//          |                    , FCOBP.SINGLE_OCAP_BYPASS_ID
//          |                    , FCOBP.PARAM_INDEX
//          |                    , FCOBP.PARAM_VALUE
//          |        FROM        FDC_CONF_OCAP_BYPASS_CONDITION OBC
//          |        LEFT JOIN   FDC_CONF_SINGLE_OCAP_BYPASS FCSOB
//          |        ON          OBC.ID = FCSOB.BYPASS_CONDITION_ID
//          |        AND         FCSOB.STATUS = 'ENABLED'
//          |        LEFT JOIN   FDC_CONF_ALARM_BYPASS_CONDITION FCABC
//          |        ON          FCSOB.ALARM_BYPASS_CONDITION_ID = FCABC.ID
//          |        LEFT JOIN   FDC_CONF_OCAP_BYPASS_PARAMS FCOBP
//          |        ON          FCSOB.ID = FCOBP.SINGLE_OCAP_BYPASS_ID
//          |        WHERE       OBC.STATUS = 'ENABLED'
//          |""".stripMargin
//      )
//      val byPassResult = byPass.executeQuery()
//      while (byPassResult.next()) {
//        poList.append(ByPassConditionPO(
//          byPassResult.getLong("ID"),
//          byPassResult.getString("BYPASS_NAME"),
//          byPassResult.getBoolean("NOTIFICATION_SWITCH"),
//          byPassResult.getBoolean("ACTION_SWITCH"),
//          byPassResult.getLong("SINGLE_OCAP_ID"),
//          byPassResult.getLong("ALARM_BYPASS_CONDITION_ID"),
//          byPassResult.getString("CODE"),
//          byPassResult.getString("NAME"),
//          byPassResult.getBoolean("HAS_PARAM"),
//          byPassResult.getLong("SINGLE_OCAP_BYPASS_ID"),
//          byPassResult.getInt("PARAM_INDEX"),
//          byPassResult.getString("PARAM_VALUE")
//        ))
//      }
//      byPassResult.close()
//      byPass.close()
//
//      val conditionMap = poList.groupBy(p => p.id).map(e => {
//        val id = e._1
//        val head = e._2.head
//        val details = e._2.groupBy(a => a.alarmPassConditionId).map(as => {
//          val apcs = as._2
//          var recentRuns = ""
//          if (apcs.head.hasParam) {
//            recentRuns = apcs.sortBy(p => p.paramIndex).map(p => p.paramValue).reduce(_ + "$" + _)
//          }
//          Detail(apcs.head.name, recentRuns)
//        })
//        (id, ByPassCondition(head.actionSwitch,
//          head.notificationSwitch,
//          details.toList))
//      })
//
//
//      val selectStmt = conn.prepareStatement(
//        """
//          |SELECT CONTEXT_ID, PLAN_ID, PLAN_NAME, CONTROL_PLAN_VERSION, SENSOR_ALIAS_NAME, INDICATOR_ID,
//          | INDICATOR_NAME, ALGO_PARAMS, MISSING_RATIO, CONTROL_WINDOW_ID,DRIFT_STATUS,CALC_STATUS,
//          | ALGO_CLASS, ALGO_TYPE, W2W_TYPE, LOGISTIC_STATUS, BYPASS_CONDITION_ID FROM V_INDICATOR_INFO
//          |""".stripMargin)
//      val resultSet = selectStmt.executeQuery
//      var count = 0
//      while (resultSet.next) {
//        val one = IndicatorConfig(resultSet.getLong("CONTEXT_ID"),
//          resultSet.getLong("PLAN_ID"),
//          resultSet.getString("PLAN_NAME"),
//          resultSet.getLong("CONTROL_PLAN_VERSION"),
//          resultSet.getLong("INDICATOR_ID"),
//          resultSet.getString("INDICATOR_NAME"),
//          resultSet.getString("SENSOR_ALIAS_NAME"),
//          resultSet.getDouble("MISSING_RATIO"),
//          resultSet.getLong("CONTROL_WINDOW_ID"),
//          resultSet.getString("ALGO_CLASS"),
//          resultSet.getString("ALGO_PARAMS"),
//          resultSet.getString("ALGO_TYPE"),
//          resultSet.getString("W2W_TYPE"),
//          resultSet.getBoolean("DRIFT_STATUS"),
//          resultSet.getBoolean("CALC_STATUS"),
//          resultSet.getBoolean("LOGISTIC_STATUS"),
//          Option(conditionMap.get(resultSet.getLong("BYPASS_CONDITION_ID")).orNull))
//
//        indicatorConfigDataByAll.append(ConfigData[IndicatorConfig]("indicatorconfig",serialNo="", status = true, one))
//        count = count + 1
//      }
//      logger.warn("indicatorConfig SIZE :" + count)
//      selectStmt.close()
//      resultSet.close()
//      indicatorConfigDataByAll
//    } catch {
//      case e: Exception => logger.warn("indicatorConfig load error" + e)
//        throw e
//    } finally {
//      if (conn != null) {
//        conn.close()
//      }
//    }
//  }
//}
