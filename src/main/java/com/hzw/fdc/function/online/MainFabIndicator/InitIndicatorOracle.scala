package com.hzw.fdc.function.online.MainFabIndicator

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.ProjectConfig
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import scala.collection.mutable.ListBuffer

object InitIndicatorOracle {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcIndicatorConfigBroadcastProcessFunction])

  Class.forName("oracle.jdbc.driver.OracleDriver")
  lazy val Conn: Connection = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL,
    ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)

  logger.warn("byPassConn start!!!")

  lazy val OracleConfigList: ListBuffer[ConfigData[IndicatorConfig]] = initOracleConfig()

  // 初始化Oracle配置
  def initOracleConfig(): ListBuffer[ConfigData[IndicatorConfig]] = {

    val indicatorConfigDataByAll =  ListBuffer[ConfigData[IndicatorConfig]]()
    logger.warn("Oracle indicatorConfig start!!!")

    try {

      val byPass = Conn.prepareStatement(
        """
          |select      OBC.*
          |                    , FCSOB.ID AS SINGLE_OCAP_ID
          |                    , FCSOB.ALARM_BYPASS_CONDITION_ID
          |                    , FCABC.CODE
          |                    , FCABC.NAME
          |                    , FCABC.HAS_PARAM
          |                    , FCOBP.SINGLE_OCAP_BYPASS_ID
          |                    , FCOBP.PARAM_INDEX
          |                    , FCOBP.PARAM_VALUE
          |        FROM        FDC_CONF_OCAP_BYPASS_CONDITION OBC
          |        LEFT JOIN   FDC_CONF_SINGLE_OCAP_BYPASS FCSOB
          |        ON          OBC.ID = FCSOB.BYPASS_CONDITION_ID
          |        AND         FCSOB.STATUS = 'ENABLED'
          |        LEFT JOIN   FDC_CONF_ALARM_BYPASS_CONDITION FCABC
          |        ON          FCSOB.ALARM_BYPASS_CONDITION_ID = FCABC.ID
          |        LEFT JOIN   FDC_CONF_OCAP_BYPASS_PARAMS FCOBP
          |        ON          FCSOB.ID = FCOBP.SINGLE_OCAP_BYPASS_ID
          |        WHERE       OBC.STATUS = 'ENABLED'
          |""".stripMargin
      )
      val byPassResult = byPass.executeQuery()
      val poList = new ListBuffer[ByPassConditionPO]()
      while (byPassResult.next()) {
        poList.append(ByPassConditionPO(
          byPassResult.getLong("ID"),
          byPassResult.getString("BYPASS_NAME"),
          byPassResult.getBoolean("NOTIFICATION_SWITCH"),
          byPassResult.getBoolean("ACTION_SWITCH"),
          byPassResult.getLong("SINGLE_OCAP_ID"),
          byPassResult.getLong("ALARM_BYPASS_CONDITION_ID"),
          byPassResult.getString("CODE"),
          byPassResult.getString("NAME"),
          byPassResult.getBoolean("HAS_PARAM"),
          byPassResult.getLong("SINGLE_OCAP_BYPASS_ID"),
          byPassResult.getInt("PARAM_INDEX"),
          byPassResult.getString("PARAM_VALUE")
        ))
      }
      val conditionMap = poList.groupBy(p => p.id).map(e => {
        val id = e._1
        val head = e._2.head
        val details = e._2.groupBy(a => a.alarmPassConditionId).map(as => {
          val apcs = as._2
          var recentRuns = ""
          if (apcs.head.hasParam) {
            recentRuns = apcs.sortBy(p => p.paramIndex).map(p => p.paramValue).reduce(_ + "$" + _)
          }
          Detail(apcs.head.name, recentRuns)
        })
        (id, ByPassCondition(head.actionSwitch,
          head.notificationSwitch,
          details.toList))
      })
      println(conditionMap.toJson)


      val selectStmt = Conn.prepareStatement(
        """
          |SELECT CONTEXT_ID, PLAN_ID, PLAN_NAME, CONTROL_PLAN_VERSION, SENSOR_ALIAS_NAME, INDICATOR_ID,
          | INDICATOR_NAME, ALGO_PARAMS, MISSING_RATIO, CONTROL_WINDOW_ID,DRIFT_STATUS,CALC_STATUS,LOGISTIC_STATUS,
          | ALGO_CLASS, ALGO_TYPE, W2W_TYPE, BYPASS_CONDITION_ID  FROM V_INDICATOR_INFO
          |""".stripMargin)
      val resultSet = selectStmt.executeQuery
      var count = 0
      while (resultSet.next) {


       val map= IndicatorConfig(contextId = resultSet.getLong("CONTEXT_ID"),
          controlPlanId = resultSet.getLong("PLAN_ID"),
          controlPlanName = resultSet.getString("PLAN_NAME"),
          controlPlanVersion = resultSet.getLong("CONTROL_PLAN_VERSION"),
          indicatorId = resultSet.getLong("INDICATOR_ID"),
          indicatorName = resultSet.getString("INDICATOR_NAME"),
          sensorAlias = resultSet.getString("SENSOR_ALIAS_NAME"),
          missingRatio = resultSet.getDouble("MISSING_RATIO"),
          controlWindowId = resultSet.getLong("CONTROL_WINDOW_ID"),
          algoClass = resultSet.getString("ALGO_CLASS"),
          algoParam = resultSet.getString("ALGO_PARAMS"),
          algoType = resultSet.getString("ALGO_TYPE"),
          w2wType = resultSet.getString("W2W_TYPE"),
          driftStatus = resultSet.getBoolean("DRIFT_STATUS"),
          calculatedStatus = resultSet.getBoolean("CALC_STATUS"),
          logisticStatus = resultSet.getBoolean("LOGISTIC_STATUS"),
         Option(conditionMap.get(resultSet.getLong("BYPASS_CONDITION_ID")).orNull)
        )


        indicatorConfigDataByAll.append(ConfigData("indicatorconfig", "", status = true, map))
        count = count + 1
      }
      logger.warn("Oracle indicatorConfig SIZE :" + count)
    } catch {
      case e: Exception => logger.warn("indicator load error" + e)
    } finally {
      if (Conn != null) {
        Conn.close()
      }
    }
    indicatorConfigDataByAll
  }
}
