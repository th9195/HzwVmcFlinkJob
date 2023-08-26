package com.hzw.fdc.function.online.MainFabAlarm

import com.hzw.fdc.scalabean.{Action, ConfigData, MicConfig, indicatorLimit}
import com.hzw.fdc.util.ProjectConfig
import org.slf4j.Logger

import java.sql.DriverManager
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

object InitMicOracle {

  // 初始化Oracle配置
  def initOracleConfig(logger: Logger): List[ConfigData[MicConfig]] = {

    val micConfigDataByAll = ListBuffer[ConfigData[MicConfig]]()
    logger.warn("Oracle micConfig start!!!")

    Class.forName("oracle.jdbc.driver.OracleDriver")
    val conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL, ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)
    try {
      val selectStmt = conn.prepareStatement(
        """SELECT
          |	MIC_ID,
          |	MIC_NAME,
          |	ALARM_ACTION_IDS,
          |	OCAP_ALARM_ACTION_IDS,
          | ALARM_ACTION_NAMES,
          |	CONTEXT_ID,
          |	CONTROL_PLAN_ID,
          |	PLAN_NAME,
          |	PLAN_TYPE,
          |	VERSION,
          |	INDICATOR_ID,
          | INDICATOR_NAME,
          |	ALARM_LEVELS,
          |	TOOL_GROUP_ID,
          |	CHAMBER_GROUP_ID,
          |	RECIPE_GROUP_ID,
          |	STAGE_ID,
          | X_OF_TOTAL,
          |	PRODUCT_ID FROM V_MIC_INFO""".stripMargin)
      val resultSet = selectStmt.executeQuery
      var count = 0
      val map1 = new concurrent.TrieMap[String, MicConfig]()
      while (resultSet.next) {
        val micId = resultSet.getLong("MIC_ID")
        val micName = resultSet.getString("MIC_NAME")
        val controlPlanId = resultSet.getLong("CONTROL_PLAN_ID")
        val controlPlanName = resultSet.getString("PLAN_NAME")
        val controlPlanVersion = resultSet.getLong("VERSION")
        val actionId = resultSet.getString("ALARM_ACTION_IDS")
        val ocapActionId = resultSet.getString("OCAP_ALARM_ACTION_IDS")
        val actionName = resultSet.getString("ALARM_ACTION_NAMES")
        val indicatorId = resultSet.getLong("INDICATOR_ID")
        val indicatorName = resultSet.getString("INDICATOR_NAME")
        val oocLevel = resultSet.getString("ALARM_LEVELS")
        val x_of_total = resultSet.getLong("X_OF_TOTAL")
        val key = controlPlanId + "|" + micId
        if (map1.contains(key)) {
          val mic = map1(key)
          var configDef = mic.`def`
          configDef = mic.`def` :+ indicatorLimit(indicatorId, indicatorName, oocLevel.split(",").map(_.toLong).toList)
          val mic1 = MicConfig(controlPlanId, controlPlanName, controlPlanVersion, actionId, ocapActionId, actionName, micId, micName, x_of_total, configDef, List.empty)
          map1.put(key, mic1)
        } else {
          val configDef = indicatorLimit(indicatorId, indicatorName, oocLevel.split(",").map(_.toLong).toList)
          val mic = MicConfig(controlPlanId, controlPlanName, controlPlanVersion, actionId, ocapActionId, actionName, micId, micName, x_of_total, List(configDef), List.empty)
          map1 += (key -> mic)
        }
      }



      resultSet.close()
      selectStmt.close()

      val alarmActions = conn.prepareStatement("select * from FDC_CONF_ALARM_ACTION WHERE  STATUS = 'ENABLED'")
      val alarmActionsResults = alarmActions.executeQuery()
      val alarmActionMap = mutable.Map[Long, Action]()
      while (alarmActionsResults.next()) {
        val id = alarmActionsResults.getLong("id")
        val actionName = alarmActionsResults.getString("ACTION_NAME")
        alarmActionMap.put(id, Action(id, actionName, Option(""), "notification"))
      }
      alarmActionsResults.close()
      alarmActions.close()

      val ocapAlarmActions = conn.prepareStatement(
        """
          |        SELECT      OA.*
          |                    , ACTION_NAME
          |                    , ALARM_ACTION_ID
          |                    , M.EXT_INFO
          |        FROM        FDC_CONF_OCAP_ACTION OA
          |        LEFT JOIN   FDC_CONF_OCAP_ACTION_MAPPING M
          |        ON          OA.ID = M.OCAP_ACTION_ID
          |        LEFT JOIN   FDC_CONF_ALARM_ACTION AA
          |        ON          AA.ID = M.ALARM_ACTION_ID
          |        AND         AA.STATUS = 'ENABLED'
          |        AND         OA.STATUS = 'ENABLED'
          |""".stripMargin)
      val ocapAlarmActionsResults = ocapAlarmActions.executeQuery()
      val ocapAlarmActionMap = new mutable.HashMap[Long, mutable.HashMap[Long, ListBuffer[Action]]]
      while (ocapAlarmActionsResults.next()) {
        val controlPlanId = ocapAlarmActionsResults.getLong("CONTROL_PLAN_ID")
        val oamp = ocapAlarmActionMap.get(controlPlanId)
        val oam = oamp.getOrElse(new mutable.HashMap[Long, ListBuffer[Action]]())

        val id = ocapAlarmActionsResults.getLong("id")
        val alp = oam.get(id)
        val al = alp.getOrElse(new ListBuffer[Action]())

        val actionName = ocapAlarmActionsResults.getString("ACTION_NAME")
        val actionId = ocapAlarmActionsResults.getLong("ALARM_ACTION_ID")
        val extInfo = ocapAlarmActionsResults.getString("EXT_INFO")
        al.append(Action(actionId, actionName, Option(extInfo), "action"))
        oam.put(id, al)
      }
      ocapAlarmActionsResults.close()
      ocapAlarmActions.close()

      for (mic <- map1.values) {
        val actions = new ListBuffer[Action]()
        if (mic.actionId != null && mic.actionId != "") {
          val actionIds = mic.actionId.split(",").filter(x => !"11".equals(x)).map(x => x.toLong).toList
          for (id <- actionIds) {
            val action = alarmActionMap.get(id)
            if (action.isDefined) {
              actions.append(action.get)
            }
          }
        }
        if (mic.ocapActionId != null && mic.ocapActionId != "") {
          val actionIds = mic.ocapActionId.split(",").map(x => x.toLong).toList
          for (id <- actionIds) {
            val controlPlan = ocapAlarmActionMap.get(mic.controlPlanId)
            if (controlPlan.isDefined) {
              val action = controlPlan.get.get(id)
              actions.appendAll(action.get)
            }
          }
        }

        val map = MicConfig(controlPlanId = mic.controlPlanId,
          controlPlanName = mic.controlPlanName,
          controlPlanVersion = mic.controlPlanVersion,
          actionId = mic.actionId,
          ocapActionId = mic.ocapActionId,
          actionName = mic.actionName,
          micId = mic.micId,
          micName = mic.micName,
          xofTotal = mic.xofTotal,
          `def` = mic.`def`,
          actions = actions.toList)
        micConfigDataByAll.append(ConfigData[MicConfig](
          "micConfig",
          "",
          status = true,
          map))
        count = count + 1
        logger.warn(s"Oracle micConfig_step1: " + map)
      }
      logger.warn("Oracle micConfig SIZE :" + count)
    } catch {
      case e: Exception => logger.warn("mic load error" + e)
    }finally {
      if (conn != null) {
        conn.close()
      }
    }
    micConfigDataByAll.toList
  }

}
