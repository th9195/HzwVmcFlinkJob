package com.hzw.fdc.function.online.MainFabAlarm

import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.ProjectConfig
import org.slf4j.{Logger, LoggerFactory}

import java.sql.DriverManager
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class InitAlarmOracle {
  private val logger: Logger = LoggerFactory.getLogger(classOf[InitAlarmOracle])


  case class FDC_CONF_SPEC_LIMIT(SPEC_ID: String, SPEC_LEVEL: String, LIMIT_TYPE: String, LIMIT_VALUE: String)

  case class V_INDICATOR_RULE_INFO(SPEC_ID: String, ALARM_LEVEL: String, RULE: String, RULE_NAME: String,
                                   CRITERIA_PROFILE_ID: String, UL_LL_TYPE: String, ACTION_ID: String, ACTION_NAME: String, PARAM: String, OCAP_INDEX: String, EXT_INFO: String, OCAP_ACTION_ID: String)


  // {"SPEC_DEF_ID":  {"ALARM_LEVEL": [V_INDICATOR_RULE_INFO, V_INDICATOR_RULE_INFO]}}
  val RuleMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.Set[V_INDICATOR_RULE_INFO]]]

  val LimitMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.Set[FDC_CONF_SPEC_LIMIT]]]


  // 初始化Oracle配置
  def initAlarmOracleConfig(): ListBuffer[AlarmRuleConfig] = {

    val indicatorRULEByAll = ListBuffer[AlarmRuleConfig]()
    logger.warn("Oracle indicatorRULEByAll_start!!!")

    Class.forName("oracle.jdbc.driver.OracleDriver")
    val conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL, ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)

    try {
      val selectStmt = conn.prepareStatement(
        """SELECT
          |SPEC_ID,
          |SPEC_LEVEL,
          |LIMIT_TYPE,
          |LIMIT_VALUE
          |from FDC_CONF_SPEC_LIMIT
          |""".stripMargin)
      val resultSet = selectStmt.executeQuery
      while (resultSet.next) {
        val one = FDC_CONF_SPEC_LIMIT(
          resultSet.getString("SPEC_ID"),
          resultSet.getString("SPEC_LEVEL"),
          resultSet.getString("LIMIT_TYPE"),
          resultSet.getString("LIMIT_VALUE"))


        val SPEC_ID = resultSet.getString("SPEC_ID")
        val SPEC_LEVEL = resultSet.getString("SPEC_LEVEL")

        if (!LimitMap.contains(SPEC_ID)) {
          val typeMap = mutable.HashMap[String, mutable.Set[FDC_CONF_SPEC_LIMIT]](SPEC_LEVEL -> mutable.Set(one))
          LimitMap += (SPEC_ID -> typeMap)
        } else {
          val typeMap = LimitMap(SPEC_ID)

          if (typeMap.contains(SPEC_LEVEL)) {
            val tool_set = typeMap(SPEC_LEVEL)
            tool_set.add(one)
            typeMap += (SPEC_LEVEL -> tool_set)
            LimitMap.put(SPEC_ID, typeMap)
          } else {
            typeMap += (SPEC_LEVEL -> mutable.Set(one))
            LimitMap += (SPEC_ID -> typeMap)
          }
        }
      }
      resultSet.close()
      selectStmt.close()


      val selectStmt2 = conn.prepareStatement(
        """SELECT SPEC_ID, alarm_level, rule, rule_name, criteria_profile_id, UL_LL_TYPE, ACTION_ID, ACTION_NAME, PARAM, OCAP_INDEX, EXT_INFO, OCAP_ACTION_ID  FROM V_INDICATOR_RULE_INFO""".stripMargin)
      val resultSet2 = selectStmt2.executeQuery
      while (resultSet2.next) {
        val one = V_INDICATOR_RULE_INFO(
          resultSet2.getString("SPEC_ID"),
          resultSet2.getString("alarm_level"),
          resultSet2.getString("rule"),
          resultSet2.getString("rule_name"),
          resultSet2.getString("criteria_profile_id"),
          resultSet2.getString("UL_LL_TYPE"),
          resultSet2.getString("ACTION_ID"),
          resultSet2.getString("ACTION_NAME"),
          resultSet2.getString("PARAM"),
          resultSet2.getString("OCAP_INDEX"),
          resultSet2.getString("EXT_INFO"),
          resultSet2.getString("OCAP_ACTION_ID"))

        val rule = resultSet2.getString("rule")
        val SPEC_DEF_ID = resultSet2.getString("SPEC_ID")

        val key = rule
        if (!RuleMap.contains(SPEC_DEF_ID)) {
          val typeMap = mutable.HashMap[String, mutable.Set[V_INDICATOR_RULE_INFO]](key -> mutable.Set(one))
          RuleMap += (SPEC_DEF_ID -> typeMap)
        } else {
          val typeMap = RuleMap(SPEC_DEF_ID)

          if (typeMap.contains(key)) {
            val tool_set = typeMap(key)
            tool_set.add(one)
            typeMap += (key -> tool_set)
            RuleMap.put(SPEC_DEF_ID, typeMap)
          } else {
            typeMap += (key -> mutable.Set(one))
            RuleMap += (SPEC_DEF_ID -> typeMap)
          }
        }
      }
      resultSet2.close()
      selectStmt2.close()

      val ocapSelectStmt = conn.prepareStatement(
        """SELECT      OA.ID AS OCAP_ACTION_ID
          |        , ACTION_NAME
          |        , ALARM_ACTION_ID
          |        , M.EXT_INFO as EXT_INFO
          |        FROM        FDC_CONF_OCAP_ACTION OA
          |        LEFT JOIN   FDC_CONF_OCAP_ACTION_MAPPING M
          |        ON          OA.ID = M.OCAP_ACTION_ID
          |        LEFT JOIN   FDC_CONF_ALARM_ACTION AA
          |        ON          AA.ID = M.ALARM_ACTION_ID
          |        AND         AA.STATUS = 'ENABLED'
          |        WHERE       OA.STATUS = 'ENABLED'""".stripMargin)
      val ocapResultSet = ocapSelectStmt.executeQuery
      val ocapActionList = mutable.ListBuffer[(String, AlarmRuleAction)]()
      while (ocapResultSet.next()) {
        ocapActionList.append((ocapResultSet.getString("OCAP_ACTION_ID"), AlarmRuleAction(ocapResultSet.getLong("ALARM_ACTION_ID"), Option(ocapResultSet.getString("ACTION_NAME")), Option.apply(ocapResultSet.getString("EXT_INFO")), Option.apply("action"))))
      }
      ocapResultSet.close()
      ocapSelectStmt.close()
      val ocapActionMap = ocapActionList.groupBy(x => x._1).map(x => (x._1, x._2.map(y => y._2)))

      val selectStmt3 = conn.prepareStatement(
        """SELECT
          |CONTROL_PLAN_ID,
          |CONTROL_PLAN_VERSION,
          |TOOL_NAME,
          |CHAMBER_NAME,
          |RECIPE_NAME,
          |PRODUCT_NAME,
          |STAGE_NAME,
          |INDICATOR_ID,
          |W2W_TYPE,
          |SPEC_ID
          |from V_INDICATOR_SPEC_INFO""".stripMargin)
      val resultSet3 = selectStmt3.executeQuery
      var count = 0
      while (resultSet3.next) {

        val SPEC_ID = resultSet3.getString("SPEC_ID")

        val parseResult = ParseLimitAndRule(SPEC_ID, ocapActionMap)
        if (LimitMap.contains(SPEC_ID)) {
          val one = AlarmRuleConfig(
            resultSet3.getInt("CONTROL_PLAN_ID"),
            resultSet3.getInt("CONTROL_PLAN_VERSION"),
            Option(resultSet3.getString("TOOL_NAME")),
            Option(resultSet3.getString("CHAMBER_NAME")),
            Option(resultSet3.getString("RECIPE_NAME")),
            Option(resultSet3.getString("PRODUCT_NAME")),
            Option(resultSet3.getString("STAGE_NAME")),
            resultSet3.getString("INDICATOR_ID"),
            resultSet3.getString("SPEC_ID"),
            resultSet3.getString("W2W_TYPE"),
            null,
            parseResult._2,
            parseResult._1.toList,
            limitConditionName = "",
            indicatorType = "",
            isEwma = false)

          indicatorRULEByAll.append(one)
          count = count + 1
        }
      }
      logger.warn("Oracle AlarmRuleConfig SIZE :" + count)
      resultSet3.close()
      selectStmt3.close()

      logger.warn(s"indicatorRULEByAll_end!!! SIZE：${indicatorRULEByAll.size}")
      indicatorRULEByAll
    } catch {
      case e: Exception => logger.warn("AlarmRuleConfig indicator load error" + e)
        throw e
    } finally {
      if (conn != null) {
        conn.close()
      }
    }

  }


  /**
   * 解析Limit 和 Rule
   */
  def ParseLimitAndRule(SPEC_ID: String, ocapActionMap: Map[String, mutable.ListBuffer[AlarmRuleAction]]): Tuple2[mutable.Set[AlarmRuleType], AlarmRuleLimit] = {


    val alarmRuleTypes: mutable.Set[AlarmRuleType] = mutable.Set()
    val alarmRuleLimit = AlarmRuleLimit(None, None, None, None, None, None)
    try {

      // 解析Limit
      val limit_config = LimitMap(SPEC_ID)
      limit_config.foreach(x => {
        val limitSet = x._2
        val alarmLevel = x._1
        for (limit <- limitSet) {
          alarmLevel match {
            case "1" =>
              if (limit.LIMIT_TYPE == "UL") {
                alarmRuleLimit.UCL = Option(limit.LIMIT_VALUE.toDouble)
              }
              if (limit.LIMIT_TYPE == "LL") {
                alarmRuleLimit.LCL = Option(limit.LIMIT_VALUE.toDouble)
              }
            case "2" =>
              if (limit.LIMIT_TYPE == "UL") {
                alarmRuleLimit.UBL = Option(limit.LIMIT_VALUE.toDouble)
              }
              if (limit.LIMIT_TYPE == "LL") {
                alarmRuleLimit.LBL = Option(limit.LIMIT_VALUE.toDouble)
              }
            case "3" =>
              if (limit.LIMIT_TYPE == "UL") {
                alarmRuleLimit.USL = Option(limit.LIMIT_VALUE.toDouble)
              }
              if (limit.LIMIT_TYPE == "LL") {
                alarmRuleLimit.LSL = Option(limit.LIMIT_VALUE.toDouble)
              }
            case _ =>
          }
        }

      })

      // 解析rule
      if (RuleMap.contains(SPEC_ID)) {
        val rule_config = RuleMap(SPEC_ID)

        for (k <- rule_config) {
          val rule = k._1
          val ruleSet = k._2
          val criteriaProfileMap = ruleSet.groupBy(x => x.CRITERIA_PROFILE_ID)
          val alarmRuleParamMapList = ruleSet.groupBy(o => o.OCAP_INDEX)
            .map(p => {
              val ocapIndex = p._1
              p._2.groupBy(x => x.CRITERIA_PROFILE_ID + "," + x.PARAM)
                .map(y => {
                  val keySplit = y._1.split(",")
                  val criteriaProfileId = keySplit(0)
                  val param = keySplit(1).split("\\|")
                  val list = y._2

                  val N: Option[Long] = if (param.length == 2) {
                    Option(param.last.toLong)
                  } else {
                    null

                  }
                  val ruleParam = AlarmRuleParam(
                    ocapIndex.toInt,
                    Option(param.head.toLong),
                    N,
                    list.filter(a => a.ACTION_ID != null).flatMap(a => {
                      val actionList = new mutable.ListBuffer[AlarmRuleAction]()
                      if ("11".equals(a.ACTION_ID) && a.OCAP_ACTION_ID != null) {
                        actionList.appendAll(ocapActionMap.getOrElse(a.OCAP_ACTION_ID, new mutable.ListBuffer[AlarmRuleAction]()))
                      } else {
                        actionList.append(AlarmRuleAction(a.ACTION_ID.toLong, Option(a.ACTION_NAME), Option.apply(a.EXT_INFO), Option.apply("notification")))
                      }
                      actionList
                    }).toList
                  )
                  (criteriaProfileId, ruleParam)
                })
            })
          val USLorRule45 = ListBuffer[AlarmRuleParam]()
          val UBL = ListBuffer[AlarmRuleParam]()
          val UCL = ListBuffer[AlarmRuleParam]()
          val LCL = ListBuffer[AlarmRuleParam]()
          val LBL = ListBuffer[AlarmRuleParam]()
          val LSL = ListBuffer[AlarmRuleParam]()
          for (x <- alarmRuleParamMapList) {
            for (y <- x.groupBy(x => x._1)) {
              val profile = criteriaProfileMap(y._1).head
              val ulllType = profile.UL_LL_TYPE
              val level = profile.ALARM_LEVEL
              val alarmRuleParams = y._2(y._1)
              if (rule == "4" || rule == "5") {
                USLorRule45.append(alarmRuleParams)
              } else {
                level match {
                  case "1" =>
                    if (ulllType == "UL") {
                      UCL.append(alarmRuleParams)
                    }
                    if (ulllType == "LL") {
                      LCL.append(alarmRuleParams)
                    }
                  case "2" =>
                    if (ulllType == "UL") {
                      UBL.append(alarmRuleParams)
                    }
                    if (ulllType == "LL") {
                      LBL.append(alarmRuleParams)
                    }
                  case "3" =>
                    if (ulllType == "UL") {
                      USLorRule45.append(alarmRuleParams)
                    }
                    if (ulllType == "LL") {
                      LSL.append(alarmRuleParams)
                    }
                  case _ =>
                }
              }

            }

          }
          val oneType = AlarmRuleType(
            rule.toInt,
            ruleSet.head.RULE_NAME,
            USLorRule45.toList,
            UBL.toList,
            UCL.toList,
            LCL.toList,
            LBL.toList,
            LSL.toList
          )

          alarmRuleTypes.add(oneType)
        }
      }
    } catch {
      case e: Exception => logger.warn("ParseLimitAndRule error" + e)
    }

    (alarmRuleTypes, alarmRuleLimit)
  }

  // 初始化Oracle视图的 alarm开关配置
  def initAlarmSwitchEventOracleConfig(): ListBuffer[AlarmSwitchEventConfig] = {

    val alarmSwitchEventByAll = ListBuffer[AlarmSwitchEventConfig]()
    logger.warn("Oracle AlarmSwitchEventConfig start!!!")

    Class.forName("oracle.jdbc.driver.OracleDriver")
    val conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL, ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)

    try {
      val selectStmt = conn.prepareStatement(
        """SELECT
          |TOOL_NAME,
          |CHAMBER_NAME,
          |EVENT_STATUS
          |from V_CHAMBER_ALARM_SWITCH
          |""".stripMargin)
      val resultSet = selectStmt.executeQuery
      while (resultSet.next) {
        val one = AlarmSwitchEventConfig(
          resultSet.getString("TOOL_NAME"),
          resultSet.getString("CHAMBER_NAME"),
          resultSet.getString("EVENT_STATUS"),
          0L,
          1)
        alarmSwitchEventByAll.append(one)
      }
      resultSet.close()
      selectStmt.close()
    } catch {
      case e: Exception => logger.warn("AlarmSwitchEventConfig error" + e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }

    alarmSwitchEventByAll
  }

  /**
   * 判断字符串为空
   */
  def hasLength(str: String): Boolean = {
    str != null && str != ""
  }
}
