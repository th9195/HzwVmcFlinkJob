package com.hzw.fdc.function.online.MainFabAutoLimit

import java.sql.DriverManager

import com.hzw.fdc.scalabean.{AutoLimitConfig, Condition, DeploymentInfo}
import com.hzw.fdc.util.ProjectConfig
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object InitAutoLimitOracle {


  // 初始化Oracle配置
  def initOracleConfig(logger: Logger): ListBuffer[AutoLimitConfig] = {

    val autoLimitConfigDataByAll: ListBuffer[AutoLimitConfig] =  ListBuffer()
    logger.warn("Oracle autoLimitConfig  start!!!")

    Class.forName("oracle.jdbc.driver.OracleDriver")
    val conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL, ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)
    try {
      val selectStmt = conn.prepareStatement(
        """ SELECT CONTROL_PLAN_ID,
          |       VERSION,
          |       REMOVE_OUTLIER,
          |       MAX_RUN_NUMBER,
          |       EVERY,
          |       TRIGGER_METHOD_NAME,
          |       TRIGGER_METHOD_VALUE,
          |       ACTIVE,
          |       INDICATOR_ID,
          |       SPEC_ID,
          |       TOOL,
          |       CHAMBER,
          |       RECIPE,
          |       PRODUCT,
          |       STAGE,
          |       LIMIT_METHOD,
          |       LIMIT_METHOD_VALUE,
          |       CPK,
          |       TARGET from V_AUTO_LIMIT""".stripMargin)
      val resultSet = selectStmt.executeQuery
      val auToLimitMap = new mutable.HashMap[String, mutable.Set[mutable.HashMap[String, String]]]

      var count = 0
      while (resultSet.next) {
        val controlPlanId = resultSet.getString("CONTROL_PLAN_ID")
        val version =  resultSet.getString("VERSION")
        val removeOutlier =  resultSet.getString("REMOVE_OUTLIER")
        val maxRunNumber =  resultSet.getString("MAX_RUN_NUMBER")
        val every =  resultSet.getString("EVERY")
        val triggerMethodName =  resultSet.getString("TRIGGER_METHOD_NAME")
        val triggerMethodValue = resultSet.getString("TRIGGER_METHOD_VALUE")
        val active =  resultSet.getString("ACTIVE")


        val map = mutable.HashMap(
            "controlPlanId" -> controlPlanId
          , "version" -> version
          , "removeOutlier" -> removeOutlier
          , "maxRunNumber" -> maxRunNumber
          , "every" -> every
          , "triggerMethodName" -> triggerMethodName
          , "triggerMethodValue" -> triggerMethodValue
          , "active" -> checkNull(active)
          , "indicatorId" -> resultSet.getString("INDICATOR_ID")
          , "specId" -> resultSet.getString("SPEC_ID")
          , "tool" -> checkNull(resultSet.getString("TOOL"))
          , "chamber" -> checkNull(resultSet.getString("CHAMBER"))
          , "recipe" -> checkNull(resultSet.getString("RECIPE"))
          , "product" -> checkNull(resultSet.getString("PRODUCT"))
          ,"stage"-> checkNull(resultSet.getString("STAGE"))
          ,"limitMethod"-> checkNull(resultSet.getString("LIMIT_METHOD"))
          ,"limitValue"-> checkNull(resultSet.getString("LIMIT_METHOD_VALUE"))
          ,"cpk"-> checkNull(resultSet.getString("CPK"))
          ,"target"-> checkNull(resultSet.getString("TARGET"))
        )
        count = count + 1
        val key = controlPlanId + version + removeOutlier + maxRunNumber + every + triggerMethodName + triggerMethodValue + active

        if(auToLimitMap.contains(key)){
          val autoSet = auToLimitMap(key)
          autoSet.add(map)
          auToLimitMap.put(key, autoSet)
        }else{
          auToLimitMap += (key -> mutable.Set(map))
        }
      }


      for((k, autoSet) <- auToLimitMap){
        try {
          val config = autoSet.head
          val controlPlanId = config("controlPlanId").toLong
          val version = config("version").toLong
          val removeOutlier = config("removeOutlier").toString
          val maxRunNumber = config("maxRunNumber").toString
          val every = config("every").toString
          val triggerMethodName = config("triggerMethodName").toString
          val triggerMethodValue = config("triggerMethodValue").toString
          val active = config("active").toString

          val deploymentInfoList: ListBuffer[DeploymentInfo] = ListBuffer()
          autoSet.foreach(map => {

            val condition = Condition(
              map("tool").split(",").toList,
              map("chamber").split(",").toList,
              map("recipe").split(",").toList,
              map("product").split(",").toList,
              map("stage").split(",").toList)

            val deploymentInfo = DeploymentInfo(
              map("indicatorId").toLong,
              map("specId").toLong,
              condition,
              map("limitMethod").toString,
              map("limitValue").toString,
              map("cpk").toString,
              map("target").toString
            )
            deploymentInfoList.append(deploymentInfo)
          })
          autoLimitConfigDataByAll.append(AutoLimitConfig(
            "autoLimitSettings",
            //          status = true,
            controlPlanId,
            version,
            removeOutlier,
            maxRunNumber,
            every,
            triggerMethodName,
            triggerMethodValue,
            active,
            deploymentInfoList.toList
          ))
        }catch {
          case e: Exception => logger.warn(s"autoLimitConfig error:$e  config:${autoSet.head}")
        }
      }

      logger.warn("Oracle autoLimitConfig SIZE :" + count)
    }catch {
      case e: Exception => logger.warn("Oracle autoLimitConfig error:" + e)
    }finally {
      if (conn != null) {
        conn.close()
      }
    }
    autoLimitConfigDataByAll
  }


  def checkNull(str: String): String = {
    if(str == null || str == "null"){
      ""
    }else{
      str
    }
  }
}
