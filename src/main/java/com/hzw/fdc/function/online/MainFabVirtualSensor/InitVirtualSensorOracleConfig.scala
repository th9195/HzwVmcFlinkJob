package com.hzw.fdc.function.online.MainFabVirtualSensor

import com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor.FdcOfflineVIrtualResultProcessFunction
import com.hzw.fdc.scalabean.{SensorAliasPO, VirtualLevel, VirtualSensorAlgoParam, VirtualSensorConfig, VirtualSensorConfigData, VirtualSensorParamPO, VirtualSensorToolMsg, VirtualSensorViewPO}
import com.hzw.fdc.util.ProjectConfig
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author lwt
 * @date 2021/7/6
 */
object InitVirtualSensorOracleConfig {

  // 初始化VirtualSensorOracle配置
  def initOracleConfig(logger: Logger): ListBuffer[VirtualSensorConfig] = {

    val virtualSensorConfigList = ListBuffer[VirtualSensorConfig]()
    logger.warn("Oracle InitVirtualSensorOracleConfig start!!!")

    Class.forName("oracle.jdbc.driver.OracleDriver")

    val conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL, ProjectConfig.MAIN_FAB_CORE_ORACLE_USER, ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD)
    try {
      val poList = ListBuffer[VirtualSensorParamPO]()
      val selectStmt = conn.prepareStatement("select * from FDC_CONF_VIRTUAL_SENSOR_PARAMS where STATUS = 'ENABLED'")
      val resultSet = selectStmt.executeQuery
      while (resultSet.next) {
        poList.append(VirtualSensorParamPO(resultSet.getLong("VIRTUAL_SENSOR_ID")
          , resultSet.getInt("PARAM_INDEX")
          , resultSet.getString("PARAM_NAME")
          , resultSet.getString("PARAM_DATA_TYPE")
          , resultSet.getString("PARAM_VALUE")))
      }
      resultSet.close()
      selectStmt.close()

      var aliasNameMap = Map[String, SensorAliasPO]()
      val sensorAlisaIdList = poList.filter(s => "SENSOR_ALIAS_ID".equals(s.paramDataType)).map(s => s.paramValue).toList
      if (sensorAlisaIdList.nonEmpty) {
        val aliasIdStr = sensorAlisaIdList.mkString("(", ",", ")")
        val poList3 = ListBuffer[SensorAliasPO]()
        val selectStmt3 = conn.prepareStatement(s"select FCSA.ID as ID,FCSA.SENSOR_ALIAS_NAME AS SENSOR_ALIAS_NAME,FCS.SVID AS SVID from FDC_CONF_SENSOR_ALIAS FCSA LEFT JOIN FDC_CONF_SENSOR FCS on FCSA.ID = FCS.ALIAS_ID WHERE FCSA.ID in $aliasIdStr")
        val resultSet3 = selectStmt3.executeQuery
        while (resultSet3.next) {
          poList3.append(SensorAliasPO(resultSet3.getString("ID")
            , resultSet3.getString("SENSOR_ALIAS_NAME")
            , resultSet3.getString("SVID")))
        }
        aliasNameMap = poList3.map(s => (s.sensorAliasId, s)).toMap
        resultSet3.close()
        selectStmt3.close()
      }
      val paramsMap = poList.groupBy(x => x.virtualSensorId).map(x => {
        (x._1, x._2.map(y => {
          val paramDataType = y.paramDataType
          var paramValue = y.paramValue
          var svid = ""
          if ("SENSOR_ALIAS_ID".equals(paramDataType)) {
            val entry = aliasNameMap(paramValue)
            paramValue = entry.sensorAliasName
            svid = entry.svid
          }
          VirtualSensorAlgoParam(
            y.paramIndex,
            y.paramName,
            paramDataType,
            paramValue,
            svid)
        }).toList)
      })

      val poList2 = ListBuffer[VirtualSensorViewPO]()
      val selectStmt2 = conn.prepareStatement("select * from V_VIRTUAL_SENSOR")
      val resultSet2 = selectStmt2.executeQuery
      while (resultSet2.next) {
        poList2.append(VirtualSensorViewPO(resultSet2.getLong("VIRTUAL_SENSOR_ID")
          , resultSet2.getLong("CONTROL_PLAN_ID")
          , resultSet2.getLong("CONTROL_PLAN_VERSION")
          , resultSet2.getString("TOOL_NAME")
          , resultSet2.getString("CHAMBER_NAME")
          , resultSet2.getString("SVID")
          , resultSet2.getString("SENSOR_ALIAS_NAME")
          , resultSet2.getString("ALGO_NAME")
          , resultSet2.getString("ALGO_CLASS")))
      }
      resultSet2.close()
      selectStmt2.close()

      val levelMap = getLevelMap(conn, poList2.map(x => VirtualLevel(Option.apply(x.virtualSensorId), Option.empty, Option.empty, Option.empty)))
      val configs = poList2.groupBy(x => x.virtualSensorId).map(x => {
        val groupList = x._2
        val one = groupList.head
        val toolMsg = groupList.map(y => VirtualSensorToolMsg(
          y.toolName,
          y.chamberName)).toList
        val op = paramsMap.get(one.virtualSensorId)
        val data = VirtualSensorConfigData(
          one.controlPlanId,
          one.controlPlanVersion,
          one.svid,
          one.virtualSensorAliasName,
          levelMap.getOrElse(one.virtualSensorId, 0),
          one.algoName,
          one.algoClass,
          toolMsg,
          op.getOrElse(List())
        )
        if (op.isEmpty) {
          logger.warn(s"InitVirtualSensorParamConfig is empty $data")
        }
        VirtualSensorConfig(
          System.currentTimeMillis().toString,
          "virtualSensorConfig",
          status = true,
          data
        )
      })
      virtualSensorConfigList.appendAll(configs.filter(x => x.datas.param.nonEmpty))
    } catch {
      case e: Exception => logger.warn("InitVirtualSensorOracleConfig load error" + e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
    virtualSensorConfigList
  }

  def getLevelMap(conn: Connection, vm: ListBuffer[VirtualLevel]): mutable.HashMap[Long, Int] = {
    val virtualSensorIds = vm.map(v => v.virtualSensorId.get.toString)
    val idStr = virtualSensorIds.distinct.reduce(_ + "," + _)
    val virtualSensorLevel = new ListBuffer[VirtualLevel]()
    val selectStmt = conn.prepareStatement(
      s"""
         |        select A.VIRTUAL_SENSOR_ID as virtualSensorId, nvl2(FCVS.ID, 1, 0)  as sensorLevel, A.PARAM_VALUE as paramValue
         |        from (select distinct PARAM_VALUE,VIRTUAL_SENSOR_ID
         |        from FDC_CONF_VIRTUAL_SENSOR_PARAMS
         |        where STATUS = 'ENABLED'
         |        and VIRTUAL_SENSOR_ID in ($idStr)
         |        AND PARAM_DATA_TYPE = 'SENSOR_ALIAS_ID') A
         |        left join FDC_CONF_VIRTUAL_SENSOR FCVS ON FCVS.SENSOR_ALIAS_ID = A.PARAM_VALUE AND FCVS.STATUS = 'ENABLED'
         |""".stripMargin)
    val resultSet = selectStmt.executeQuery
    while (resultSet.next) {
      virtualSensorLevel.append(VirtualLevel(Option.apply(resultSet.getLong("virtualSensorId"))
        , Option.empty
        , Option.apply(resultSet.getInt("sensorLevel"))
        , Option.apply(resultSet.getLong("paramValue"))))
    }
    resultSet.close()
    selectStmt.close()

    val result = new mutable.HashMap[Long, Int]
    if (virtualSensorLevel.isEmpty) {
      for (aLong <- virtualSensorIds) {
        result.put(aLong.toLong, 0)
      }
      return result
    }
    if (vm.exists(x => x.rootVirtualSensorId.isDefined)) {
      val map = vm.map(x => (x.virtualSensorId.get, x)).toMap
      virtualSensorLevel.foreach(x => {
        x.rootVirtualSensorId = map.getOrElse(x.virtualSensorId.get, VirtualLevel(Option.empty, Option.empty, Option.empty, Option.empty)).rootVirtualSensorId
      })
    } else {
      virtualSensorLevel.foreach(x => x.rootVirtualSensorId = x.virtualSensorId)
    }
    val bigLevel = new ListBuffer[VirtualLevel]()
    for (virtualLevel <- virtualSensorLevel) {
      if (virtualLevel.sensorLevel.get > 0) {
        bigLevel.append(VirtualLevel(virtualLevel.paramValue, virtualLevel.virtualSensorId, Option.empty, Option.empty))
      }
      val value = result.get(virtualLevel.rootVirtualSensorId.get)
      if (value.isEmpty) {
        result.put(virtualLevel.rootVirtualSensorId.get, 0)
      } else {
        result.put(virtualLevel.rootVirtualSensorId.get, value.get)
      }
    }
    if (bigLevel.nonEmpty) {
      val levelMap = getLevelMap(conn, bigLevel)
      for ((k, v) <- levelMap.toList) {
        val value = result.get(k)
        if (value.isEmpty) {
          result.put(k, 0)
        } else {
          result.put(k, v + 1)
        }
      }
    }
    result
  }

}
