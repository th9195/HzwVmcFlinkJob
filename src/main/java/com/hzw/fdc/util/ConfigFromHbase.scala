package com.hzw.fdc.util

import com.hzw.fdc.function.online.MainFabAlarm.AlarmSwitchEventConfig
import com.hzw.fdc.json.JsonUtil
import com.hzw.fdc.scalabean._
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Optional
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author lwt
 * @deprecated 从hbase中读取全量配置
 */
object ConfigFromHbase {
  /**
   * @deprecated 初始化后需要跟新的配置
   */
  object UpdateAfterInit {

    def loadWindowConfig(): List[ConfigData[List[WindowConfigData]]] = {
      loadConfig[ConfigData[List[WindowConfigData]]]("context")
    }

    def loadRuleConfig(): List[ConfigData[AlarmRuleConfig]] = {
      loadConfig[ConfigData[AlarmRuleConfig]]("alarmconfig")
    }

    def loadMicAlarmConfig(): List[ConfigData[MicConfig]] = {
      loadConfig[ConfigData[MicConfig]]("micalarmconfig")
    }

    def loadChamberAlarmSwitchConfig(): List[ConfigData[AlarmSwitchEventConfig]] = {
      loadConfig[ConfigData[AlarmSwitchEventConfig]]("alarmSwitchEventConfig")
    }

    def loadVirtualSensorConfig(): List[ConfigData[VirtualSensorConfigData]] = {
      loadConfig[ConfigData[VirtualSensorConfigData]]("virtualSensorConfig")
    }

    def loadAutoLimitSettingConfig(): List[ConfigData[AutoLimitConfig]] = {
      loadConfig[ConfigData[AutoLimitConfig]]("autoLimitSettings")
    }

    def loadIndicatorConfig(): List[ConfigData[IndicatorConfig]] = {
      loadConfig[ConfigData[IndicatorConfig]]("indicatorconfig")
    }

    def loadContextConfig(): List[ConfigData[ContextConfigData]] = {
      loadConfig[ConfigData[ContextConfigData]]("context")
    }

    private def loadConfig[T](configName: String)(implicit m: Manifest[T]): List[T] = {
      val loader = new ConfigFromHbaseLoader[T]
      val timestamp = loader.getVersionInfo("data")
      loader.loadConfig(ProjectConfig.HBASE_SYNC_LOG_TABLE, "", "f1", configName, timestamp + "_")
    }

  }

  /**
   * @deprecated 初始化所有配置 ConfigFromHbase.InitAll.loadWindowConfig()
   */
  object InitAll {

    def loadWindowConfig(): List[ConfigData[List[WindowConfigData]]] = {
      loadConfig[ConfigData[List[WindowConfigData]]](ProjectConfig.HBASE_SYNC_WINDOW_TABLE)
    }

    def loadRuleConfig(): List[ConfigData[AlarmRuleConfig]] = {
      loadConfig[ConfigData[AlarmRuleConfig]](ProjectConfig.HBASE_SYNC_RULE_TABLE)
    }

    def loadMicAlarmConfig(): List[ConfigData[MicConfig]] = {
      loadConfig[ConfigData[MicConfig]](ProjectConfig.HBASE_SYNC_MIC_ALARM_TABLE)
    }

    def loadChamberAlarmSwitchConfig(): List[ConfigData[AlarmSwitchEventConfig]] = {
      loadConfig[ConfigData[AlarmSwitchEventConfig]](ProjectConfig.HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE)
    }

    def loadVirtualSensorConfig(): List[ConfigData[VirtualSensorConfigData]] = {
      loadConfig[ConfigData[VirtualSensorConfigData]](ProjectConfig.HBASE_SYNC_VIRTUAL_SENSOR_TABLE)
    }

    def loadAutoLimitSettingConfig(): List[ConfigData[AutoLimitConfig]] = {
      loadConfig[ConfigData[AutoLimitConfig]](ProjectConfig.HBASE_SYNC_AUTO_LIMIT_TABLE)
    }

    def loadIndicatorConfig(): List[ConfigData[IndicatorConfig]] = {
      loadConfig[ConfigData[IndicatorConfig]](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE)
    }

    def loadContextConfig(): List[ConfigData[ContextConfigData]] = {
      loadConfig[ConfigData[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE)
    }

    private def loadConfig[T](configName: String)(implicit m: Manifest[T]): List[T] = {
      val loader = new ConfigFromHbaseLoader[T]
      val version = loader.getVersionInfo("version")
      loader.loadConfig(configName, version, "f1", "data", "")
    }

  }

  private class ConfigFromHbaseLoader[T] {

    private val logger: Logger = LoggerFactory.getLogger(classOf[ConfigFromHbaseLoader[T]])

    /**
     * 获取最新table version
     */
    def getVersionInfo(coulum: String): String = {
      var connection: Connection = null
      try {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
        conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
        connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_FULL_SYNC_TIME_TABLE))
        val get = new Get("starttime".getBytes())
        get.addColumn("f1".getBytes(), coulum.getBytes())
        val result = table.get(get)
        val version = Bytes.toString(CellUtil.cloneValue(result.current()))
        logger.warn(s"full sync version: $version")
        version
      } finally {
        if (connection != null) {
          connection.close()
        }
      }
    }

    def loadConfig(tablePrefix: String, version: String, f: String, c: String, rowkey: String)(implicit m: Manifest[T]): List[T] = {
      var connection: Connection = null
      var tableName = tablePrefix
      if (Optional.ofNullable(version).isPresent) {
        tableName = tableName + version
      }
      val allConfig = new ListBuffer[T]()
      try {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
        conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
        connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf(tableName))
        val scan = new Scan()
        if (!Optional.ofNullable(rowkey).isPresent) {
          scan.setStartRow(rowkey.getBytes())
        }
        scan.addColumn(f.getBytes(), c.getBytes())
        val allControlPlan = table.getScanner(scan)
        val iterator = allControlPlan.iterator()
        if (iterator.hasNext) {
          val result = iterator.next()
          if (result == null) {
            logger.warn(s"load $tablePrefix config failed: empty")
            return allConfig.toList
          }
          val cell = result.getColumnLatestCell(f.getBytes(), c.getBytes())
          var configJson = Bytes.toString(CellUtil.cloneValue(cell))
          if (configJson == null || configJson == "") {
            configJson = "{}"
          }
          logger.warn(s"load $tablePrefix config success: $configJson")

          allConfig.append(JsonUtil.fromJson[T](configJson))
        } else {
          logger.warn(s"load $tablePrefix config failed: empty")
          allConfig.append(JsonUtil.fromJson[T]("{}"))
        }
        allConfig.toList
      } finally {
        if (connection != null) {
          connection.close()
        }
      }
    }

  }

}
