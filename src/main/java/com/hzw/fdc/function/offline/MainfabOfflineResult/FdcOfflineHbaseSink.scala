package com.hzw.fdc.function.offline.MainfabOfflineResult

import com.hzw.fdc.scalabean.OfflineIndicatorResult
import com.hzw.fdc.service.offline.MainFabOfflineResultService
import com.hzw.fdc.util.ProjectConfig
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.slf4j.{Logger, LoggerFactory}

class FdcOfflineHbaseSink extends RichSinkFunction[OfflineIndicatorResult] {

  var connection: Connection = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineResultService])

  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    //创建hbase连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    connection = ConnectionFactory.createConnection(conf)
  }

  /**
   * 每条数据写入
   *
   * @param value
   * @param context
   */
  override def invoke(value: OfflineIndicatorResult, context: SinkFunction.Context): Unit = {
    invokeIndicatorDataSite(value)
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    connection.close()
    super.close()

  }

  /**
   * 插入hbase
   *
   */
  def invokeIndicatorDataSite(value: OfflineIndicatorResult): Unit = {
    try {
      //      logger.warn(s"invokeRunDataSite_step1: " + value)
      // rowkey查询的主要字段
      val TOOL_ID = value.toolName
      val CHAMBER_ID = value.chamberName
      val INDICATOR_ID = value.indicatorId

      val indicatorSpecialID = TOOL_ID + CHAMBER_ID + INDICATOR_ID

      val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)), 4, '0')

      val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))

      //      logger.warn(s"invokeRunDataSite_step2")

      val timestatmp = value.windowStartTime
      val reverseTime = Long.MaxValue - timestatmp.toLong

      val rowkey = s"${slatKey}|${indicatorSpecialMd5}|${reverseTime}"


      val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_OFFLINE_INDICATOR_DATA_TABLE))
      val put = new Put(Bytes.toBytes(rowkey))

      val RUN_ID = value.runId
      val INDICATOR_NAME = value.indicatorName
      val INDICATOR_VALUE = value.indicatorValue.toString
      val RUN_START_TIME = value.runStartTime
      val RUN_END_TIME = value.runEndTime
      val PRODUCT_ID = ""
      val STAGE = ""
      val ALARM_LEVEL = "-1"
      val LIMIT = "N/N/N/N/N/N"
      val OCAP = "N/A"

      //      logger.warn(s"invokeRunDataSite_step3 PRODUCT_ID: " + PRODUCT_ID + "\tSTAGE: " + STAGE)

      val RULE_TRIGGER = "N/A"
      val CONTROL_PLAN_VERSION = 0
      val INDICATOR_TIME = 0L
      val ALARM_TIME: Long = 0L
      val RECIPE = ""
      val WINDOW_START_TIME = value.windowStartTime
      val WINDOW_END_TIME = value.windowEndTime

      //      logger.warn(s"invokeRunDataSite_step3")

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_ID"), Bytes.toBytes(TOOL_ID))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_ID"), Bytes.toBytes(CHAMBER_ID))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(RECIPE))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_ID"), Bytes.toBytes(INDICATOR_ID))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(INDICATOR_NAME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_VALUE"), Bytes.toBytes(INDICATOR_VALUE))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT_ID"), Bytes.toBytes(PRODUCT_ID))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(STAGE))

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_TIME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_TIME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_START_TIME"), Bytes.toBytes(WINDOW_START_TIME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_END_TIME"), Bytes.toBytes(WINDOW_END_TIME))

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes(ALARM_LEVEL))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LIMIT"), Bytes.toBytes(LIMIT))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("OCAP"), Bytes.toBytes(OCAP))

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE_TRIGGER"), Bytes.toBytes(RULE_TRIGGER))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_VERSION"), Bytes.toBytes(CONTROL_PLAN_VERSION))

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_TIME"), Bytes.toBytes(INDICATOR_TIME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_TIME))

      table.put(put)
      table.close()

      //      logger.warn(s"invokeRunDataSite_step4: ")
    }catch {
      case ex: Exception => logger.warn(s"alarm job invokeIndicatorDataSite Exception:$ex data: $value")
    }
  }

  def hasLength(str: String): String = {
    if(str != null){
      str
    }else{
      ""
    }
  }
}
