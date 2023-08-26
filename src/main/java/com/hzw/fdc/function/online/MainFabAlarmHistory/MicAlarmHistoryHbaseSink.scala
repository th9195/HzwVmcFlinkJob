package com.hzw.fdc.function.online.MainFabAlarmHistory

import com.hzw.fdc.scalabean.AlarmMicRule
import com.hzw.fdc.util.{ProjectConfig, SnowFlake}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.{Date, Optional}

@Deprecated
class MicAlarmHistoryHbaseSink extends RichSinkFunction[AlarmMicRule] {

  var connection: Connection = _
  val snowFlake: SnowFlake = new SnowFlake(0, 1)
  private val logger: Logger = LoggerFactory.getLogger(classOf[MicAlarmHistoryHbaseSink])

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
  override def invoke(value: AlarmMicRule, context: SinkFunction.Context): Unit = {
    try {
//      logger.warn(s"invokeMicAlarmHistoryHbaseSink_start: " + value)
      val TOOL_NAME = value.toolName
      val CHAMBER_NAME = value.chamberName
      val toolMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(TOOL_NAME))
      val saltKey = StringUtils.leftPad(Integer.toString(Math.abs(toolMd5.hashCode % 10000)), 4, '0')

      val timestatmp = value.alarmCreateTime
      val reverseTime = Long.MaxValue - timestatmp

      val alarmNo = snowFlake.nextId().toString
      val rowkey = s"$saltKey|$toolMd5|$reverseTime|$alarmNo"

      val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_ALARM_HISTROY_TABLE))
      val put = new Put(Bytes.toBytes(rowkey))

      val RUN_ID = value.runId
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val RUN_START_T = format.format(new Date(value.runStartTime))
      val RUN_END_T = format.format(new Date(value.runEndTime))
      val ALARM_T = format.format(new Date(timestatmp))

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_NAME"), Bytes.toBytes(TOOL_NAME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_NAME"), Bytes.toBytes(CHAMBER_NAME))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(if (Optional.ofNullable(value.micName).isPresent) value.micName else ""))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_NAME"), Bytes.toBytes(if (Optional.ofNullable(value.controlPlanName).isPresent) value.controlPlanName else ""))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes("0"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE"), Bytes.toBytes(""))
      val actionType = value.action.map(x => x.`type`)
      if (actionType.nonEmpty) {
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION_NAME"), Bytes.toBytes(actionType.reduce(_ + "," + _)))
      } else {
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION_NAME"), Bytes.toBytes(""))
      }
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_T))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_T))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_T))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(ALARM_T))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("DESCRIPTION"), Bytes.toBytes("mic"))

      table.put(put)
      table.close()

//      logger.warn(s"invokeMicAlarmHistoryHbaseSink_end")
    } catch {
      case ex: Exception => logger.warn(s"alarm job invokeMicAlarmHistoryHbaseSink Exception:$ex data: $value")
    }
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    connection.close()
    super.close()

  }

  def hasLength(str: String): String = {
    if (str != null) {
      str
    } else {
      ""
    }
  }


}
