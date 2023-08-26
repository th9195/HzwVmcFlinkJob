package com.hzw.fdc.function.online.MainFabAlarmHbase

import com.hzw.fdc.scalabean.{AlarmRuleResult, IndicatorLimitResult}
import com.hzw.fdc.util.{ProjectConfig, SnowFlake}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.{Date, Optional}


class AlarmHistoryHbaseSink extends  RichSinkFunction[(AlarmRuleResult, IndicatorLimitResult)] {

  var connection: Connection = _
  val snowFlake: SnowFlake = new SnowFlake(0, 0)
  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmHistoryHbaseSink])

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
  override def invoke(value: (AlarmRuleResult, IndicatorLimitResult), context: SinkFunction.Context): Unit = {
    val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_ALARM_HISTROY_TABLE))
    try {
      //      logger.warn(s"invokeAlarmHistoryHbaseSink_start: " + value)
      val alarmRule = value._1
      val limitResult = value._2
      val TOOL_NAME = alarmRule.toolName
      val CHAMBER_NAME = alarmRule.chamberName
      val toolMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(TOOL_NAME))
      val saltKey = StringUtils.leftPad(Integer.toString(Math.abs(toolMd5.hashCode % 10000)), 4, '0')

      val timestatmp = alarmRule.alarmCreateTime
      val reverseTime = Long.MaxValue - timestatmp

      val alarmNo = snowFlake.nextId().toString
      val rowkey = s"$saltKey|$toolMd5|$reverseTime|$alarmNo"


      val put = new Put(Bytes.toBytes(rowkey))

      val RUN_ID = alarmRule.runId
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val CREATE_TIME = format.format(new Date(alarmRule.windowDataCreateTime))
      val RUN_START_T = format.format(new Date(alarmRule.runStartTime))
      val RUN_END_T = format.format(new Date(alarmRule.runEndTime))
      val ALARM_T = format.format(new Date(timestatmp))

      alarmRule.RULE.foreach(x => {
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_NAME"), Bytes.toBytes(TOOL_NAME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_NAME"), Bytes.toBytes(CHAMBER_NAME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(if (Optional.ofNullable(alarmRule.indicatorName).isPresent) alarmRule.indicatorName else ""))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_NAME"), Bytes.toBytes(if (Optional.ofNullable(alarmRule.controlPlanName).isPresent) alarmRule.controlPlanName else ""))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes(limitResult.alarmLevel.toString))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE"), Bytes.toBytes(x.ruleTypeName))
        val actionTypeList = x.alarmInfo.flatMap(r => r.action).map(r => r.`type`)
        if (actionTypeList.nonEmpty) {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION_NAME"), Bytes.toBytes(actionTypeList.mkString(",")))
        } else {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION_NAME"), Bytes.toBytes(""))
        }
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_T))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_T))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_T))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(CREATE_TIME))
        table.put(put)
      })

    } catch {
      case ex: Exception => logger.warn(s"alarm job invokeAlarmHistoryHbaseSink Exception:$ex data: $value")
    }finally {
      table.close()
    }
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    connection.close()
    super.close()

  }


}
