package com.hzw.fdc.function.online.MainFabAlarmHbase

import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}


class AlarmStatisticHbaseSink extends RichSinkFunction[(String, String, String)] {

  var connection: Connection = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmStatisticHbaseSink])

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
  override def invoke(value: (String, String, String), context: SinkFunction.Context): Unit = {
    try {
      //      logger.warn(s"invokeAlarmStatisticHbaseSink_start: " + value)
      val rowkey = value._1
      val column = value._2
      val f = value._3
      val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_ALARM_STATISTIC_TABLE))
      try {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes(f), Bytes.toBytes("STATISTIC"), Bytes.toBytes(column))
        table.put(put)
      } catch {
        case ex: Exception => logger.warn(s"alarm job invokeAlarmStatisticHbaseSink Exception:$ex data: $value")
      } finally {
        table.close()
      }
      //      logger.warn(s"invokeAlarmStatisticHbaseSink_end")
    } catch {
      case ex: Exception => logger.warn(s"alarm job invokeAlarmStatisticHbaseSink Exception:$ex data: $value")
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
