package com.hzw.fdc.function.online.MainFabWindow

import com.hzw.fdc.scalabean.{ErrorCode, MainFabRawData, RunData, RunEventData}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}
import com.hzw.fdc.json.MarshallableImplicits._

/**
 * @author gdj
 * @create 2020-06-06-19:21
 *
 */
class RunDataHbaseSink(tableName: String)  extends RichSinkFunction[RunData] {
  var connection: Connection = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[RunDataHbaseSink])

  override def open(parameters: Configuration): Unit = {
    // 获取全局配置变量
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
   */
  override def invoke(value: RunData, context: SinkFunction.Context): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))

    try {
      val product = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.product.getOrElse("") else "").mkString(",")

      val stage = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.stage.getOrElse("") else "").mkString(",")
      val route = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.route.getOrElse("") else "").mkString(",")
      val technology = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.technology.getOrElse("") else "").mkString(",")
      val lotType = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.lotType.getOrElse("") else "").mkString(",")
      val wafers = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.wafers.map(y=>y.get.waferName.getOrElse("")).mkString("|")else "").mkString(",")
      val operation = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.operation.getOrElse("") else "").mkString(",")
      val layer = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.layer.getOrElse("") else "").mkString(",")
      val carrier = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.carrier.getOrElse("") else "").mkString(",")
      val lotName = value.lotMESInfo.map(x=>if(x.nonEmpty) x.get.lotName.getOrElse("") else "").mkString(",")

      val key = s"${value.toolName}_${value.chamberName}".hashCode % 10
      val put = new Put(Bytes.toBytes(s"${key}_${value.toolName}_${value.chamberName}_${value.runStartTime}"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(value.runId))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_NAME"), Bytes.toBytes(value.toolName))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_NAME"), Bytes.toBytes(value.chamberName))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(value.recipe))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(value.runStartTime))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(value.createTime))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT"), Bytes.toBytes(product))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ROUTE"), Bytes.toBytes(route))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(stage))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TECHNOLOGY"), Bytes.toBytes(technology))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TYPE"), Bytes.toBytes(lotType))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WAFER_NAME"), Bytes.toBytes(wafers))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("OPERATION"), Bytes.toBytes(operation))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LAYER"), Bytes.toBytes(layer))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CARRIER"), Bytes.toBytes(carrier))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOT_NAME"), Bytes.toBytes(lotName))


      
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MATERIAL_NAME"), Bytes.toBytes(value.materialName))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("DCType"), Bytes.toBytes(value.DCType))
      if(value.dataMissingRatio.nonEmpty)
      {put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MISSING_RATIO"), Bytes.toBytes(value.dataMissingRatio.get))}
      if(value.runEndTime.nonEmpty)
      {put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(value.runEndTime.get))}
      if(value.timeRange.nonEmpty)
      {put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PROCESS_TIME"), Bytes.toBytes(value.timeRange.get))}
      if(value.step.nonEmpty)
      {put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STEP"), Bytes.toBytes(value.step.get))}
      if(value.runDataNum.nonEmpty)
      {put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_DATA_NUM"), Bytes.toBytes(value.runDataNum.get))}

      table.put(put)

    }catch {
      case exception: Exception =>logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(), Map("MainFabRawDataListSize" -> "","RunEventEnd" -> ""), exception.toString).toJson)
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


  def hasLength(str: String): String = {
    if (str != null) {
      str
    } else {
      ""
    }
  }

}
