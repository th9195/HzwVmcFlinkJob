package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{AlarmRuleResult, FdcData, MainFabRawData, RunData, RunEventData}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-06-06-19:21
 *
 */
class WindowEndRunDataHbaseSink(tableName: String) extends RichSinkFunction[JsonNode] {
  var connection: Connection = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[WindowEndRunDataHbaseSink])

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

//  override def timeout(input: JsonNode, resultFuture: ResultFuture[String]): Unit = {
//    super.timeout(input, resultFuture)
//    resultFuture.complete(List("timeout"))
//  }

  /**
   * 数据写入
   */
  override def invoke(value: JsonNode): Unit = {
    var table: Table = null
    try {

      table = connection.getTable(TableName.valueOf(tableName))

      val runStartEvent = toBean[FdcData[RunData]](value).datas

      val runid = s"${runStartEvent.toolName}--${runStartEvent.chamberName}--${runStartEvent.runStartTime}"

      val product = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("") else "").mkString(",")
      val stage = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("") else "").mkString(",")
      val route = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.route.getOrElse("") else "").mkString(",")
      val technology = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.technology.getOrElse("") else "").mkString(",")
      val lotType = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.lotType.getOrElse("") else "").mkString(",")

      val wafers = try {
          runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.wafers.map(y => y.get.waferName.getOrElse("")).mkString("|") else "").mkString(",")
        }catch {
          case e: Exception => logger.warn(s"${runStartEvent.lotMESInfo}\t ${e.toString}")
            ""
        }
      val operation = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.operation.getOrElse("") else "").mkString(",")
      val layer = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.layer.getOrElse("") else "").mkString(",")
      val carrier = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.carrier.getOrElse("") else "").mkString(",")
      val lotName = runStartEvent.lotMESInfo.map(x => if (x.nonEmpty) x.get.lotName.getOrElse("") else "").mkString(",")


      val key = s"${runStartEvent.toolName}_${runStartEvent.chamberName}".hashCode % 10
      val put = new Put(Bytes.toBytes(s"${key}_${runStartEvent.toolName}_${runStartEvent.chamberName}_${runStartEvent.runStartTime}"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(runid))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_NAME"), Bytes.toBytes(runStartEvent.toolName))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_NAME"), Bytes.toBytes(runStartEvent.chamberName))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(runStartEvent.runStartTime))

      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(System.currentTimeMillis()))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(runStartEvent.recipe))

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
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MATERIAL_NAME"), Bytes.toBytes(runStartEvent.materialName))

      if(runStartEvent.errorCode.isDefined){
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ERROR_CODE"), Bytes.toBytes(runStartEvent.errorCode.get))
      }

      if(runStartEvent.timeRange.isDefined){
        if(runStartEvent.timeRange.get != -1L){
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PROCESS_TIME"), Bytes.toBytes(runStartEvent.timeRange.get))
        }
      }

      if(runStartEvent.dataMissingRatio.isDefined) {
        if (runStartEvent.dataMissingRatio.get != -1) {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MISSING_RATIO"), Bytes.toBytes(runStartEvent.dataMissingRatio.get))
        }
      }

      if(runStartEvent.runEndTime.isDefined) {
        if (runStartEvent.runEndTime.get != -1L) {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(runStartEvent.runEndTime.get))
        }
      }

      if(runStartEvent.runDataNum.isDefined) {
        if (runStartEvent.runDataNum.get != -1L) {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_DATA_NUM"), Bytes.toBytes(runStartEvent.runDataNum.get))
        }
      }

      if(runStartEvent.step.isDefined){
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STEP"), Bytes.toBytes(runStartEvent.step.get))
      }
      table.put(put)
    }catch {
      case ex: Exception => logger.warn(s"alarmHbaseSink  invokeIndicatorDataSite Exception:${ExceptionInfo.getExceptionInfo(ex)}")
    }finally {
      if(table != null){
        table.close()
      }
    }
//    finally {
//      resultFuture.complete(List("ok"))
//    }

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
