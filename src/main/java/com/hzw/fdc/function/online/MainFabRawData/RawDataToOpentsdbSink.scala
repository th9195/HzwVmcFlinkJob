package com.hzw.fdc.function.online.MainFabRawData

import java.text.DecimalFormat
import java.util

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{ErrorCode, MainFabRawData}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.opentsdb.client.bean.request.Point
import org.opentsdb.client.bean.response.DetailResult
import org.opentsdb.client.http.callback.BatchPutHttpResponseCallback
import org.opentsdb.client.{OpenTSDBClient, OpenTSDBClientFactory, OpenTSDBConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex


class RawDataToOpentsdbSink() extends RichSinkFunction[JsonNode] {
  var client: OpenTSDBClient = _
  var fmt: DecimalFormat = _
  var numberPattern: Regex = "^[A-Za-z0-9.\\-\\_\\/]+$".r
  private val logger: Logger = LoggerFactory.getLogger(classOf[RawDataToOpentsdbSink])


  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    //创建opentsdb连接
    val openTSDBconfig = OpenTSDBConfig.address(ProjectConfig.OPENTSDB_CLIENT_URL.replace("http://", ""), ProjectConfig.OPENTSDB_CLIENT_PORT)
      // http连接池大小，默认100
      .httpConnectionPool(100)
      // http请求超时时间，默认100s
      .httpConnectTimeout(2000)
      // 异步写入数据时，每次http提交的数据条数，默认50
      .batchPutSize(200)
      // 异步写入数据中，内部有一个队列，默认队列大小20000
      .batchPutBufferSize(50000)
      // 异步写入等待时间，如果距离上一次请求超多300ms，且有数据，则直接提交
      .batchPutTimeLimit(1200)
      // 每批数据提交完成后回调
      .batchPutCallBack(new BatchPutHttpResponseCallback.BatchPutCallBack {
        override def response(points: util.List[Point], result: DetailResult): Unit = {
          //写入成功
        }

        override def responseError(points: util.List[Point], result: DetailResult): Unit = {
          logger.warn("OpenTSDB put rawdata responseError" + points + "result" + result)
        }

        override def failed(points: util.List[Point], e: Exception): Unit = {
          logger.warn("OpenTSDB put failed" + e)
        }
      }).config()

    client = OpenTSDBClientFactory.connect(openTSDBconfig)

    val putUrl = ProjectConfig.OPENTSDB_CLIENT_URL + ":" + ProjectConfig.OPENTSDB_CLIENT_PORT + "/api/put?details"
    logger.warn(s"opentsdb put url: $putUrl")
  }

  /**
   * 每条数据写入
   *
   */
  override def invoke(input: JsonNode, context: SinkFunction.Context): Unit = {
    try {
      val rawData = toBean[MainFabRawData](input)
      rawData.data.foreach(x => {


        val metric = s"${rawData.toolName}.${rawData.chamberName}.${x.svid}"

        val tagsMap = new util.HashMap[String, String]()
        if (rawData.stepId.toString.nonEmpty) {
          val stepId = if(rawData.stepId.toString != "-99" )rawData.stepId.toString else "1"
          tagsMap.put("stepId", stepId)
        }
        if (rawData.stepName.nonEmpty) {
          val stepName = numberPattern.findFirstMatchIn(rawData.stepName)
          if(stepName.nonEmpty){
            tagsMap.put( "stepName", rawData.stepName.replace(" ", "_"))
          }else{
            logger.warn(s"${rawData.toolName}.${rawData.chamberName}.${x.svid} ${rawData.stepName}  " +
              s"stepName no write opentsdb reason: style")
          }
        }
        if (x.unit.nonEmpty) {
          val tagsUnit: String = numberPattern.findFirstMatchIn(x.unit) match {
            case Some(_) => x.unit
            case None => stringToUnicode(x.unit)
          }
          tagsMap.put("unit", tagsUnit)
        }

        val point = Point.metric(metric)
          .tag(tagsMap)
          .value(rawData.timestamp, x.sensorValue)
          .build()
        client.put(point)

      })
    }catch {
      case e: Exception => logger.warn(ErrorCode("002004d001C",
        System.currentTimeMillis(), Map("inputMap" -> input), ExceptionInfo.getExceptionInfo(e)).toJson)
    }
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    client.gracefulClose()
    super.close()

  }

  /**
   *  获取客户端
   */
  def getOpentsdbClientConnect(): Unit = {
    try{

    }catch {
      case e: Exception => logger.warn(ErrorCode("002004d001C",
        System.currentTimeMillis(), Map("getOpentsdbClientConnect" -> ""), ExceptionInfo.getExceptionInfo(e)).toJson)
    }
  }

  /**
   * String转unicode
   *
   * @param unicode
   * @return
   */
  def stringToUnicode(unicode: String): String = {
    val chars = unicode.toCharArray
    val builder = new StringBuilder

    for (i <- 0 until chars.size) {
      val c = unicode.charAt(i)

      builder.append(String.format("\\u%04x", Integer.valueOf(c)))
    }
    builder.toString()
  }
}