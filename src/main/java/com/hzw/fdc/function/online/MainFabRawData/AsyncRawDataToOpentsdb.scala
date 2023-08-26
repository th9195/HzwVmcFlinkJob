package com.hzw.fdc.function.online.MainFabRawData

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager
import org.apache.http.impl.nio.reactor.{DefaultConnectingIOReactor, IOReactorConfig}
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}
import java.text.DecimalFormat

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import com.hzw.fdc.json.MarshallableImplicits._

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
import scala.util.matching.Regex


class AsyncRawDataToOpentsdb extends RichSinkFunction[JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AsyncRawDataToOpentsdb])


  var fmt: DecimalFormat = _
  var numberPattern: Regex = "^[A-Za-z0-9.\\-\\_\\/]+$".r

  private var client: CloseableHttpAsyncClient = _
  var putUrl: String = _

  /**
   *   增加一个重试写入功能, 允许重试写入三次
   */

  override def open(parameters: Configuration): Unit = {

    //获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)


    //请求超时配置，hbase.rpc.timeout超时时间是10秒,所以scoket超时设置成10秒
    val requestConfig = RequestConfig.custom
      .setConnectTimeout(11000) //连接超时,连接建立时间,三次握手完成时间
      .setSocketTimeout(11000) //请求超时,数据传输过程中数据包之间间隔的最大时间
      .setConnectionRequestTimeout(6000).build //使用连接池来管理连接,从连接池获取连接的超时时间

    //配置io线程
    val ioReactorConfig = IOReactorConfig.custom
      .setIoThreadCount(Runtime.getRuntime.availableProcessors)
      .setSoKeepAlive(true).build

    //设置连接池大小
    val ioReactor = new DefaultConnectingIOReactor(ioReactorConfig)
    val connManager = new PoolingNHttpClientConnectionManager(ioReactor)
    connManager.setMaxTotal(2000)
    connManager.setDefaultMaxPerRoute(1500)

    //异步httpClient
    client = HttpAsyncClients.custom
      .setConnectionManager(connManager)
      .setDefaultRequestConfig(requestConfig).build

    client.start()
    putUrl = ProjectConfig.OPENTSDB_CLIENT_URL + ":" + ProjectConfig.OPENTSDB_CLIENT_PORT + "/api/put?details"
    logger.warn(s"opentsdb put url: $putUrl")
  }

  override def invoke(input: JsonNode, context: SinkFunction.Context): Unit = {
    val cacheList: ListBuffer[OpenTSDBPoint] = ListBuffer()
    try {

      val rawData = toBean[MainFabRawData](input)

      rawData.data.filter(_.sensorValue != Double.NaN).map(x => {

        val metric = s"${rawData.toolName}.${rawData.chamberName}.${x.svid}"

        val tags: mutable.Map[String, String] = mutable.Map()
        if (rawData.stepId.toString.nonEmpty) {
          val stepId = if(rawData.stepId.toString != "-99" )rawData.stepId.toString else "1"
          tags += "stepId" -> stepId
        }
        if (rawData.stepName.nonEmpty) {
          val stepName = numberPattern.findFirstMatchIn(rawData.stepName)
          if(stepName.nonEmpty){
            tags += "stepName" -> rawData.stepName.replace(" ", "_")
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
          tags += "unit" -> tagsUnit
        }

        OpenTSDBPoint(metric = metric,
          value = x.sensorValue,
          timestamp = rawData.timestamp,
          tags = tags.toMap)
      }
      ).grouped(ProjectConfig.RAW_DATA_WRITE_BATCH_COUNT).foreach(dataList => {
        val writeFlag = writeOpentsdb(dataList)
        // 如果写入失败，缓存起来, 重新去写入
        if(!writeFlag){
          cacheList ++= dataList
        }
      })

      List(1, 2).foreach(elem => {
        if(cacheList.nonEmpty){
          val tmpList: List[OpenTSDBPoint] = cacheList.toList
          cacheList.clear()
          tmpList.grouped(50).foreach(dataList => {
            val writeFlag = writeOpentsdb(dataList)
            // 重试第二次，如果写入失败，缓存起来, 重新去写入
            if(!writeFlag && elem == 1){
              cacheList ++= dataList
            }

            // 重试达到三次了, 还有失败的，打印失败信息
            if(!writeFlag && elem == 2){
              logger.warn(ErrorCode("002004d003C", System.currentTimeMillis(), Map("dataList" -> dataList),
                "rawData重试写入三次失败").toJson)
            }
          })
        }
      })
    } catch {
      case e: Exception => logger.warn(ErrorCode("002004d001C", System.currentTimeMillis(), Map("inputMap" -> input),
        ExceptionInfo.getExceptionInfo(e)).toJson)
    }
//    finally {
//      resultFuture.complete(List("ok"))
//    }

  }


  /**
   *   http写入opentsdb
   */
  def writeOpentsdb(dataList: List[OpenTSDBPoint]): Boolean = {
    var writeFlag = true
    val httpPost = new HttpPost(putUrl)
    httpPost.setEntity(new StringEntity(dataList.toJson, "utf-8"))
    httpPost.addHeader("Content-Type", "application/json")
    client.execute(httpPost, new FutureCallback[HttpResponse] {
      override def completed(t: HttpResponse): Unit = {
        val str = EntityUtils.toString(t.getEntity, "UTF-8")
        if (new ObjectMapper().readTree(str).get("errors").iterator().hasNext) {
          logger.warn(s"$str")
          writeFlag = false
        }
      }

      override def failed(e: Exception): Unit = {
        logger.warn(s"save raw data error: ${ExceptionInfo.getExceptionInfo(e)} elem: ${dataList.toJson}")
        writeFlag = false
      }

      override def cancelled(): Unit = {
        logger.warn(s"save raw data cancel")
        writeFlag = false
      }
    })
    writeFlag
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


//  override def timeout(input: JsonNode, resultFuture: ResultFuture[String]): Unit = {
//    logger.warn(s"RichAsyncFunction timeout  run size：$input")
//    resultFuture.complete(List("timeout"))
//  }

  override def close(): Unit = {
    super.close()
    if (client != null) {
      client.close()
    }
  }
}
