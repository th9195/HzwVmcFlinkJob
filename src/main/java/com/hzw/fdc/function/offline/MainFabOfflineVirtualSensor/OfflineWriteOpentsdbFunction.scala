package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor

import com.fasterxml.jackson.databind.ObjectMapper
import com.hzw.fdc.scalabean.{ErrorCode, OfflineVirtualSensorOpentsdbResult, OpenTSDBPoint}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
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
import com.hzw.fdc.json.MarshallableImplicits._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *  离线virtual sensor 写入opentsdb
 */
class OfflineWriteOpentsdbFunction extends RichSinkFunction[List[OpenTSDBPoint]] {

  var client: CloseableHttpAsyncClient = _
  var putUrl: String = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineWriteOpentsdbFunction])


  override def open(parameters: Configuration): Unit = {

    //获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)


    //请求超时配置，hbase.rpc.timeout超时时间是10秒,所以scoket超时设置成10秒
    val requestConfig = RequestConfig.custom
      .setConnectTimeout(5000) //连接超时,连接建立时间,三次握手完成时间
      .setSocketTimeout(10000) //请求超时,数据传输过程中数据包之间间隔的最大时间
      .setConnectionRequestTimeout(3000).build //使用连接池来管理连接,从连接池获取连接的超时时间

    //配置io线程
    val ioReactorConfig = IOReactorConfig.custom
      .setIoThreadCount(Runtime.getRuntime.availableProcessors)
      .setSoKeepAlive(true).build

    //设置连接池大小
    val ioReactor = new DefaultConnectingIOReactor(ioReactorConfig)
    val connManager = new PoolingNHttpClientConnectionManager(ioReactor)
    connManager.setMaxTotal(200)
    connManager.setDefaultMaxPerRoute(150)

    //异步httpClient
    client = HttpAsyncClients.custom
      .setConnectionManager(connManager)
      .setDefaultRequestConfig(requestConfig).build

    client.start()
    putUrl = ProjectConfig.OPENTSDB_CLIENT_URL + ":" + ProjectConfig.OPENTSDB_CLIENT_PORT + "/api/put?details"
    logger.warn(s"opentsdb put url: $putUrl")

  }

  /**
   * 每条数据写入
   */
  override def invoke(input: List[OpenTSDBPoint], context: SinkFunction.Context): Unit = {
    try {
      input.grouped(ProjectConfig.RAW_DATA_WRITE_BATCH_COUNT).foreach(dataList => {
        writeOpentsdb(dataList)
      })
    }catch {
      case ex: Exception => logger.warn("opentsdb write error: " + ex.toString)
    }
  }

  /**
   *   http写入opentsdb
   */
  def writeOpentsdb(dataList: List[OpenTSDBPoint]): Boolean = {
    var writeFlag = true
    try {
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
    }catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("function" -> "writeOpentsdb"), ex.toString).toString)
    }
    writeFlag
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    super.close()
    if (client != null) {
      client.close()
    }
  }
}