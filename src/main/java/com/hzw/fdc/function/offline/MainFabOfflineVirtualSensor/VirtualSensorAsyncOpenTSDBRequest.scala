package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor

import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.http.{HttpResponse, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.concurrent.FutureCallback
import org.apache.http.config.SocketConfig
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, DefaultHttpRequestRetryHandler, HttpClientBuilder, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager
import org.apache.http.impl.nio.reactor.{DefaultConnectingIOReactor, IOReactorConfig}
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks


class VirtualSensorAsyncOpenTSDBRequest extends RichAsyncFunction[OfflineVirtualSensorOpentsdbQuery, VirtualResult] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VirtualSensorAsyncOpenTSDBRequest])

//  private var client: CloseableHttpClient = _

  override def open(parameters: Configuration): Unit = {

    //获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

//
//    //请求超时配置
//    val requestConfig = RequestConfig.custom
//      .setConnectTimeout(5000) //连接超时,连接建立时间,三次握手完成时间
//      .setSocketTimeout(5000) //请求超时,数据传输过程中数据包之间间隔的最大时间
//      .setConnectionRequestTimeout(3000).build //使用连接池来管理连接,从连接池获取连接的超时时间
//
//    val socketConfig = SocketConfig.custom()
//      .setTcpNoDelay(true)
//      .setSoReuseAddress(true)
//      .setSoLinger(60)
//      .setSoTimeout(500)
//      .setSoKeepAlive(true)
//      .build()
//
//    val requestRetryHandler = new DefaultHttpRequestRetryHandler(0, false)
//
//    val connManager = new PoolingHttpClientConnectionManager()
//    connManager.setMaxTotal(1500)
//    connManager.setDefaultMaxPerRoute(150)
//    connManager.setDefaultSocketConfig(socketConfig)
//
//
//    //异步httpClient
//    client = HttpClients.custom
//      .setConnectionManager(connManager)
//      .setDefaultRequestConfig(requestConfig)
//      .setRetryHandler(requestRetryHandler)
//      .build

  }

  override def asyncInvoke(input: OfflineVirtualSensorOpentsdbQuery, resultFuture: async.ResultFuture[VirtualResult]): Unit = {

    var response: CloseableHttpResponse = null
    val httpPost: HttpPost = null
    var httpClient: CloseableHttpClient = null

    try {
      //创建HttpClient对象
      httpClient  = HttpClientBuilder.create().build()

      val startTime = System.currentTimeMillis()
      val task = input.task
      if (input.queryStr.nonEmpty) {
        logger.warn(s"${task} 查询语句: ${input.queryStr}")
        val httpPost = new HttpPost(ProjectConfig.OPENTSDB_RAWDATA_URL)
        httpPost.setEntity(new StringEntity(input.queryStr))
        httpPost.addHeader("Content-Type", "application/json")

        response = httpClient.execute(httpPost)
        val statusCode = response.getStatusLine.getStatusCode
        if(HttpStatus.SC_OK == statusCode){
          val entity = response.getEntity
          /**
           * 查询成功返回
           * {"metric":"RNITool_31.YMTC_chamber.RNI01SEN030","values":[{"time":1613706210066,"value":408.0,"stepId":"1"}],"errorMsg":""}
           * 当errorMsg不为空时 查询出现错误
           */
          if(null != entity){

            val str = EntityUtils.toString(entity, "UTF-8")
            val list = str.fromJson[List[OpenTSDBHttpResponse]]
            val metric = list.head.metric
            val spilt = metric.split("\\.")
            val toolName = spilt(0)
            val chamberName = spilt(1)
            val sensorAliasName = spilt(2)

            val resultList = mutable.ListBuffer[VirtualSensorDps]()
            list.foreach(x => {
//              logger.info(s"query result size ${x.dps.size}")
              if(x.dps.nonEmpty) {
                x.dps.map(y => {
                  val time = y._1.toLong
                  val stepId = x.tags.getOrElse("stepId", "1")
                  val stepName = x.tags.getOrElse("stepName", "unknown")
                  val unit = x.tags.getOrElse("unit", "unknown")
                  resultList.append(
                    VirtualSensorDps(stepId = stepId,
                      stepName = stepName,
                      unit = unit,
                      timestamp=time,
                      sensorValue = y._2
                    )
                  )
                })
              }
            })

            val taskToResult: ListBuffer[VirtualResult] = new ListBuffer[VirtualResult]()

            if(resultList.nonEmpty) {
              val runDataList = input.task.runData

              val runRawDataMap = TrieMap[String,ListBuffer[VirtualSensorDps]]()
              resultList.foreach(rawData => {
                val timestamp = rawData.timestamp
                val loop = new Breaks
                loop.breakable{
                  runDataList.foreach(runData => {
                    val runId = runData.runId
                    val runIdSpit = runId.split("--", -1)
                    val configToolName = runIdSpit(0)
                    val configChamberName = runIdSpit(1)
                    val runStart = runData.runStart
                    val runEnd = runData.runEnd

                    if(toolName == configToolName &&
                      chamberName == configChamberName &&
                      timestamp > runStart &&
                      timestamp < runEnd
                    ){
                      val virtualSensorList: ListBuffer[VirtualSensorDps] = runRawDataMap.getOrElse(runId,new ListBuffer[VirtualSensorDps])
                      virtualSensorList += rawData
                      runRawDataMap.put(runId,virtualSensorList)
                      loop.break()
                    }
                  })
                }
              })

              runRawDataMap.foreach(runRawDataElem => {
                val runId = runRawDataElem._1
                val runRawdataList = runRawDataElem._2
                if(runRawdataList.nonEmpty){
                  val virtualRun = VirtualResult(taskId = task.taskId,
                    toolName = toolName,
                    chamberName=chamberName,
                    sensorAliasName = sensorAliasName,
                    batchId=task.batchId,
                    runId=runId,
                    task = task,
                    dps = runRawdataList
                  )
                  taskToResult.append(virtualRun)
                }else{
                  logger.warn(s"input == ${input.toJson} ; runId == ${runId} ; runRawdataList is empty ; ")
                }
              })
              if(taskToResult.nonEmpty){
                resultFuture.complete(taskToResult)
              }else{
                logger.warn(s"input == ${input.toJson} taskToResult is empty")
              }

            }else{
              logger.warn(s"input == ${input.toJson} opentsdb sensor is empty")
            }
            val endTime = System.currentTimeMillis()
            logger.warn(s"${task} virtual sensor完成一个异步任务 状态：成功 耗时: ${endTime - startTime} run size：${input.task.runData.size}" +
              s" resultSize: ${resultList.size}")
          }else{
            logger.warn(s"input == ${input.toJson} 查询结果entity为空 ")
          }
        }else{
          logger.warn(s"input == ${input.toJson} 查询结果statusCode异常！statusCode == ${statusCode}")
        }
      }else{
        logger.warn(s"input == ${input.toJson} 查询语句为空! queryStr ==  ${input.queryStr}")
      }

    } catch {
      case e: Exception => logger.warn(s" MainFabOfflineWindowApplication asyncInvoke error : ${ExceptionInfo.getExceptionInfo(e)}  input ： $input")
        resultFuture.complete(Iterable(getEmptyResult(input.task.taskId, input.task)))
    }finally {
      if(httpClient != null){
        try{
          httpClient.close()
        }catch {
          case ex: Exception => logger.warn("httpClient error: " + ex.toString)
        }
      }
    }
  }

  override def timeout(input: OfflineVirtualSensorOpentsdbQuery, resultFuture: ResultFuture[VirtualResult]): Unit = {

    logger.warn(s"RichAsyncFunction timeout  run size：${input.task.runData.size}")
    resultFuture.complete(Iterable(getEmptyResult(input.task.taskId, input.task)))
  }

  override def close(): Unit = {
    super.close()
//    client.close()
  }

  /**
   *  返回空的结果
   */
  def getEmptyResult(taskId: Long, task: OfflineVirtualSensorTask): VirtualResult = {
    val res = VirtualResult(0L, "", "", "", 0L, "", task, ListBuffer())
    res
  }
}
