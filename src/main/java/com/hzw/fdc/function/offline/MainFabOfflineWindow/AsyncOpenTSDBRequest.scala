package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.fasterxml.jackson.databind.ObjectMapper
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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class AsyncOpenTSDBRequest extends RichAsyncFunction[OfflineOpentsdbQuery, (OfflineTask, OfflineOpentsdbResult)] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AsyncOpenTSDBRequest])

//  private var client: CloseableHttpClient = _

//  private var httpClient: CloseableHttpClient _
  var client: CloseableHttpAsyncClient = _

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

//    //请求超时配置
//    val requestConfig = RequestConfig.custom
//      .setConnectTimeout(4000) //连接超时,连接建立时间,三次握手完成时间
//      .setSocketTimeout(4000) //请求超时,数据传输过程中数据包之间间隔的最大时间
//      .setConnectionRequestTimeout(1000).build //使用连接池来管理连接,从连接池获取连接的超时时间
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
//    connManager.setMaxTotal(200)
//    connManager.setDefaultMaxPerRoute(100)
//    connManager.setDefaultSocketConfig(socketConfig)
//
//
//    //异步httpClient
//    client = HttpClients.custom
//      .setConnectionManager(connManager)
//      .setDefaultRequestConfig(requestConfig)
//      .setRetryHandler(requestRetryHandler)
//      .build

//    client.start()
  }

  override def asyncInvoke(input: OfflineOpentsdbQuery, resultFuture: async.ResultFuture[(OfflineTask, OfflineOpentsdbResult)]): Unit = {
//    var response: CloseableHttpResponse = null
//    var httpPost: HttpPost = null
//    var httpClient: CloseableHttpClient = null
    try {
      //创建HttpClient对象
//      httpClient  = HttpClientBuilder.create().build()
      val startTime = System.currentTimeMillis()
     // val task = input.task
      if (input.queryStr.nonEmpty) {

        //过滤当前run 不对应的sensor
        var taskSvidNum: Int = 1
        val filterSensorList = input.sensorList.filter(s=>{input.chamberName.equals(s.chamberName) && input.toolName.equals(s.toolName)})
        if(filterSensorList.nonEmpty && filterSensorList.size > taskSvidNum){
          taskSvidNum = filterSensorList.size
        }

        logger.warn(s"查询语句: ${input.queryStr} sensorList: ${input.sensorList} taskSvidNum: ${taskSvidNum} " +
          s"${input.toolName}|${input.chamberName}|${input.svid}" )
        val httpPost = new HttpPost(ProjectConfig.OPENTSDB_RAWDATA_URL)
        httpPost.setEntity(new StringEntity(input.queryStr))
        httpPost.addHeader("Content-Type", "application/json")

        client.execute(httpPost, new FutureCallback[HttpResponse] {
          override def completed(t: HttpResponse): Unit = {
//            val str = EntityUtils.toString(t.getEntity, "UTF-8")
              val statusCode = t.getStatusLine.getStatusCode
              if(HttpStatus.SC_OK == statusCode){
                val entity = t.getEntity
                /**
                 * 查询成功返回
                 * {"metric":"RNITool_31.YMTC_chamber.RNI01SEN030","values":[{"time":1613706210066,"value":408.0,"stepId":"1"}],"errorMsg":""}
                 * 当errorMsg不为空时 查询出现错误
                 */
                if(null != entity){
                  val str = EntityUtils.toString(entity, "UTF-8")
                  val list = str.fromJson[List[OpenTSDBHttpResponse]]
                  val rl = mutable.ListBuffer[OfflineOpentsdbResultValue]()
                  if(list.nonEmpty) {

                    list.foreach(x => {
                      if (x.dps.nonEmpty) {
                        val step = x.tags.getOrElse("stepId", "1").toLong
                        x.dps.map(y => {
                          rl.append(OfflineOpentsdbResultValue(y._1.toLong, y._2, step))
                        })
                      }
                    })
                  }

                  val taskToResult: ListBuffer[(OfflineTask, OfflineOpentsdbResult)] = new ListBuffer()
                  // 修复  condition indictor 多chamber 离线计算不出来
                  if(rl.nonEmpty) {
                    val sortRl = rl.sortBy(_.time)
                    val metric = list.head.metric

                    val runData = input.task.runData
                    val runSize = input.task.runData.size
                    var runDataIndex = 0
                    val runRawdataList = mutable.ListBuffer[OfflineOpentsdbResultValue]()

                    //              logger.warn(s"sortRl: ${sortRl} runData: ${runData}")
                    /**
                     *  查询rawTrace 算法结构优化
                     */
                    var runScala = runData(runDataIndex)
                    sortRl.foreach(rawdata => {

                      var flag = true
                      while (flag) {
                        if(runSize > runDataIndex) {
                          //                    val runScala = runData(runDataIndex)

                          val runStart = runScala.runStart
                          val runEnd = runScala.runEnd
                          if (runEnd >= rawdata.time) {
                            // rawTrace大于End,同时小于Start，才会写入列表
                            if (runStart <= rawdata.time) {
                              runRawdataList.append(rawdata)
                              flag = false
                            }else{
                              runRawdataList.clear()
                              flag = false
                            }
                          } else {
                            if (runRawdataList.nonEmpty) {

                              // rawdata 不是当前的run, 结束当前run的数据
                              taskToResult.append((
                                input.task,
                                OfflineOpentsdbResult(metric = metric,
                                  toolName = input.toolName,
                                  chamberName = input.chamberName,
                                  sensorAlias = input.sensorAlias,
                                  runId = runScala.runId,
                                  runStart = runStart,
                                  runEnd = runEnd,
                                  dataMissingRatio = 0,
                                  taskSvidNum = taskSvidNum,
                                  values = runRawdataList.toList,
                                  isCalculationSuccessful = true,
                                  calculationInfo = ""
                                )
                              ))
                            }
                            // 跑到下一个run
                            runDataIndex += 1
                            if(runSize > runDataIndex){
                              runScala = runData(runDataIndex)
                            }
                            runRawdataList.clear()
                          }
                        }else{
                          flag = false
                        }
                      }
                    })

                    val run = if(runDataIndex < runSize){
                      runData(runDataIndex)
                    }else{
                      runData.last
                    }

                    if(runRawdataList.nonEmpty){

                      // rawdata 不是当前的run, 结束当前run的数据
                      taskToResult.append((
                        input.task,
                        OfflineOpentsdbResult(metric = metric,
                          toolName = input.toolName,
                          chamberName = input.chamberName,
                          sensorAlias = input.sensorAlias,
                          runId = run.runId,
                          runStart = run.runStart,
                          runEnd = run.runEnd,
                          dataMissingRatio = 0,
                          taskSvidNum = taskSvidNum,
                          values = runRawdataList.toList,
                          isCalculationSuccessful = true,
                          calculationInfo = ""
                        )
                      ))
                      //                logger.warn(s"indicator runId ：${runData.last.runId} taskToResult: ${taskToResult}")
                    }

                    if(taskToResult.nonEmpty){
                      //                logger.warn("taskToResult:" + taskToResult )
                      resultFuture.complete(taskToResult)
                    }
                    val endTime = System.currentTimeMillis()
                    logger.warn(s"indicator 完成一个异步任务 状态：成功 耗时: ${endTime - startTime} run size：${input.task.runData.size}")
                  }
                }
              }
          }

          override def failed(e: Exception): Unit = {
            logger.warn(s"save raw data error: ${ExceptionInfo.getExceptionInfo(e)} elem: ${input.queryStr}")

          }

          override def cancelled(): Unit = {
            logger.warn(s"save raw data cancel")

          }
        })

//        response = client.execute(httpPost)

      }
    } catch {
      case e: Exception => logger.warn(s" MainFabOfflineWindowApplication asyncInvoke error : ${ExceptionInfo.getExceptionInfo(e)}  input ： $input")
        resultFuture.complete(Iterable((input.task,
          OfflineOpentsdbResult(metric = "FAIL",
            toolName = input.toolName,
            chamberName = input.chamberName,
            sensorAlias = input.sensorAlias,
            runId = "",
            runStart = 0L,
            runEnd = 0L,
            dataMissingRatio = 0,
            taskSvidNum = 1,
            values = Nil,
            isCalculationSuccessful = false,
            calculationInfo =s"Exception ${e.printStackTrace()}")
        )))
    }finally {
//      if(httpClient != null){
//        try{
//          httpClient.close()
//        }catch {
//          case ex: Exception => logger.warn("httpClient error: " + ex.toString)
//        }
//      }
    }



//    finally {
//      try{
//         if(null != response){
//           response.close()
//         }
//      }catch {
//        case e: Exception => logger.warn(s" response error: $e")
//      }
//
//      try{
//        if(null != httpPost){
//          httpPost.releaseConnection()
//        }
//      }catch {
//        case e: Exception => logger.warn(s" httpPost error: $e")
//      }
//    }
  }

  override def timeout(input: OfflineOpentsdbQuery, resultFuture: ResultFuture[(OfflineTask, OfflineOpentsdbResult)]): Unit = {

    logger.warn(s"RichAsyncFunction timeout  run size：${input.task.runData.size} 查询语句: ${input.queryStr}")
    resultFuture.complete(Iterable((input.task,
      OfflineOpentsdbResult(metric = "FAIL",
        toolName = input.toolName,
        chamberName = input.chamberName,
        sensorAlias = input.sensorAlias,
        runId = "",
        runStart = 0L,
        runEnd = 0L,
        dataMissingRatio = 0,
        taskSvidNum = 1,
        values = Nil,
        isCalculationSuccessful = false,
        calculationInfo = "查询opentsdb timeout")
    )))


  }

  override def close(): Unit = {
    super.close()
//    client.close()
  }
}
