package com.hzw.fdc.dao

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.TDao
import com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor.{OfflineVirtualSensorOpentsdbQueryRequestFunction, VirtualSensorAsyncOpenTSDBRequest}
import com.hzw.fdc.function.offline.MainFabOfflineWindow.{AsyncOpenTSDBRequest, OfflineOpentsdbQueryRequestFunction}
import com.hzw.fdc.scalabean.{OfflineOpentsdbQuery, OfflineOpentsdbResult, OfflineTask, OfflineVirtualSensorOpentsdbResult, OfflineVirtualSensorTask, VirtualResult, VirtualSensorDps}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream}
import com.hzw.fdc.json.MarshallableImplicits._

import java.util
import java.util.concurrent.TimeUnit

/**
 * @author ：gdj
 * @date ：Created in 2021/5/22 14:53
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineWindowDao extends TDao {

  val taskDataStream = getKafkaJsonSource(
    ProjectConfig.KAFKA_OFFLINE_TASK_TOPIC,
    ProjectConfig.KAFKA_QUORUM,
    ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_WINDOW_JOB,
    MainFabConstants.latest,
    MainFabConstants.MAIN_FAB_OFFLINE_WINDOW_TASK_KAFKA_SOURCE_UID,
    MainFabConstants.MAIN_FAB_OFFLINE_WINDOW_TASK_KAFKA_SOURCE_UID
  )

  /**
   * 获得离线indicator任务消息
   */
  def getOfflineTask(): DataStream[JsonNode] = {


    taskDataStream.filter(json => {

      val dataType = json.findPath("dataType").asText()
      dataType.equals("OfflineIndicatorCalculate")
    })

  }

//  /**
//   * 获得离线virtual sensor任务消息
//   */
//  def getOfflineVirtualSensorTask(): DataStream[JsonNode] = {
//
//    taskDataStream.filter(json => {
//      val dataType = json.findPath("dataType").asText()
//      dataType.equals("OfflineVirtualSensorCalculate")
//
//    })
//
//  }

  /**
   * 异步查询opentsdb，获得离线任务要计算的rawData
   *
   * @return (任务信息，相应的run的rawData)
   */
  def getOfflineTaskRawData(): DataStream[(OfflineTask, OfflineOpentsdbResult)] = {

    val data: DataStream[OfflineOpentsdbQuery] = getOfflineTask()
      .keyBy(json => {
        val batchId = json.findPath("batchId").asText()
        scala.util.Random.nextInt(10) +  "|"  + batchId
      })
      .process(new OfflineOpentsdbQueryRequestFunction())

    AsyncDataStream.orderedWait(
      data,
      new AsyncOpenTSDBRequest(),
      11000,
      TimeUnit.MILLISECONDS,
      50)
  }

//  /**
//   * 获得virtualSensor离线任务要计算的rawData
//   *
//   * @return (任务信息，相应的run的rawData)
//   */
//  def getOfflineVirtualSensorTaskRawData(): DataStream[VirtualResult] = {
//
//    val data = getOfflineVirtualSensorTask()
//      .flatMap(new OfflineVirtualSensorOpentsdbQueryRequestFunction)
//      .rebalance
//
//    AsyncDataStream.unorderedWait(
//      data,
//      new VirtualSensorAsyncOpenTSDBRequest(),
//      5000,
//      TimeUnit.MILLISECONDS,
//      100)
//
//  }

}
