package com.hzw.fdc.service.offline

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.TService
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor.{FdcOfflineVIrtualResultProcessFunction, FdcOfflineVirtualKeyProcessFunction, OfflineVirtualSensorKeyedBroadcastProcessFunction, OfflineVirtualSensorOpentsdbQueryRequestFunction, OfflineWriteOpentsdbFunction, VirtualSensorAsyncOpenTSDBRequest, VirtualSensorProcessWindowFunction}
import com.hzw.fdc.function.offline.MainFabOfflineWindow.{AsyncOpenTSDBRequest, OfflineOpentsdbQueryRequestFunction, VirtualSensorCountTrigger}
import com.hzw.fdc.json.JsonUtil.beanToJsonNode
import com.hzw.fdc.scalabean.{OfflineOpentsdbResult, OfflineSensorResultScala, OfflineTask, OfflineVirtualSensorElemResult, OfflineVirtualSensorOpentsdbResult, VirtualResult}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, OutputTag}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}
import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.TDao
import com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor.{OfflineVirtualSensorOpentsdbQueryRequestFunction, VirtualSensorAsyncOpenTSDBRequest}
import com.hzw.fdc.scalabean.{OfflineOpentsdbResult, OfflineTask, OfflineVirtualSensorOpentsdbResult, OfflineVirtualSensorTask, VirtualResult, VirtualSensorDps}
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream}
import com.hzw.fdc.json.MarshallableImplicits._
import java.util
import java.util.concurrent.TimeUnit

import com.hzw.fdc.dao.MainFabOfflineVirtualSensorDao
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows


class MainFabOfflineVirtualSensorService extends TService {

  private val Dao = new MainFabOfflineVirtualSensorDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineVirtualSensorService])

  lazy val virtualSensorElemOutput = new OutputTag[OfflineVirtualSensorElemResult]("virtualSensorElem")

  /**
   * 获取
   *
   * @return
   */
  override def getDao() = Dao

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {


    val offlineStreamTask: DataStream[JsonNode] = getDatas()

    //根据MainFabCore下发的虚拟sensor离线任务，从opentsdb里获取rawData
    val offlineVirtualDataStream: DataStream[OfflineVirtualSensorOpentsdbResult] = getOfflineVirtualSensorTaskRawData(offlineStreamTask)
      .keyBy(x => x.runId)
      .process(new FdcOfflineVirtualKeyProcessFunction())

    // virtual sensor 离线计算
    val offlineVirtualResultDataStream = offlineVirtualDataStream
      .keyBy(x => x.runId)
      .process(new OfflineVirtualSensorKeyedBroadcastProcessFunction)


    // 设置翻滚窗口,聚合rawData数据
    val virtualSensorResultStream = offlineVirtualResultDataStream
      .keyBy(data => {data.taskId.toString})
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .trigger(new VirtualSensorCountTrigger())
      .process(new VirtualSensorProcessWindowFunction())
      .name("offline virtual sensor Tumble")
      .uid("offline virtual sensor Tumble")

    // 虚拟sensor离线计算写入opentsdb
    virtualSensorResultStream
      .addSink(new OfflineWriteOpentsdbFunction())
      .name(MainFabConstants.OpenTSDBSink)
      .uid("offline virtual sensor To OpenTSDB Sink")


    // 配置信息
    val offlineConfigBroadcastStream = offlineStreamTask

    // 往后台发送改虚拟sensor计算的结果
    val offlineSensorResultScalaStream: DataStream[OfflineSensorResultScala] = offlineVirtualResultDataStream
      .map(x => beanToJsonNode[OfflineVirtualSensorOpentsdbResult](x))
      .union(offlineConfigBroadcastStream)
      .keyBy(data => {
        val taskId = data.findPath("taskId").asText()
        taskId
      })
      .process(new FdcOfflineVIrtualResultProcessFunction)
      .name("FdcOfflineResultStream")

    val virtualSensorElemStream = offlineSensorResultScalaStream.getSideOutput(virtualSensorElemOutput)

    //离线计算virtual sensor任务成功返回结果
    offlineSensorResultScalaStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC,
      new FdcKafkaSchema[OfflineSensorResultScala](ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC
        , (e: OfflineSensorResultScala) => e.dataType
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaWindowSink)
      //添加uid用于监控
      .uid("offline virtual sensor task To kafka")


    virtualSensorElemStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC,
      new FdcKafkaSchema[OfflineVirtualSensorElemResult](ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC
        , (e: OfflineVirtualSensorElemResult) => e.dataType
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("offline virtual sensor elem task To kafka")
      //添加uid用于监控
      .uid("offline virtual sensor elem task To kafka")
  }

  /**
   * 获得离线virtual sensor任务消息
   */
  override protected def getDatas(): DataStream[JsonNode] = {

    val taskDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_OFFLINE_TASK_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_VIRTUAL_SENSOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_OFFLINE_VIRTUAL_SENSOR_TASK_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_OFFLINE_VIRTUAL_SENSOR_TASK_KAFKA_SOURCE_UID
    ).filter(json => {
      val dataType = json.findPath("dataType").asText()
      dataType.equals("OfflineVirtualSensorCalculate")
    })

    taskDataStream
  }

  /**
   * 获得virtualSensor离线任务要计算的rawData
   *
   * @return (任务信息，相应的run的rawData)
   */
  def getOfflineVirtualSensorTaskRawData(offlineStreamTask: DataStream[JsonNode]): DataStream[VirtualResult] = {

    val data = offlineStreamTask
      .keyBy(json => {
        val taskId = json.findPath("taskId").asText()
        taskId
      })
      .flatMap(new OfflineVirtualSensorOpentsdbQueryRequestFunction)

    AsyncDataStream.unorderedWait(
      data,
      new VirtualSensorAsyncOpenTSDBRequest(),
      5000,
      TimeUnit.MILLISECONDS,
      100)

  }

}
