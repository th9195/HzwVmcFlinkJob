package com.hzw.fdc.service.offline

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.TService
import com.hzw.fdc.dao.MainFabOfflineWindowDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor.{FdcOfflineVIrtualResultProcessFunction, FdcOfflineVirtualKeyProcessFunction, OfflineVirtualSensorKeyedBroadcastProcessFunction, OfflineWriteOpentsdbFunction, VirtualSensorProcessWindowFunction}
import com.hzw.fdc.function.offline.MainFabOfflineWindow._
import com.hzw.fdc.json.JsonUtil.beanToJsonNode
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class MainFabOfflineWindowService extends TService {

  private val Dao = new MainFabOfflineWindowDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineWindowService])


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

    //根据MainFabCore下发的离线任务，从opentsdb里获取rawData
    val offlineDataStream: DataStream[(OfflineTask, OfflineOpentsdbResult)] = getDao().getOfflineTaskRawData()
      .name("getOfflineTaskRawData")
      .uid("getOfflineTaskRawData")

    //按照run 聚合，为了处理一个run 有多个sensor 要查的情况
    val offlineWindowDataStream: DataStream[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData)] = offlineDataStream
      .filter(x => {x._2.metric != "FAIL"})
      .keyBy(x => s"${x._1.taskId}")
      .process(new MainFabOfflineSensorKeyedProcessFunction())
      .name(MainFabConstants.WindowTask)
      .uid(MainFabConstants.WindowTask)


//    //按照run 聚合，为了处理一个run 有多个sensor 要查的情况
//    val offlineWindowDataStream: DataStream[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData)] = offlineDataStream
//      .filter(x => {x._2.metric != "FAIL"})
//      .keyBy(x => s"${x._1.taskId}|${x._2.runId}")
//      .window(GlobalWindows.create())
//      .trigger(new OpentsdbResultCountTrigger)
//      .process(new MainFabOfflineWindowCountOpentsdbResultProcessFunction)
//      .name(MainFabConstants.WindowTask)
//      .uid(MainFabConstants.WindowTask)

//    //任务内run 全到了，然后再全局排序，按照顺序输出
//    val sortOfflineWindowDataStream = offlineWindowDataStream.keyBy(_._1.taskId)
//      .window(GlobalWindows.create())
//      .trigger(new OfflineTaskCountTrigger)
//      .process(new OfflineTaskCountProcessFunction).name("Aggregate RunData")


//    val offlineWindowStream: DataStream[FdcData[OfflineWindowListData]] = offlineWindowDataStream
//      .keyBy(x => x._1.taskId.toString)
//      .process(new MainFabOfflineWindowKeyedProcessFunction())

    val offlineWindowStream: DataStream[FdcData[OfflineWindowListData]] = if(ProjectConfig.ENABLE_OFFLINE_SPLIT_WINDOW_BY_STEPID){
      offlineWindowDataStream
        .keyBy(x => x._1.taskId.toString)
        .process(new MainFabOfflineWindowKeyedProcessFunctionNew())
        .name("math window New")
        .uid("math window New")
    }else{
      offlineWindowDataStream
        .keyBy(x => x._1.taskId.toString)
        .process(new MainFabOfflineWindowKeyedProcessFunction())
        .name("math window")
        .uid("math window")
    }

    //划分好窗口的数据写入kafka
    offlineWindowStream
      .addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_OFFLINE_WINDOW_DATA_TOPIC,
      new FdcKafkaSchema[FdcData[OfflineWindowListData]](ProjectConfig.KAFKA_OFFLINE_WINDOW_DATA_TOPIC
        //按照taskId分区
        , (e: FdcData[OfflineWindowListData]) => e.datas.taskId.toString
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaWindowSink)
      //添加uid用于监控
      .uid(MainFabConstants.KafkaWindowSink)


//    // virtual sensor 离线计算任务
//    offlineVirtualSensorTask()

  }


//  /**
//   *   virtual sensor 离线计算任务
//   */
//  def offlineVirtualSensorTask(): Unit={
//    //根据MainFabCore下发的虚拟sensor离线任务，从opentsdb里获取rawData
//    val offlineVirtualDataStream: DataStream[OfflineVirtualSensorOpentsdbResult] = getDao().getOfflineVirtualSensorTaskRawData()
//      .keyBy(x => x.runId)
//      .process(new FdcOfflineVirtualKeyProcessFunction())
//
//    // virtual sensor 离线计算
//    val offlineVirtualResultDataStream = offlineVirtualDataStream
//      .keyBy(x => x.runId)
//      .process(new OfflineVirtualSensorKeyedBroadcastProcessFunction)
//
//
//    // 设置翻滚窗口,聚合rawData数据
//    val virtualSensorResultStream = offlineVirtualResultDataStream
//      .filter(x => {
//        val svid = x.data.svid
//        val virtualConfig = x.virtualConfigList.filter(_.svid == svid)
//        if(virtualConfig.nonEmpty) {
//          x.data.svid != "" && virtualConfig.head.needSave
//        }else{
//          false
//        }
//      })
//      .keyBy(data => {data.taskId.toString})
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
//      .trigger(new VirtualSensorCountTrigger())
//      .process(new VirtualSensorProcessWindowFunction())
//      .name("offline virtual sensor Tumble")
//      .uid("offline virtual sensor Tumble")
//
//    // 虚拟sensor离线计算写入opentsdb
//    virtualSensorResultStream
//      .addSink(new OfflineWriteOpentsdbFunction())
//      .name(MainFabConstants.OpenTSDBSink)
//      .uid("offline virtual sensor To OpenTSDB Sink")
//
//
//    // 配置信息
//    val offlineConfigBroadcastStream = getDao().getOfflineVirtualSensorTask()
//
//    // 往后台发送改虚拟sensor计算的结果
//    val offlineSensorResultScalaStream: DataStream[OfflineSensorResultScala] = offlineVirtualResultDataStream
//      .map(x => beanToJsonNode[OfflineVirtualSensorOpentsdbResult](x))
//      .union(offlineConfigBroadcastStream)
//      .keyBy(data => {
//        val taskId = data.findPath("taskId").asText()
//        taskId
//      })
//      .process(new FdcOfflineVIrtualResultProcessFunction)
//      .name("FdcOfflineResultStream")
//
//    val virtualSensorElemStream = offlineSensorResultScalaStream.getSideOutput(virtualSensorElemOutput)
//
//    //离线计算virtual sensor任务成功返回结果
//    offlineSensorResultScalaStream.addSink(new FlinkKafkaProducer(
//      ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC,
//      new FdcKafkaSchema[OfflineSensorResultScala](ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC
//        , (e: OfflineSensorResultScala) => e.dataType
//      )
//      , ProjectConfig.getKafkaProperties()
//      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
//    )).name(MainFabConstants.KafkaWindowSink)
//      //添加uid用于监控
//      .uid("offline virtual sensor task To kafka")
//
//
//    virtualSensorElemStream.addSink(new FlinkKafkaProducer(
//      ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC,
//      new FdcKafkaSchema[OfflineVirtualSensorElemResult](ProjectConfig.KAFKA_OFFLINE_RESULT_TOPIC
//        , (e: OfflineVirtualSensorElemResult) => e.dataType
//      )
//      , ProjectConfig.getKafkaProperties()
//      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
//    )).name("offline virtual sensor elem task To kafka")
//      //添加uid用于监控
//      .uid("offline virtual sensor elem task To kafka")
//  }
}
