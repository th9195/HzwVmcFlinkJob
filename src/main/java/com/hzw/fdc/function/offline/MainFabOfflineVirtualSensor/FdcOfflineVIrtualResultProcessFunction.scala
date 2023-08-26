package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{ErrorCode, OfflineSensorResultScala, OfflineVirtualSensorElemResult, OfflineVirtualSensorOpentsdbResult, OfflineVirtualSensorTask, SensorElem, VirtualConfig, taskIdTimestamp, virtualSensorTimeOut}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.concurrent

/**
 *  虚拟sensor计算
 */
class FdcOfflineVIrtualResultProcessFunction  extends KeyedProcessFunction[String, JsonNode,
  OfflineSensorResultScala] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineVIrtualResultProcessFunction])

  lazy val virtualSensorElemOutput = new OutputTag[OfflineVirtualSensorElemResult]("virtualSensorElem")

  // {"taskId + virtualSensorAliasName": [runId, runId]}}
  val OfflineConfigAll = new concurrent.TrieMap[String, scala.collection.immutable.Set[String]]()

  // 缓存离线计算的数据 {"taskId + virtualSensorAliasName": [runId, runId]}}
  val OfflineCacheDataAll = new concurrent.TrieMap[String, scala.collection.immutable.Set[String]]()

  // 缓存离线计算的sensor数据 {"taskId + virtualSensorAliasName": {"runId":  [OfflineVirtualSensorOpentsdbResult]}}
  val OfflineElemData = new concurrent.TrieMap[String, concurrent.TrieMap[String,
    scala.collection.immutable.Set[OfflineVirtualSensorOpentsdbResult]]]()

  /** The state that is maintained by this process function */
  private var taskIdState: ValueState[virtualSensorTimeOut] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val virtualSensorStateDescription = new ValueStateDescriptor[virtualSensorTimeOut](
      "virtualSensorOutValueState", TypeInformation.of(classOf[virtualSensorTimeOut]))
    taskIdState = getRuntimeContext.getState(virtualSensorStateDescription)
  }

  /**
   * 数据流超时
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String,
    JsonNode, OfflineSensorResultScala]#OnTimerContext, out: Collector[OfflineSensorResultScala]): Unit = {
    try {
      taskIdState.value() match {
        case virtualSensorTimeOut(cacheKey, batchId, lastModified)
          if (System.currentTimeMillis() >= lastModified + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong) =>

          logger.warn(s"onTimer_tep1: $cacheKey \t ${taskIdState.value()}")
          if (OfflineConfigAll.contains(cacheKey)) {

            val taskId = cacheKey.split("\\|").head

            out.collect(OfflineSensorResultScala(taskId.toLong, batchId.toLong, "DONE", "time out", "OfflineVirtualSensorCalculate"))

            OfflineConfigAll.remove(cacheKey)
            OfflineCacheDataAll.remove(cacheKey)
            OfflineElemData.remove(cacheKey)
            taskIdState.clear()
            close()
          }

        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("onTimer" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }

  /**
   * 数据流
   */
  override def processElement(value: JsonNode, ctx: KeyedProcessFunction[String, JsonNode,
    OfflineSensorResultScala]#Context, out: Collector[OfflineSensorResultScala]): Unit = {

    try {
      val dataType = value.findPath("dataType").asText()
      if (dataType == "OfflineVirtualSensorCalculate") {
        val a = toBean[OfflineVirtualSensorTask](value)

        for(virtualConfig <- a.virtualConfigList){
          addOfflineConfig(a, virtualConfig)
          val cacheKey = s"${a.taskId}|${virtualConfig.virtualSensorAliasName}"
          // write the state back
          taskIdState.update(virtualSensorTimeOut(cacheKey, batchId = a.batchId.toString, System.currentTimeMillis()))

          // schedule the next timer 60 seconds from the current event time
          ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong)
        }
      } else {

        val record = toBean[OfflineVirtualSensorOpentsdbResult](value)
        val myDatas = record

        val cacheKey = s"${myDatas.taskId}|${myDatas.data.sensorAlias}"
        val batchId = myDatas.batchId.toString

        try {
          val runId = myDatas.runId

          val lastSensorStatus = record.lastSensorStatus

          // 是要返回给后台的virtualSensorAliasName和taskId
          if(OfflineConfigAll.contains(cacheKey)) {

            if(!record.data.sensorValue.equals(Double.NaN)) {
              // 缓存sensor数据
              if (OfflineElemData.contains(cacheKey)) {
                val runIdMap = OfflineElemData(cacheKey)

                // 存在runId
                if (runIdMap.contains(runId)) {
                  var sensorSet = runIdMap(runId)
                  sensorSet += record
                  runIdMap.put(runId, sensorSet)
                } else {
                  runIdMap.put(runId, Set(record))
                }
                OfflineElemData.put(cacheKey, runIdMap)
              } else {
                // 不存在cacheKey
                val runIdMap = concurrent.TrieMap[String,
                  scala.collection.immutable.Set[OfflineVirtualSensorOpentsdbResult]](runId -> Set(record))
                OfflineElemData.put(cacheKey, runIdMap)
              }
            }

            // 一个run的最后一个点
            if (lastSensorStatus) {

              // 缓存runId数据
              if (OfflineCacheDataAll.contains(cacheKey)) {
                var runIdSet = OfflineCacheDataAll(cacheKey)
                runIdSet += runId
                OfflineCacheDataAll.put(cacheKey, runIdSet)
              } else {
                OfflineCacheDataAll += (cacheKey -> Set(runId))
              }

              // 发送整个run的所有sensor
              val runIdMap = OfflineElemData(cacheKey)
              val sensorSet = runIdMap(runId)

              val metric = s"${record.toolName}.${record.chamberName}.${record.data.svid}"

              val groupByStepIdMap = new concurrent.TrieMap[String, ListBuffer[OfflineVirtualSensorOpentsdbResult]]
              sensorSet.foreach(x => {
                if (groupByStepIdMap.contains(x.data.stepId)) {
                  val sensorList = groupByStepIdMap(x.data.stepId)
                  sensorList += x
                  groupByStepIdMap.put(x.data.stepId, sensorList)
                } else {
                  groupByStepIdMap.put(x.data.stepId, ListBuffer(x))
                }
              })

              val value = ListBuffer[SensorElem]()

              // 按stepId分组
              for (elem <- groupByStepIdMap) {
                val stepId = elem._1
                val sensorList = elem._2

                val tags = Map[String, String]("stepId" -> stepId)
                var dps = Map[String, Double]()
                sensorList.foreach(x => {
                  dps += (x.timestamp.toString -> x.data.sensorValue)
                })

                val sensorElem = SensorElem(
                  metric = metric,
                  runId = runId,
                  aggregateTags = List(),
                  dps = dps,
                  tags = tags
                )
                value.append(sensorElem)
              }

              val virtualSensorElem = OfflineVirtualSensorElemResult(
                taskId = record.taskId,
                dataType = "OfflineVirtualSensorCalculate",
                status = "DEALING",
                errorMsg = "",
                value = value
              )

              ctx.output(virtualSensorElemOutput, virtualSensorElem)

              if (OfflineConfigAll.contains(cacheKey)) {
                val runIdConfigSet = OfflineConfigAll(cacheKey)
                val runIdDataSet = OfflineCacheDataAll(cacheKey)
                if (runIdConfigSet.subsetOf(runIdDataSet)) {

                  Thread.sleep(3000)
                  out.collect(OfflineSensorResultScala(myDatas.taskId, myDatas.batchId, "DONE", "", "OfflineVirtualSensorCalculate"))

                  OfflineConfigAll.remove(cacheKey)
                  OfflineCacheDataAll.remove(cacheKey)
                  OfflineElemData.remove(cacheKey)
                  close()
                }
              }
            }
          }
        } catch {
          case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
            Map("FdcOfflineVIrtualResultProcessFunction" -> "失败"), exception.toString).toString)

        }finally {
          // write the state back
          taskIdState.update(virtualSensorTimeOut(cacheKey, batchId, System.currentTimeMillis()))

          // schedule the next timer 60 seconds from the current event time
          ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong)

        }
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("FdcOfflineVIrtualResultProcessFunction" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   *  增加离线的配置
   */
  def addOfflineConfig(value: OfflineVirtualSensorTask, virtualConfig: VirtualConfig): Unit = {
    try {
      // 对runData排序
      val cacheKey = s"${value.taskId}|${virtualConfig.virtualSensorAliasName}"
      var runIdSet = value.runData.map(_.runId).toSet
      // 缓存数据
      if(OfflineConfigAll.contains(cacheKey)){
        var runIdSetTmp = OfflineConfigAll(cacheKey)
        runIdSet ++= runIdSetTmp
        OfflineConfigAll.put(cacheKey, runIdSet)
      }else{
        OfflineConfigAll += (cacheKey -> runIdSet)
      }

    }catch {
      case exception: Exception =>logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("addOfflineConfig" -> "增加离线virtual sensor配置"), value.toString).toString)
    }
  }
}

