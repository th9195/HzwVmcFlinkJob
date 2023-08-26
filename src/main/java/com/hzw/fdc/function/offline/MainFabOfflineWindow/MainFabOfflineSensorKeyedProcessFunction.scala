package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.scalabean.{FdcData, OfflineMainFabRawData, OfflineOpentsdbResult, OfflineSensorData, OfflineTask, OfflineTaskRunData, OfflineWindowListData, taskIdTimestamp}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

/**
 *  按照taskId 和runId 聚合sensor列表, 返回消息用于切窗口
 */
class MainFabOfflineSensorKeyedProcessFunction extends KeyedProcessFunction[String, (OfflineTask, OfflineOpentsdbResult),
  (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData)] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineSensorKeyedProcessFunction])

  private var taskIdRunIdMap: MapState[String, scala.collection.mutable.Set[OfflineOpentsdbResult]] = _

  /** The state that is maintained by this process function */
  private var taskIdState: ValueState[taskIdTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    val taskIdRunIdMapStateDescription: MapStateDescriptor[String, scala.collection.mutable.Set[OfflineOpentsdbResult]] = new
        MapStateDescriptor[String, scala.collection.mutable.Set[OfflineOpentsdbResult]]("taskIdRunIdMapState", TypeInformation.of(classOf[String]),
          TypeInformation.of(classOf[scala.collection.mutable.Set[OfflineOpentsdbResult]]))
    // 设置过期时间
    taskIdRunIdMapStateDescription.enableTimeToLive(ttlConfig)
    taskIdRunIdMap = getRuntimeContext.getMapState(taskIdRunIdMapStateDescription)

    val taskIdStateDescription = new ValueStateDescriptor[taskIdTimestamp]("offlineTaskIdKeyTimeOutValueState",
      TypeInformation.of(classOf[taskIdTimestamp]))
    taskIdState = getRuntimeContext.getState(taskIdStateDescription)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (OfflineTask, OfflineOpentsdbResult),
    (OfflineTask, List[OfflineMainFabRawData], OfflineTaskRunData)]#OnTimerContext, out: Collector[(OfflineTask,
    List[OfflineMainFabRawData], OfflineTaskRunData)]): Unit = {
    try{
      super.onTimer(timestamp, ctx, out)
      taskIdState.value match {
        case taskIdTimestamp(taskIdKey, lastModified)
          if (System.currentTimeMillis() >= lastModified + 300000L) =>
           taskIdRunIdMap.clear()
           close()
        case _ =>
      }
    }catch {
      case ex: Exception => logger.warn(s"onTimer error: ${ex.toString}")
    }
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(value: (OfflineTask, OfflineOpentsdbResult), ctx: KeyedProcessFunction[String,
    (OfflineTask, OfflineOpentsdbResult), (OfflineTask, List[OfflineMainFabRawData], OfflineTaskRunData)]#Context,
                              out: Collector[(OfflineTask, List[OfflineMainFabRawData], OfflineTaskRunData)]): Unit = {
    try{
      val offlineTask = value._1
      val oneRun = value._2
      val key = s"${value._1.taskId}|${value._2.runId}"
      val taskSvidNum = value._2.taskSvidNum

      if(taskSvidNum > 1){
        // 需要对多个sensor聚合
        if(taskIdRunIdMap.contains(key)){
          val offlineOpentsdbResultList = taskIdRunIdMap.get(key)
          if(offlineOpentsdbResultList.size == taskSvidNum - 1){
            // 聚合sensor 输出
            offlineOpentsdbResultList.add(value._2)

            val timeKeyData = new mutable.HashMap[String, ListBuffer[OfflineSensorData]]()

            for (oneRun <- offlineOpentsdbResultList) {
              for (elem <- oneRun.values) {
                val oneRunTimeKey = s"${oneRun.toolName}|${oneRun.chamberName}|${elem.time}|${elem.stepId}"
                if (timeKeyData.contains(oneRunTimeKey)) {
                  val offlineSensorDataList = timeKeyData(oneRunTimeKey)
                  offlineSensorDataList.append(OfflineSensorData(oneRun.sensorAlias, oneRun.sensorAlias, elem.value, "unit"))
                  timeKeyData.put(oneRunTimeKey, offlineSensorDataList)
                }else{
                  timeKeyData.put(oneRunTimeKey, ListBuffer(OfflineSensorData(oneRun.sensorAlias,
                    oneRun.sensorAlias, elem.value, "unit")))
                }
              }
            }

            val offlineMainFabRawDataList: ListBuffer[OfflineMainFabRawData] = new ListBuffer()
            for (elem <- timeKeyData) {
              val timeKeyList = elem._1.split("\\|")
              val time = timeKeyList(2)
              val stepId = timeKeyList.last
              offlineMainFabRawDataList.append(OfflineMainFabRawData(
                dataType = MainFabConstants.rawData,
                toolName = oneRun.toolName,
                chamberName = oneRun.chamberName,
                timestamp = time.toLong,
                runId = oneRun.runId,
                runStart = oneRun.runStart,
                runEnd = oneRun.runEnd,
                dataMissingRatio = oneRun.dataMissingRatio,
                stepId = stepId.toLong,
                stepName = "N/A",
                data = elem._2.toList
              ))
            }

            val taskRunData= OfflineTaskRunData(toolName = oneRun.toolName,
              chamberName = oneRun.chamberName,
              dataMissingRatio = oneRun.dataMissingRatio,
              runId = oneRun.runId,
              runStart = oneRun.runStart,
              runEnd = oneRun.runEnd,
              isCalculationSuccessful = true,
              calculationInfo = "")

            out.collect((offlineTask, offlineMainFabRawDataList.sortBy(_.timestamp).toList, taskRunData))

            taskIdRunIdMap.remove(key)
          }else{
            offlineOpentsdbResultList.add(value._2)
            taskIdRunIdMap.put(key, offlineOpentsdbResultList)
          }
        }else{
          taskIdRunIdMap.put(key, mutable.Set(value._2))
        }
      }else{
        // 只有单个sensor, 直接输出不聚合
        val offlineMainFabRawDataList: ListBuffer[OfflineMainFabRawData] = new ListBuffer()
        for (elem <- oneRun.values) {
          offlineMainFabRawDataList.append(OfflineMainFabRawData(
            dataType = MainFabConstants.rawData,
            toolName = oneRun.toolName,
            chamberName = oneRun.chamberName,
            timestamp = elem.time,
            runId = oneRun.runId,
            runStart = oneRun.runStart,
            runEnd = oneRun.runEnd,
            dataMissingRatio = oneRun.dataMissingRatio,
            stepId = elem.stepId,
            stepName = "N/A",
            data = List[OfflineSensorData](
              OfflineSensorData(oneRun.sensorAlias, oneRun.sensorAlias, elem.value, "unit")
            )
          ))
        }

        val taskRunData= OfflineTaskRunData(toolName = oneRun.toolName,
          chamberName = oneRun.chamberName,
          dataMissingRatio = oneRun.dataMissingRatio,
          runId = oneRun.runId,
          runStart = oneRun.runStart,
          runEnd = oneRun.runEnd,
          isCalculationSuccessful = true,
          calculationInfo = "")

        out.collect((offlineTask, offlineMainFabRawDataList.sortBy(_.timestamp).toList, taskRunData))
      }
    }catch {
      case e: Exception => logger.warn(s"Offline processElement OfflineTask: ${value._1} " +
        s"Exception :${ExceptionInfo.getExceptionInfo(e)}"  )
    }finally {
      // write the state back
      taskIdState.update(taskIdTimestamp(value._1.taskId.toString, System.currentTimeMillis()))
      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 300000L)
    }
  }
}
