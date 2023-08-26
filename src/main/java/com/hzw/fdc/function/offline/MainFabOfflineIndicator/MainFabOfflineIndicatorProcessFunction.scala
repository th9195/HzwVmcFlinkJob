package com.hzw.fdc.function.offline.MainFabOfflineIndicator

import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.ALGO.ALGO
import com.hzw.fdc.scalabean._
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util.NoSuchElementException

import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}


/**
 * @author gdj
 * @create 2020-07-01-13:53
 *
 */
@SerialVersionUID(1L)
class MainFabOfflineIndicatorProcessFunction extends KeyedProcessFunction[String,
  FdcData[OfflineWindowListData], (ALGO, ListBuffer[OfflineRawData], OfflineIndicatorConfig, List[OfflineIndicatorConfig])] {

  lazy private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineIndicatorProcessFunction])


  //缓存配置 key: {"taskId + indicatorId": "OfflineIndicatorConfig"}}}
  private val indicatorConfigByAll = new concurrent.TrieMap[String, OfflineIndicatorConfig]()

  /** The state that is maintained by this process function */
  private var indicatorTaskIdState: ValueState[taskIdTimestamp] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val taskIdStateDescription = new
        ValueStateDescriptor[taskIdTimestamp]("indicatorTaskIdTimeOutValueState", TypeInformation.of(classOf[taskIdTimestamp]))
    indicatorTaskIdState = getRuntimeContext.getState(taskIdStateDescription)
  }

  /**
   *  数据流超时机制
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, FdcData[OfflineWindowListData], (ALGO,
    ListBuffer[OfflineRawData], OfflineIndicatorConfig, List[OfflineIndicatorConfig])]#OnTimerContext,
                       out: Collector[(ALGO, ListBuffer[OfflineRawData], OfflineIndicatorConfig, List[OfflineIndicatorConfig])]): Unit = {
    super.onTimer(timestamp, ctx, out)

    indicatorTaskIdState.value match {
      case taskIdTimestamp(cacheKey, lastModified)
        if (timestamp >= lastModified + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong) =>
        logger.warn(s"onTimer_tep1: $cacheKey")
        if(indicatorConfigByAll.contains(cacheKey)) {
          indicatorConfigByAll.remove(cacheKey)
          // 关闭
          close()
        }
      case _ =>
    }
  }

  /**
   *  数据流
   */
  override def processElement(record: FdcData[OfflineWindowListData], ctx: KeyedProcessFunction[String, FdcData[OfflineWindowListData],
     (ALGO, ListBuffer[OfflineRawData], OfflineIndicatorConfig, List[OfflineIndicatorConfig])]#Context,
             out: Collector[(ALGO, ListBuffer[OfflineRawData], OfflineIndicatorConfig, List[OfflineIndicatorConfig])]): Unit = {

    record.`dataType` match {

      case "offlineFdcWindowDatas" =>
        try {
          val myDatas = record.datas
          val windowDataList = myDatas.windowDatasList

          val taskId = myDatas.taskId

          // 保留配置信息
          for (offlineIndicatorConfig <- myDatas.offlineIndicatorConfigList) {
            val offlineKey = taskId + "|" + offlineIndicatorConfig.indicatorId
            if(offlineIndicatorConfig.algoType == "1" && !indicatorConfigByAll.contains(offlineKey) ||
              offlineIndicatorConfig.algoClass == "cycleCount"){
              try {
                // write the state back
                indicatorTaskIdState.update(taskIdTimestamp(offlineKey, ctx.timestamp))

                // schedule the next timer 60 seconds from the current event time
                ctx.timerService.registerProcessingTimeTimer(ctx.timestamp + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong)
              }catch {
                case ex: Exception => None
              }

              // 不包含
              if(!indicatorConfigByAll.contains(offlineKey)){
                indicatorConfigByAll.put(offlineKey, offlineIndicatorConfig)
              }
            }
          }

          val cycleMap = new mutable.HashMap[String, mutable.HashMap[Int, OfflineRawData]]()

          for (windowDataScala <- windowDataList) {
            val listdata = windowDataScala.sensorDataList
            val indicatorIds = windowDataScala.indicatorId

            val cycleIndex = windowDataScala.cycleIndex

            val rawData = parseRawData(myDatas, listdata, windowDataScala)


            // 保存cycle window 的数据
            if(cycleIndex != -1){
              for (indicator <- indicatorIds) {
                val key = indicator.toString + "|" + windowDataScala.sensorAlias
                if(cycleMap.contains(key)){
                  val value = cycleMap(key)
                  value += (cycleIndex -> rawData)
                  cycleMap.put(key, value)
                }else{
                  val rawMap = mutable.HashMap[Int, OfflineRawData](cycleIndex -> rawData)
                  cycleMap += (key -> rawMap)
                }
              }
            }else {
              if (indicatorIds.nonEmpty) {
                for (elem <- indicatorIds) {
                  val offlineKey = taskId + "|" + elem.toString
                  if (indicatorConfigByAll.contains(offlineKey)) {
                    val indicatorConfigScala = indicatorConfigByAll(offlineKey)

                    val collectElem = (parseAlgo(indicatorConfigScala), ListBuffer(rawData), indicatorConfigScala, myDatas.offlineIndicatorConfigList)
                    out.collect(collectElem)
                  } else {
                    logger.warn(ErrorCode("009008d001C", System.currentTimeMillis(),
                      Map("record" -> record), s"base indicator配置中不存在: $offlineKey").toJson)
                  }
                }
              } else {
                logger.warn(ErrorCode("009008d001C", System.currentTimeMillis(),
                  Map("record" -> record), s"indicatorId 是空的").toJson)
              }
            }
          }

          for((k, v) <- cycleMap){
            val indicatorId = k.split("\\|").head
            val offlineKey = taskId + "|" + indicatorId
            if (indicatorConfigByAll.contains(offlineKey)) {
              val indicatorConfigScala = indicatorConfigByAll(offlineKey)

              val x: ListBuffer[OfflineRawData] = scala.collection.immutable.ListMap(v.toSeq.sortBy(_._1):_*).values.to[ListBuffer]
              val collectElem: (ALGO, ListBuffer[OfflineRawData], OfflineIndicatorConfig, List[OfflineIndicatorConfig])
              = (parseAlgo(indicatorConfigScala), x, indicatorConfigScala, myDatas.offlineIndicatorConfigList)

              out.collect(collectElem)
            } else {
              logger.warn(ErrorCode("009008d001C", System.currentTimeMillis(),
                Map("record" -> record), s"cycleMap base indicator配置中不存在: $offlineKey").toJson)
            }
          }
        } catch {
            case ex: Exception => logger.warn(ErrorCode("009008d001C", System.currentTimeMillis(),
              Map("record" -> record), s"indicator job Mach error $ex").toJson)
        }
      case _ => logger.warn(s"indicator job Kafka no mach type")
    }


  }

  override def close(): Unit = {
    super.close()
  }


  /**
   *  解析rawdata
   */
  def parseRawData(myDatas: OfflineWindowListData, listdata: List[sensorDataList], windowDataScala: OfflineWindowData): OfflineRawData = {

    val rawData = OfflineRawData(
      batchId = myDatas.batchId,
      taskId = myDatas.taskId,
      toolName = myDatas.toolName,
      chamberName = myDatas.chamberName,
      cycleUnitCount = windowDataScala.cycleUnitCount,
      unit = windowDataScala.unit,
      data = listdata,
      runId = myDatas.runId,
      runStartTime = myDatas.runStartTime,
      runEndTime = myDatas.runEndTime,
      windowStartTime = windowDataScala.windowStartTime,
      windowEndTime = windowDataScala.windowStopTime,
      windowEndDataCreateTime = myDatas.windowEndDataCreateTime,
      isCalculationSuccessful = windowDataScala.isCalculationSuccessful,
      calculationInfo = windowDataScala.calculationInfo)
    rawData
  }

  def parseAlgo(indicatorConfig: OfflineIndicatorConfig): ALGO ={
    var algo: ALGO = null
    try {
      algo = ALGO.withName(indicatorConfig.algoClass)
    } catch {
      case ex: NoSuchElementException =>
        algo = ALGO.UNKNOWN
        logger.warn(s" ALGO error indicatorConfig.ALGO_CLASS")
    }
    algo
  }
}

