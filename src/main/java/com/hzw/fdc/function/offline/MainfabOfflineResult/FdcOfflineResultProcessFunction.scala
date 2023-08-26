package com.hzw.fdc.function.offline.MainfabOfflineResult

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorElem, OfflineIndicatorResult, OfflineIndicatorTask, OfflineIndicatorValue, OfflineResultScala, OfflineRunIdConfig, OfflineRunStatus, taskIdTimestamp}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala._

import scala.collection.{concurrent, mutable}

/**
 *  判断离线计算indicator task的run是否都已经计算完成，同时返回结果信息
 */
class FdcOfflineResultProcessFunction extends KeyedProcessFunction[String, JsonNode, OfflineResultScala] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineResultProcessFunction])

  lazy val OfflineIndicatorElemOutput = new OutputTag[OfflineIndicatorElem]("OfflineIndicatorElem")

  // {"taskId + | + indicatorid": [runId, runId]}}
  val OfflineConfigAll = new concurrent.TrieMap[String, scala.collection.immutable.Set[OfflineRunIdConfig]]()

  // 缓存离线计算的数据 {"taskId + | +indicatorid": [runId, runId]}}
  val offlineDataByAll =  new concurrent.TrieMap[String, scala.collection.immutable.Set[OfflineRunStatus]]()

  // 批量编号对应有哪些task
  val batchConfigMap = new concurrent.TrieMap[String, Set[String]]()

  /** The state that is maintained by this process function */
  private var taskIdState: ValueState[taskIdTimestamp] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val taskIdStateDescription = new
        ValueStateDescriptor[taskIdTimestamp]("taskIdKeyTimeOutValueState", TypeInformation.of(classOf[taskIdTimestamp]))
    taskIdState = getRuntimeContext.getState(taskIdStateDescription)
  }

  /**
   * 数据流超时
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JsonNode, OfflineResultScala]#OnTimerContext,
                       out: Collector[OfflineResultScala]): Unit = {
    super.onTimer(timestamp, ctx, out)
     try {
       taskIdState.value match {
         case taskIdTimestamp(batchIdKey, lastModified)
           if (System.currentTimeMillis() >= lastModified + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong) =>

           logger.warn(s"indicator onTimer_tep1: $batchIdKey")

//           val cacheList = cacheKey.split("\\|")
           if (batchConfigMap.contains(batchIdKey)) {
             val cacheKeyList = batchConfigMap(batchIdKey)
             for(cacheKey <- cacheKeyList) {
               val cacheList = cacheKey.split("\\|")
               val taskId = cacheList.head
               val indicatorId = cacheList(1)
               val indicatorName = cacheList.last

               OfflineConfigAll.remove(cacheKey)
               offlineDataByAll.remove(cacheKey)
               val res = OfflineResultScala(taskId.toLong,
                 batchIdKey.toLong,
                 dataType = "OfflineIndicatorCalculate",
                 status = "DONE",
                 errorMsg = s"time out",
                 indicatorId = indicatorId.toLong,
                 indicatorName = indicatorName
               )
               out.collect(res)

             }
             batchConfigMap.remove(batchIdKey)
             close()
           }

         case _ =>
       }
     }catch {
       case ex: Exception => logger.warn(s"onTimer error: $ex")
     }
  }

  override def close(): Unit = {
    super.close()
  }


  /**
   *  数据流处理
   */
  override def processElement(value: JsonNode, ctx: KeyedProcessFunction[String, JsonNode, OfflineResultScala]#Context,
                              out: Collector[OfflineResultScala]): Unit = {

    val dataType = value.findPath("dataType").asText()
    var cacheKey = ""
    try{
      cacheKey = value.findPath("batchId").asText()
    }catch {
      case ex: Exception => logger.warn(s"batchId error: $ex")
    }

    try {
      dataType match {
        case "OfflineIndicatorCalculate" =>
           addOfflineConfig(toBean[OfflineIndicatorTask](value))
        case _ => None
      }
    } catch {
      case ex: Exception => logger.warn(s"result error: $ex")
    }

    try {
      dataType match {
        case "offlineIndicator" =>

          val record = toBean[FdcData[OfflineIndicatorResult]](value)

          val indicatorResult = record.datas
          val indicatorId = indicatorResult.indicatorId.toString
          val indicatorName = indicatorResult.indicatorName
          val taskId = indicatorResult.taskId.toString

          val offlineKey = taskId  + "|" + indicatorId

          val runId = indicatorResult.runId

          if (!this.OfflineConfigAll.contains(offlineKey)) {
            return
          }
          //          logger.warn(s"FdcOfflineResultProcessFunction_step2: " + offlineKey + "\trunId: " + runId)

          // 缓存数据
          if(offlineDataByAll.contains(offlineKey)){
            var runIdSet = offlineDataByAll(offlineKey)
            runIdSet += OfflineRunStatus(indicatorResult.isCalculationSuccessful, runId)
            offlineDataByAll.put(offlineKey,runIdSet)
          }else{
            offlineDataByAll += (offlineKey -> Set(OfflineRunStatus(indicatorResult.isCalculationSuccessful, runId)))
          }

          if(indicatorResult.isCalculationSuccessful) {
            val indicatorElem = OfflineIndicatorElem(
              taskId = indicatorResult.taskId,
              batchId = indicatorResult.batchId.get,
              status = "DEALING",
              errorMsg = "",
              dataType = "OfflineIndicatorCalculate",
              value = OfflineIndicatorValue(
                RUN_ID = indicatorResult.runId,
                TOOL_ID = indicatorResult.toolName,
                CHAMBER_ID = indicatorResult.chamberName,
                INDICATOR_ID = indicatorResult.indicatorId,
                INDICATOR_NAME = indicatorResult.indicatorName,
                INDICATOR_VALUE = indicatorResult.indicatorValue,
                RUN_START_TIME = indicatorResult.runStartTime.toString,
                RUN_END_TIME = indicatorResult.runEndTime.toString,
                WINDOW_START_TIME = indicatorResult.windowStartTime.toString,
                WINDOW_END_TIME = indicatorResult.windowEndTime.toString
              )
            )

            ctx.output(OfflineIndicatorElemOutput, indicatorElem)
          }


          val res = isFinishMath(offlineKey, indicatorId, indicatorName, indicatorResult.batchId.get.toString)
          if(res.status != ""){
            Thread.sleep(200)
            out.collect(res)
            close()
          }
        case _ => None
      }
    } catch {
      case ex: Exception => logger.warn(s"drift NumberFormatException $value error: $ex")
    }finally {
      // write the state back
      taskIdState.update(taskIdTimestamp(cacheKey, System.currentTimeMillis()))

      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + ProjectConfig.OFFLINE_TASK_TIME_OUT.toLong)
    }
  }

  /**
   *  判断是否已经计算完成
   */
  def isFinishMath(offlineKey:String, indicatorId: String, indicatorName: String, batchId: String): OfflineResultScala= {
    var res: OfflineResultScala = OfflineResultScala(0L, 0L, "", "", "", 0, "")
    try {
      val runIdDataSet = offlineDataByAll(offlineKey)
      val runIdConfigSet = OfflineConfigAll(offlineKey)

      val runIdSize = runIdDataSet.size

      if (runIdSize == runIdConfigSet.size) {

        val runIdSuccessSize = offlineDataByAll(offlineKey).count(_.mathStatus == true)

        val errorMsg = s"$offlineKey total count: $runIdSize\t success size:$runIdSuccessSize "
        val taskId = offlineKey.split("\\|").head.toLong
        val status = "DONE"


        res = OfflineResultScala(taskId,
          batchId.toLong,
          dataType = "OfflineIndicatorCalculate",
          status = status,
          errorMsg = errorMsg,
          indicatorId = indicatorId.toLong,
          indicatorName = indicatorName
        )

        logger.warn(s"isFinishMath_resSet: " + res + "\tofflineKey: " + offlineKey)

        // 清除task缓存
        offlineDataByAll.remove(offlineKey)
        OfflineConfigAll.remove(offlineKey)

        if(batchConfigMap.contains(batchId)){
          var batchISet = batchConfigMap(batchId)
          batchISet = batchISet.-(s"${taskId}|${indicatorId}|${indicatorName}")
          if(batchISet.isEmpty){
            batchConfigMap.remove(batchId)
          }else{
            batchConfigMap.put(batchId, batchISet)
          }
        }
      }else{
        if(runIdConfigSet.size == runIdDataSet.size){
          logger.warn(s"runIdConfigSet: " + runIdConfigSet  + "\trunIdDataSet: "
            + runIdDataSet + "\tofflineKey: " + offlineKey + "\tsize: " + runIdConfigSet.size)
        }
      }
    }catch {
      case ex: Exception => logger.warn(s"isFinishMath $offlineKey\t error: $ex")
    }
    res
  }


  /**
   *  增加离线的配置
   */
  def addOfflineConfig(value: OfflineIndicatorTask): String = {
    var cacheKey = ""
    try {

      // 对runData排序
      var runData = value.runData.sortWith(_.runStart < _.runStart)


      val offlineIndicatorConfig = value.indicatorTree.current.indicatorConfig
      val indicatorId = offlineIndicatorConfig.indicatorId.toString
      val indicatorName = offlineIndicatorConfig.indicatorName
      val taskId = value.taskId.toString

      val offlineKey =  taskId + "|" + indicatorId


      // 批量编号对应的task配置
      val batchId = value.batchId.get.toString
      if(batchConfigMap.contains(batchId)){
        var taskSet = batchConfigMap(batchId)
        taskSet += s"${taskId}|${indicatorId}|${indicatorName}"
        batchConfigMap.put(batchId, taskSet)
      }else{
        batchConfigMap.put(batchId, Set(s"${taskId}|${indicatorId}|${indicatorName}"))
      }

      cacheKey = batchId


      try {
        // custom indicator 前几个run不参与计算
        if (offlineIndicatorConfig.algoClass == "indicatorAvg" || offlineIndicatorConfig.algoClass == "MA" ||
          offlineIndicatorConfig.algoClass == "drift" || offlineIndicatorConfig.algoClass == "driftPercent" ||
          offlineIndicatorConfig.algoClass == "linearFit") {
          val algoParamList = offlineIndicatorConfig.algoParam.split("\\|")
          val driftX = algoParamList(1).toInt

          val toolNameNum = runData.map(elem => {
            val runIdSpit = elem.runId.split("--", -1)
            val toolName = runIdSpit(0)
            val chamberName = runIdSpit(1)
            toolName + "|" + chamberName}).distinct.size

          val count = driftX * toolNameNum

          runData = runData.drop(count)
        }
      }catch {
        case ex: Exception  => logger.warn(s"addOfflineConfig $offlineKey\t error: $ex")
      }


      if(runData.nonEmpty) {
        val runIdSet: scala.collection.immutable.Set[OfflineRunIdConfig] = runData.map(
          x => {
            OfflineRunIdConfig(runId = x.runId,
              indicatorName = indicatorName,
              runStartTime = x.runStart,
              runEndTime = x.runEnd
            )}
        ).toSet


        if (this.OfflineConfigAll.contains(offlineKey)) {
          logger.warn(s"addOfflineConfig ERROR: " + value + "\t now OfflineConfigAll: " + OfflineConfigAll)
        } else {
          this.OfflineConfigAll.put(offlineKey, runIdSet)
          logger.warn(s"addOfflineConfig offlineKey: " + offlineKey + "\trunId SIZE:" + runIdSet.size)
        }
      }
    }catch {
      case ex: Exception => logger.warn(s"addOfflineConfig: $value\t error: $ex")
    }
    cacheKey
  }


}