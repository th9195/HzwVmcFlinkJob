package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, fromMap, toBean}
import com.hzw.fdc.scalabean.{ErrorCode, FdcData, MESMessage, MainFabLogInfo, MainFabRawData, RunData, RunEventData, lotMessage, sensorMessage, taskIdTimestamp, toolMessage}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions.iterableAsScalaIterable
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.OutputTag
import org.slf4j.{Logger, LoggerFactory}

/**
 *  处理rundata和mes信息
 */
class MainFabProcessRunFunction extends KeyedProcessFunction[String, JsonNode, FdcData[RunData]] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabProcessRunFunction])

  private var eventStartState:ValueState[RunEventData] = _
  private var stepIdListState: ListState[String] = _

  private var sensorMessageStateMap: MapState[String,sensorMessage] = _

  lazy val MESOutput = new OutputTag[JsonNode]("toolMessage")
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  val job_fun_DebugCode:String = "022003"
  val jobName:String = "MainFabDataTransformWindowService"
  val optionName : String = "MainFabProcessRunFunction"

  // 建一个累加器
  private var numOfLinesState: ValueState[Long] = _

  //记录run最后时间
  private var taskIdState: ValueState[taskIdTimestamp] = _

  //毫秒的超时时间
  private val maxTimeMS: Long = ProjectConfig.RUN_MAX_LENGTH.toLong * 60 * 60 * 1000


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

    val eventStartStateDescription = new
        ValueStateDescriptor[RunEventData]("runEventStartState", TypeInformation.of(classOf[RunEventData]))
    // 设置过期时间
    eventStartStateDescription.enableTimeToLive(ttlConfig)
    eventStartState = getRuntimeContext.getState(eventStartStateDescription)

    val stepIdListStateDescription = new ListStateDescriptor[String]("stepIdListState", TypeInformation.of(classOf[String]))
    // 设置过期时间
    stepIdListStateDescription.enableTimeToLive(ttlConfig)
    stepIdListState = getRuntimeContext.getListState(stepIdListStateDescription)

    val sensorMessageStateDescription: MapStateDescriptor[String,sensorMessage] = new
        MapStateDescriptor[String,sensorMessage]("sensorMessageState", TypeInformation.of(classOf[String]),
          TypeInformation.of(classOf[sensorMessage]))
    // 设置过期时间
    sensorMessageStateDescription.enableTimeToLive(ttlConfig)
    sensorMessageStateMap = getRuntimeContext.getMapState(sensorMessageStateDescription)

    taskIdState = getRuntimeContext
      .getState(new ValueStateDescriptor[taskIdTimestamp]("maxTimeState", classOf[taskIdTimestamp]))

    val numOfLinesStateDescription = new ValueStateDescriptor[Long]("run-num-of-lines", TypeInformation.of(classOf[Long]))
    // 设置过期时间
    numOfLinesStateDescription.enableTimeToLive(ttlConfig)
    numOfLinesState = getRuntimeContext.getState(numOfLinesStateDescription)
  }

  override def processElement(ptData: JsonNode,
                              readOnlyContext: KeyedProcessFunction[String, JsonNode, FdcData[RunData]]#Context,
                              collector: Collector[FdcData[RunData]]): Unit = {
    try {
      val dataType = ptData.get(MainFabConstants.dataType).asText()

      if(dataType == MainFabConstants.rawData){

        val RawData = toBean[MainFabRawData](ptData)

        //组装MES数据
        for (elem <- RawData.data) {
          //过滤VirtualSensor
          if(!elem.isVirtualSensor){
            if(!sensorMessageStateMap.contains(elem.svid)){
              sensorMessageStateMap.put(elem.svid, sensorMessage(svid = elem.svid,
                sensorAlias = elem.sensorAlias,
                  sensorName = elem.sensorName,
                  unit = elem.unit))
            }
          }
        }

        // 处理rundata需要的stepId
        stepIdListState.add(RawData.stepId.toString)

        val numOfLines = numOfLinesState.value()
        if(numOfLines.isValidLong){
          numOfLinesState.update(numOfLines + 1L)
        }else{
          numOfLinesState.update(0L)
        }
      }else if(dataType == MainFabConstants.eventStart){
        var RunEventStart = toBean[RunEventData](ptData)

        /**
         *  rawdata中有wafers为null时没有正常生成run
         */
        try {
          var flag = false
          val lotMESInfoList = RunEventStart.lotMESInfo.map(x => {
            if (x.get.wafers == null) {
              flag = true
              Option(x.get.copy(wafers = List()))
            } else {
              x
            }
          })

          // 代表有wafers == null
          if (flag) {
            RunEventStart = RunEventStart.copy(lotMESInfo = lotMESInfoList)
          }
        }catch {
          case exception: Exception =>logger.error(ErrorCode("022003d001C", System.currentTimeMillis(),
            Map("ptData" -> ptData,"RunEventStart" -> "wafers"), ExceptionInfo.getExceptionInfo(exception)).toJson)
        }

        eventStartState.update(RunEventStart)
        taskIdState.update(taskIdTimestamp(RunEventStart.traceId,RunEventStart.runStartTime))

        // todo 第一次生成RunData 数据 add by toby, 避免windowEnd计算时还需要生成一次RunData
        val runData : FdcData[RunData] = generateRunDataByEventStart(RunEventStart)
        collector.collect(runData)

        readOnlyContext.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + maxTimeMS)

      } else if (dataType == MainFabConstants.eventEnd) {

        try {
          val RunEventStart = eventStartState.value()
          var RunEventEnd = toBean[RunEventData](ptData)

          /**
           *  F2P2-35 rawdata中有wafers为null时没有正常生成run
           */
          try {
            var flag = false
            val lotMESInfoList = RunEventEnd.lotMESInfo.map(x => {
              if (x.get.wafers == null) {
                flag = true
                Option(x.get.copy(wafers = List()))
              } else {
                x
              }
            })

            // 代表有wafers == null
            if (flag) {
              RunEventEnd = RunEventEnd.copy(lotMESInfo = lotMESInfoList)
            }
          }catch {
            case exception: Exception =>logger.error(ErrorCode("022003d001C", System.currentTimeMillis(),
              Map("ptData" -> ptData,"RunEventEnd" -> "wafers"), ExceptionInfo.getExceptionInfo(exception)).toJson)
          }


          //有没有event start
          val runStartTime = if (RunEventStart != null) {
            RunEventStart.runStartTime
          } else {
            RunEventEnd.runEndTime - RunEventEnd.timeRange
          }

//          val runStartTime = RunEventStart.runStartTime

          //判断是否超过3小时
          val gap = RunEventEnd.runEndTime - runStartTime

          if (gap >= ProjectConfig.RUN_MAX_LENGTH * 60 * 60 * 1000) {

            val traceId = ptData.get(MainFabConstants.traceId).asText()

            logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(),
              Map("runTimeTotal" -> gap.toString, "RunEventEnd" -> RunEventEnd, "traceId" -> traceId), "run超过20小时，清除数据").toJson)

            readOnlyContext.output(mainFabLogInfoOutput,
              generateMainFabLogInfo("0001C",
                "processElement",
                "长Run超时异常:",
                Map[String,Any]("runTimeTotal" -> gap.toString,
                  "RunEventEnd" -> RunEventEnd,
                  "traceId" -> traceId,
                  "run_lenth" -> (ProjectConfig.RUN_MAX_LENGTH * 60 * 60 * 1000).toString,
                  "dataInfo" -> ptData)))

          }else {

            val runId = s"${RunEventEnd.toolName}--${RunEventEnd.chamberName}--${runStartTime}"
            val runDataSize = if(numOfLinesState.value().isValidLong) numOfLinesState.value() else 0
            val stepListString = stepIdListState.get().toList.mkString(",")

            val runData = FdcData("rundata", RunData(runId = runId,
              toolName = RunEventEnd.toolName,
              chamberName = RunEventEnd.chamberName,
              recipe = RunEventEnd.recipeName,
              dataMissingRatio = Option(RunEventEnd.dataMissingRatio),
              runStartTime = runStartTime,
              timeRange = Option(RunEventEnd.timeRange),
              runEndTime = Option(RunEventEnd.runEndTime),
              createTime = System.currentTimeMillis(),
              step = Option(stepListString),
              runDataNum = Option(runDataSize),
              traceId = RunEventEnd.traceId,
              DCType = RunEventEnd.DCType,
              completed = RunEventEnd.completed,
              materialName = RunEventEnd.materialName,
              pmStatus = RunEventEnd.pmStatus,
              pmTimestamp = RunEventEnd.pmTimestamp,
              dataVersion = RunEventEnd.dataVersion,
              lotMESInfo = RunEventEnd.lotMESInfo,
              errorCode = if(RunEventEnd.errorCode.isDefined) RunEventEnd.errorCode else Option(0)
            ))

            collector.collect(runData)

            val MESMessageList = RunEventEnd.lotMESInfo.map(elem => MESMessage(route = elem.get.route,
              `type` = elem.get.lotType,
              operation = elem.get.operation,
              layer = elem.get.layer,
              technology = elem.get.technology,
              stage = elem.get.stage,
              product = elem.get.product,
              lotData = lotMessage(locationName = RunEventEnd.locationName, moduleName = RunEventEnd.moduleName, toolName = RunEventEnd.toolName,
                chamberName = RunEventEnd.chamberName,
                lotName = elem.get.lotName,
                carrier = elem.get.carrier)))
            for (elem <- MESMessageList) {

              readOnlyContext.output(MESOutput,
                beanToJsonNode[FdcData[MESMessage]](FdcData(dataType = MainFabConstants.MESMessage,
                  datas = elem)))
            }

            val toolMessageData=FdcData(dataType = MainFabConstants.toolMessage,
              datas = toolMessage(locationName = RunEventEnd.locationName,
                moduleName = RunEventEnd.moduleName,
                toolName = RunEventEnd.toolName,
                chamberName = RunEventEnd.chamberName,
                sensors = sensorMessageStateMap.values().toList,
                recipeNames = List(RunEventEnd.recipeName)))
            //组装MES数据

            readOnlyContext.output(MESOutput,beanToJsonNode[FdcData[toolMessage]](toolMessageData))
          }
//          }
        } catch {
          case exception: Exception =>logger.error(ErrorCode("022003d001C", System.currentTimeMillis(),
            Map("ptData" -> ptData,"RunEventEnd" -> "RunEventEnd"), ExceptionInfo.getExceptionInfo(exception)).toJson)
        }finally {
          // 清除状态
          eventStartState.clear()
          sensorMessageStateMap.clear()
          stepIdListState.clear()
          numOfLinesState.clear()
          taskIdState.clear()

          // 关闭
          close()
        }
      }
    }catch {
      case exception: Exception =>logger.error(ErrorCode("022003d002C", System.currentTimeMillis(),
        Map("MainFabRawDataListSize" -> "", "ptData" -> ptData), ExceptionInfo.getExceptionInfo(exception)).toJson)
    }
  }

  override def close(): Unit = {
    super.close()
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JsonNode, FdcData[RunData]]#OnTimerContext,
                       out: Collector[FdcData[RunData]]): Unit = {
    try {
      super.onTimer(timestamp, ctx, out)

      taskIdState.value match {
        case taskIdTimestamp(cacheKey, lastModified)
          if (timestamp >= lastModified + maxTimeMS) =>
//          logger.warn(s"runData超时处理: $cacheKey")
          ctx.output(mainFabLogInfoOutput,
            generateMainFabLogInfo("0002B",
              "processElement",
              "runData超时处理",
              Map[String,Any]("cacheKey" -> cacheKey, "lastModified"->lastModified,"maxTimeMS" -> maxTimeMS)))
          try {
            val RunEventStart = eventStartState.value()
            val runId = s"${RunEventStart.toolName}--${RunEventStart.chamberName}--${RunEventStart.runStartTime}"
            val runDataSize = if(numOfLinesState.value().isValidLong) numOfLinesState.value() else 0
            val stepListString = stepIdListState.get().toList.mkString(",")

            val runEndTime = System.currentTimeMillis()

            val runData = FdcData("rundata", RunData(runId = runId,
              toolName = RunEventStart.toolName,
              chamberName = RunEventStart.chamberName,
              recipe = RunEventStart.recipeName,
              dataMissingRatio = Option(RunEventStart.dataMissingRatio),
              runStartTime = RunEventStart.runStartTime,
              timeRange = Option(RunEventStart.timeRange),
              runEndTime = Option(runEndTime),
              createTime = System.currentTimeMillis(),
              step = Option(stepListString),
              runDataNum = Option(runDataSize),
              traceId = RunEventStart.traceId,
              DCType = RunEventStart.DCType,
              completed = RunEventStart.completed,
              materialName = RunEventStart.materialName,
              pmStatus = RunEventStart.pmStatus,
              pmTimestamp = RunEventStart.pmTimestamp,
              dataVersion = RunEventStart.dataVersion,
              lotMESInfo = RunEventStart.lotMESInfo,
              errorCode = Option(-201)
            ))

            out.collect(runData)
          } catch {
            case ex: Exception => logger.error(s"runData超时处理:${ExceptionInfo.getExceptionInfo(ex)} ")
          } finally {
            // 清除状态
            eventStartState.clear()
            sensorMessageStateMap.clear()
            stepIdListState.clear()
            numOfLinesState.clear()
            taskIdState.clear()

            // 关闭
            close()
          }

        case _ =>
      }
    }catch {
      case e: Exception => logger.error(s"windowJob processRunFunction onTimer " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }

  }

  /**
   * 使用eventStart 生成 RunData
   * @param runEventStart
   * @return
   */
  def generateRunDataByEventStart(runEventStart: RunEventData): FdcData[RunData] = {
    val runId = s"${runEventStart.toolName}--${runEventStart.chamberName}--${runEventStart.runStartTime}"

    FdcData("rundata",RunData(runId = runId,
      toolName = runEventStart.toolName,
      chamberName = runEventStart.chamberName,
      recipe = runEventStart.recipeName,
      dataMissingRatio = Option(-1),
      runStartTime = runEventStart.runStartTime,
      timeRange = Option(-1L),
      runEndTime = Option(-1L),
      createTime = System.currentTimeMillis(),
      step = Option(""),
      runDataNum = Option(-1L),
      traceId = runEventStart.traceId,
      DCType = runEventStart.dataType,
      completed = runEventStart.completed,
      materialName = runEventStart.materialName,
      pmStatus = runEventStart.pmStatus,
      pmTimestamp = runEventStart.pmTimestamp,
      dataVersion = runEventStart.dataVersion,
      lotMESInfo = runEventStart.lotMESInfo,
      errorCode = runEventStart.errorCode
    ))

  }


  /**
   * 生成日志信息
   * @param debugCode
   * @param message
   * @param paramsInfo
   * @param dataInfo
   * @param exception
   * @return
   */
  def generateMainFabLogInfo(debugCode:String,funName:String,message:String,paramsInfo:Map[String,Any]=null,dataInfo:String="",exception:String = ""): MainFabLogInfo = {
    MainFabLogInfo(mainFabDebugCode = job_fun_DebugCode + debugCode,
      jobName = jobName,
      optionName = optionName,
      functionName = funName,
      logTimestamp = DateTimeUtil.getCurrentTimestamp,
      logTime = DateTimeUtil.getCurrentTime(),
      message = message,
      paramsInfo = paramsInfo,
      dataInfo = dataInfo,
      exception = exception)
  }
}

