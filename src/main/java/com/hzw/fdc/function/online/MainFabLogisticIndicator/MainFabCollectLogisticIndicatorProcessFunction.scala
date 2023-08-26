package com.hzw.fdc.function.online.MainFabLogisticIndicator

import com.hzw.fdc.engine.calculatedindicator.{CalculatedIndicatorBuilder, CalculatedIndicatorBuilderFactory, ICalculatedIndicator}
import com.hzw.fdc.scalabean.{ErrorCode, FdcData, IndicatorConfig, IndicatorResult, taskIdTimestamp}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

/**
 * @author ：gdj
 * @date ：Created in 2021/8/30 15:58
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabCollectLogisticIndicatorProcessFunction extends KeyedProcessFunction[Long, (IndicatorConfig, IndicatorResult), (FdcData[IndicatorResult], Boolean, Boolean, Boolean)] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabCollectLogisticIndicatorProcessFunction])

  //LogisticIndicatorId-->runId-->参与计算的indicator id --》IndicatorResult
  val indicatorDataByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[Long, IndicatorResult]]()

  // {"controlPlanId": {"version" : {"LogisticIndicator结果 的indicatorId": {"runid"": {"参与计算的indicatorId"": "IndicatorResult"}}}}}
  val historyIndicatorResultByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]]]]()

  /** The state that is maintained by this process function */
  private var runIdState: ValueState[taskIdTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val runIdStateDescription = new ValueStateDescriptor[taskIdTimestamp]("logisticIndicatorOutValueState",
      TypeInformation.of(classOf[taskIdTimestamp]))
    runIdState = getRuntimeContext.getState(runIdStateDescription)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (IndicatorConfig, IndicatorResult),
    (FdcData[IndicatorResult], Boolean, Boolean, Boolean)]#OnTimerContext, out: Collector[(FdcData[IndicatorResult],
    Boolean, Boolean, Boolean)]): Unit = {
    try {
      runIdState.value() match {
        case taskIdTimestamp(cacheKey, lastModified)
          if (System.currentTimeMillis() >= lastModified + 20000L) =>
          logger.warn(s"====logisticIndicator=== ${cacheKey} math is time out 60s")
          // 清除状态
          runIdState.clear()
          close()
        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("logisticIndicator", System.currentTimeMillis(),
        Map("onTimer" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }

  override def processElement(input: (IndicatorConfig, IndicatorResult),
                              context: KeyedProcessFunction[Long, (IndicatorConfig, IndicatorResult), (FdcData[IndicatorResult], Boolean, Boolean, Boolean)]#Context,
                              collector: Collector[(FdcData[IndicatorResult], Boolean, Boolean, Boolean)]): Unit = {


    try {
      val indicatorResult = input._2

      val indicatorConfig = input._1
      val algoParamList: List[String] = indicatorConfig.algoParam.split("\\^_hzw_\\^").toList
      val indicatorResultList = LoadIndicatorResult(input, algoParamList)

      if (indicatorResultList.nonEmpty) {

        try {
          // write the state back
          runIdState.update(taskIdTimestamp(indicatorResultList.head.runId, System.currentTimeMillis()))

          // schedule the next timer 60 seconds from the current event time
          context.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 20000L)
        }catch {
          case ex: Exception => logger.warn("processEnd registerProcessingTimeTimer error: " + ex.toString)
        }


        val builder: CalculatedIndicatorBuilder = CalculatedIndicatorBuilderFactory.makeBuilder

        builder.setCalculatedIndicatorClassName(input._1.indicatorName)
        builder.setAllowLoop(true)
        builder.addStaticImport("java.lang.Math")
        builder.setImplementation(algoParamList.head)
        builder.setTimeLimit(500)
        builder.setUserTimeLimit(300)


        var calculatedIndicator: ICalculatedIndicator = null

        try {
          calculatedIndicator = builder.build
          for (elem <- indicatorResultList) {

            calculatedIndicator.setIndicatorValue(elem.indicatorId.toString, elem.indicatorName, elem.indicatorValue.toDouble)

          }

          val calculatedIndicatorValue: Double = calculatedIndicator.calculate

          if((!calculatedIndicatorValue.isNaN) &&
            (!calculatedIndicatorValue.isPosInfinity) &&
            (!calculatedIndicatorValue.isNegInfinity)) {
            val logisticIndicatorResult = IndicatorResult(
              controlPlanId = indicatorResult.controlPlanId,
              controlPlanName = indicatorResult.controlPlanName,
              controlPlanVersion = indicatorResult.controlPlanVersion,
              locationId = indicatorResult.locationId,
              locationName = indicatorResult.locationName,
              moduleId = indicatorResult.moduleId,
              moduleName = indicatorResult.moduleName,
              toolGroupId = indicatorResult.toolGroupId,
              toolGroupName = indicatorResult.toolGroupName,
              chamberGroupId = indicatorResult.chamberGroupId,
              chamberGroupName = indicatorResult.chamberGroupName,
              recipeGroupName = indicatorResult.recipeGroupName,
              runId = indicatorResult.runId,
              toolName = indicatorResult.toolName,
              toolId = indicatorResult.toolId,
              chamberName = indicatorResult.chamberName,
              chamberId = indicatorResult.chamberId,
              indicatorValue = calculatedIndicatorValue.toString,
              indicatorId = indicatorConfig.indicatorId,
              indicatorName = indicatorConfig.indicatorName,
              algoClass = indicatorConfig.algoClass,
              indicatorCreateTime = System.currentTimeMillis(),
              missingRatio = indicatorResult.missingRatio,
              configMissingRatio = indicatorConfig.missingRatio,
              runStartTime = indicatorResult.runStartTime,
              runEndTime = indicatorResult.runEndTime,
              windowStartTime = indicatorResult.windowStartTime,
              windowEndTime = indicatorResult.windowEndTime,
              windowDataCreateTime = indicatorResult.windowDataCreateTime,
              limitStatus = indicatorResult.limitStatus,
              materialName = indicatorResult.materialName,
              recipeName = indicatorResult.recipeName,
              recipeId = indicatorResult.recipeId,
              product = indicatorResult.product,
              stage = indicatorResult.stage,
              bypassCondition = indicatorConfig.bypassCondition,
              pmStatus = indicatorResult.pmStatus,
              pmTimestamp = indicatorResult.pmTimestamp,
              area = indicatorResult.area,
              section = indicatorResult.section,
              mesChamberName = indicatorResult.mesChamberName,
              lotMESInfo = indicatorResult.lotMESInfo,
              dataVersion = indicatorResult.dataVersion,
              cycleIndex = indicatorResult.cycleIndex,
              unit = "")

            collector.collect((FdcData[IndicatorResult]("indicator", logisticIndicatorResult),
              indicatorConfig.driftStatus,
              indicatorConfig.calculatedStatus,
              indicatorConfig.logisticStatus))
          }else{
            logger.warn(s"indicatorResult: ${indicatorResult} \t IndicatorValue == Double.NaN  Double.PositiveInfinity  Double.NegativeInfinity")
          }

        } catch {
          case e: Exception => logger.warn(s"calculated LogisticIndicator error Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }


      }

      if (runIdState.value() != null) {
        runIdState.clear()
      }
    } catch {
      case e: Exception => logger.warn(s"builder LogisticIndicator error Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }

  /**
   * 加载IndicatorResult
   *
   * @param input
   * @return 是否都所有计算原料都到齐了
   */
  def LoadIndicatorResult(input: (IndicatorConfig, IndicatorResult), algoParamList: List[String]): List[IndicatorResult] = {

    val controlPlanId = input._1.controlPlanId
    val controlPlanVersion = input._1.controlPlanVersion
    val logisticIndicatorId = input._1.indicatorId
    val runId = input._2.runId
    val subIndicatorId = input._2.indicatorId

    if (algoParamList.size >= 2) {
      val indicatorList = algoParamList.drop(1)
      if (1 == indicatorList.size) {

        List(input._2)
      } else {

        var historyRun:concurrent.TrieMap[Long, IndicatorResult] = null

      // {"controlPlanId": {"version" : {"LogisticIndicator结果 的indicatorId": {"runid"": {"参与计算的indicatorId"": "IndicatorResult"}}}}}
      // 新增逻辑
      if (this.historyIndicatorResultByAll.contains(controlPlanId)) {
        val versionMap = this.historyIndicatorResultByAll(controlPlanId)
        //版本
        if (versionMap.contains(controlPlanVersion)) {
          val logisticIndicatorMap = versionMap(controlPlanVersion)
          //LogisticIndicator
          if (logisticIndicatorMap.contains(logisticIndicatorId)) {
            val runMap = logisticIndicatorMap(logisticIndicatorId)
            //runmap
            if (runMap.contains(runId)) {
              val indicatorMap = runMap(runId)
              indicatorMap.put(subIndicatorId, input._2)
              historyRun =indicatorMap
              runMap.put(runId, indicatorMap)

            } else {
              val indicatorMap = new concurrent.TrieMap[Long, IndicatorResult]
              indicatorMap.put(subIndicatorId, input._2)
              historyRun =indicatorMap
              runMap.put(runId, indicatorMap)
            }
            logisticIndicatorMap.put(logisticIndicatorId, runMap)

          } else {
            val indicatorMap = new concurrent.TrieMap[Long, IndicatorResult]
            indicatorMap.put(subIndicatorId, input._2)
            val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]

            runMap.put(runId, indicatorMap)

            logisticIndicatorMap.put(logisticIndicatorId, runMap)

          }

          versionMap.put(controlPlanVersion, logisticIndicatorMap)

          //新版本
        } else {
          val indicatorMap = new concurrent.TrieMap[Long, IndicatorResult]
          indicatorMap.put(subIndicatorId, input._2)
          historyRun =indicatorMap
          val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]

          runMap.put(runId, indicatorMap)

          val logisticIndicatorMap = new concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]]

          logisticIndicatorMap.put(logisticIndicatorId, runMap)


          versionMap.put(controlPlanVersion, logisticIndicatorMap)


          // 更新版本
          val k = versionMap.keys
          if (k.size > 2) {
            val minVersion = k.toList.min
            versionMap.remove(minVersion)
            logger.warn(s"addIndicatorConfigToTCSP LogisticIndicatorJob: 删除旧版本data $minVersion controlPlanId: $controlPlanId controlPlanVersion $controlPlanVersion")
          }
        }


        historyIndicatorResultByAll.put(controlPlanId, versionMap)


      } else {
        // 新 control Plan
        val indicatorMap = new concurrent.TrieMap[Long, IndicatorResult]
        indicatorMap.put(subIndicatorId, input._2)
        historyRun =indicatorMap
        val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]

        runMap.put(runId, indicatorMap)

        val logisticIndicatorMap = new concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]]

        logisticIndicatorMap.put(logisticIndicatorId, runMap)

        val versionMap = new concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorResult]]]]


        versionMap.put(controlPlanVersion, logisticIndicatorMap)


        historyIndicatorResultByAll.put(controlPlanId, versionMap)

      }

        // {"controlPlanId": {"version" : {"LogisticIndicator结果 的indicatorId": {"runid"": {"参与计算的indicatorId"": "IndicatorResult"}}}}}

        if (historyRun.size>1) {

          val subIndicatorIdList = historyRun.map(m => m._2.indicatorId).toList.sortBy(x => x)
          val mainIndicatorIdList = indicatorList.map(_.toLong).sortBy(x => x)

          if (subIndicatorIdList.containsSlice(mainIndicatorIdList)) {

            historyIndicatorResultByAll(controlPlanId)(controlPlanVersion)(logisticIndicatorId).remove(runId)
            val stringToLongToResult = historyIndicatorResultByAll(controlPlanId)(controlPlanVersion)(logisticIndicatorId)
            logger.warn(s"logisticIndicatorId :$logisticIndicatorId runNum : ${stringToLongToResult.size} ")

            historyRun.map(_._2).toList

          } else {
            Nil
          }
        } else Nil



    }

    }else{
      Nil
    }

  }
}
