package com.hzw.fdc.function.online.MainFabIndicator

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.ALGO.ALGO
import com.hzw.fdc.scalabean.{ALGO, FdcData, IndicatorConfig, IndicatorResult, RawData, WindowData, fdcWindowData, sensorDataList, windowListData}
import com.hzw.fdc.service.online.MainFabIndicatorService
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.mutable.ListBuffer


object FdcIndicatorProcessFunction {

  lazy private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabIndicatorService])

  /**
   * 数据流  ====》 基础的一些算法计算，比如avg，sum，slop
   */
  def FdcIndicatorMath(input: (ALGO, IndicatorConfig, ListBuffer[RawData])): (IndicatorResult, Boolean, Boolean, Boolean) ={
    try {
      val algo = input._1
      val fdcRawData = input._3.head

      val runStartTime = fdcRawData.runStartTime
      val runEndTime = fdcRawData.runEndTime
      val windowStartTime = fdcRawData.windowStartTime
      val windowEndTime = fdcRawData.windowEndTime
      val windowDataCreateTime = fdcRawData.windowindDataCreateTime
      val missingRatio = fdcRawData.dataMissingRatio
      val materialName = fdcRawData.materialName


      val indicatorConfig = input._2

      val limitStatus = fdcRawData.limitStatus
      val unit = fdcRawData.unit


      val indicatorName = indicatorConfig.indicatorName

      val algoParam = indicatorConfig.algoParam

      var ALGO_PARAM1 = ""
      var ALGO_PARAM2 = ""
      try {
        val algoParamList = algoParam.split("\\|")
        ALGO_PARAM1 = algoParamList(0)
        if (algoParamList.size >= 2) {
          ALGO_PARAM2 = algoParamList(1)
        }
      } catch {
        case ex: Exception => None
      }

      val driftStatus = indicatorConfig.driftStatus
      val calculatedStatus = indicatorConfig.calculatedStatus
      val logisticStatus = indicatorConfig.logisticStatus
      val controlPlanId = indicatorConfig.controlPlanId
      var startTimestamp = -1L

      val fdcRawDataList: ListBuffer[RawData] = input._3
      var indicatorValueList: List[String] = List()

      // 多个rawdataList, 比如cycleWindow 特殊处理
      for (elem <- fdcRawDataList) {

        val elemWindowStartTime = elem.windowStartTime
        val elemWindowEndTime = elem.windowEndTime

        val catWindowData: ListBuffer[sensorDataList] = elem.data.to[ListBuffer]
        startTimestamp = catWindowData.last.timestamp
        var value = "0"
        if (catWindowData.nonEmpty) {

          algo match {
            case ALGO.AVG => value = IndicatorAlgorithm.AVGAlgorithm(catWindowData)

            case ALGO.MAX => value = IndicatorAlgorithm.MAXAlgorithm(catWindowData, ALGO_PARAM1)

            case ALGO.MIN => value = IndicatorAlgorithm.MINAlgorithm(catWindowData, ALGO_PARAM1)

            case ALGO.FIRSTPOINT => value = IndicatorAlgorithm.FIRSTPOINTAlgorithm(catWindowData)

            case ALGO.HITTIME => value = IndicatorAlgorithm.HitTimeAlgorithm(catWindowData, ALGO_PARAM1, ALGO_PARAM2)

            case ALGO.HITCOUNT => value = IndicatorAlgorithm.HitCountAlgorithm(catWindowData, ALGO_PARAM1, ALGO_PARAM2)

            case ALGO.RANGE => value = IndicatorAlgorithm.RANGEAlgorithm(catWindowData, ALGO_PARAM1, ALGO_PARAM2)

            case ALGO.SLOPE => value = IndicatorAlgorithm.SlopeAlgorithm(catWindowData)

            case ALGO.STD => value = IndicatorAlgorithm.STDAlgorithm(catWindowData)

            case ALGO.SUM => value = IndicatorAlgorithm.SUMAlgorithm(catWindowData)

            case ALGO.TIMERANGE => value = IndicatorAlgorithm.TimeRangeAlgorithm(elemWindowStartTime, elemWindowEndTime)

            case ALGO.MeanT => value = IndicatorAlgorithm.meanT(catWindowData)

            case ALGO.StdDevT => value = IndicatorAlgorithm.stdDevT(catWindowData, fdcRawData.runId,
              indicatorConfig.controlPlanName, indicatorName)

            case ALGO.AREA => value = IndicatorAlgorithm.area(catWindowData)
            //
            case _ => logger.warn("no match Algorithm" + input)
          }

        }
        if (value != "error") {
          indicatorValueList = indicatorValueList :+ value
        }
      }
      if(indicatorValueList.nonEmpty) {
        val indicatorValue = indicatorValueList.reduce(_ + "|" + _)

        val indicatorResult = IndicatorResult(controlPlanId = controlPlanId,
          controlPlanName = indicatorConfig.controlPlanName,
          controlPlanVersion = indicatorConfig.controlPlanVersion,
          locationId = fdcRawData.locationId,
          locationName = fdcRawData.locationName,
          moduleId = fdcRawData.moduleId,
          moduleName = fdcRawData.moduleName,
          toolGroupId = fdcRawData.toolGroupId,
          toolGroupName = fdcRawData.toolGroupName,
          chamberGroupId = fdcRawData.chamberGroupId,
          chamberGroupName = fdcRawData.chamberGroupName,
          recipeGroupName = fdcRawData.recipeGroupName,
          runId = fdcRawData.runId,
          toolName = fdcRawData.toolName,
          toolId = fdcRawData.toolId,
          chamberName = fdcRawData.chamberName,
          chamberId = fdcRawData.chamberId,
          indicatorValue = indicatorValue,
          indicatorId = indicatorConfig.indicatorId,
          indicatorName = indicatorName,
          algoClass = indicatorConfig.algoClass,
          indicatorCreateTime = System.currentTimeMillis(),
          missingRatio = missingRatio,
          configMissingRatio = indicatorConfig.missingRatio,
          runStartTime = runStartTime,
          runEndTime = runEndTime,
          windowStartTime = windowStartTime,
          windowEndTime = windowEndTime,
          windowDataCreateTime = windowDataCreateTime,
          limitStatus = limitStatus,
          materialName = materialName,
          recipeName = fdcRawData.recipeName,
          recipeId = fdcRawData.recipeId,
          product = fdcRawData.productName,
          stage = fdcRawData.stage,
          bypassCondition = indicatorConfig.bypassCondition,
          pmStatus = fdcRawData.pmStatus,
          pmTimestamp = fdcRawData.pmTimestamp,
          area = fdcRawData.area,
          section = fdcRawData.section,
          mesChamberName = fdcRawData.mesChamberName,
          lotMESInfo = fdcRawData.lotMESInfo,
          unit = unit,
          dataVersion = fdcRawData.dataVersion,
          cycleIndex = fdcRawData.cycleIndex
        )

        val elem = (indicatorResult, driftStatus, calculatedStatus, logisticStatus)
        return elem
      }
    }catch {
      case ex: Exception =>
        logger.warn(s"indicator job  ExceptionInfo: ${ExceptionInfo.getExceptionInfo(ex)}")
    }
    null
  }


  def parseWindow(record: JsonNode): fdcWindowData ={
    try{
      record.get("dataType").asText() match {
        case "fdcWindowDatas" =>
          val windowData = toBean[fdcWindowData](record)

          return windowData
        case _ => None
      }
    }catch {
      case ex: Exception => logger.warn(s"parseWindow ExceptionInfo: ${ExceptionInfo.getExceptionInfo(ex)}")
    }
    null
  }
}
