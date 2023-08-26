package com.hzw.fdc.function.offline.MainFabOfflineIndicator

import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorResult}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

class FdcOfflineAbsProcessFunction extends KeyedProcessFunction[String,
  FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineAbsProcessFunction])

  override def processElement(record: FdcData[OfflineIndicatorResult], ctx: KeyedProcessFunction[String,
    FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                              out: Collector[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "offlineIndicator" =>
          val indicatorResult = record.datas

          try {
            for (config <- indicatorResult.offlineIndicatorConfigList) {
              if (config.algoClass == "abs" && config.algoParam == indicatorResult.indicatorId.toString) {
                try {

                  val indicatorValue = Math.abs(indicatorResult.indicatorValue.toDouble).toString

                  val res = OfflineIndicatorResult(indicatorResult.isCalculationSuccessful
                    , indicatorResult.calculationInfo
                    , indicatorResult.batchId
                    , indicatorResult.taskId
                    , indicatorResult.runId
                    , indicatorResult.toolName
                    , indicatorResult.chamberName
                    , indicatorValue
                    , config.indicatorId
                    , config.indicatorName
                    , config.algoClass
                    , indicatorResult.runStartTime
                    , indicatorResult.runEndTime
                    , indicatorResult.windowStartTime
                    , indicatorResult.windowEndTime
                    , indicatorResult.windowindDataCreateTime
                    , indicatorResult.unit
                    , indicatorResult.offlineIndicatorConfigList)
                  out.collect(ListBuffer((res, config.driftStatus, config.calculatedStatus, config.logisticStatus)))
                } catch {
                  case ex: Exception => logger.warn(s"AbsFunction error $ex indicatorResult:$indicatorResult ")
                }
              }
            }
          } catch {
            case ex: Exception => logger.warn(s"drift NumberFormatException $ex")
              for (config <- indicatorResult.offlineIndicatorConfigList) {
                if (config.algoClass == "abs" && config.algoParam == indicatorResult.indicatorId.toString) {
                  val DriftIndicator = OfflineIndicatorResult(
                    isCalculationSuccessful = false,
                    s"$ex",
                    indicatorResult.batchId,
                    indicatorResult.taskId,
                    indicatorResult.runId,
                    indicatorResult.toolName,
                    indicatorResult.chamberName,
                    "",
                    config.indicatorId,
                    config.indicatorName,
                    config.algoClass,
                    indicatorResult.runStartTime,
                    indicatorResult.runEndTime,
                    indicatorResult.windowStartTime,
                    indicatorResult.windowEndTime,
                    indicatorResult.windowindDataCreateTime,
                    "",
                    indicatorResult.offlineIndicatorConfigList
                  )
                  out.collect(ListBuffer((DriftIndicator, config.driftStatus, config.calculatedStatus, config.logisticStatus)))
                }
              }
          }

        case _ => logger.warn(s"AbsIndicator processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(s"Abs NumberFormatException $record \t $ex")
    }
  }
}
