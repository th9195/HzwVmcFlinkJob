package com.hzw.fdc.function.offline.MainFabOfflineIndicator


import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorConfig, OfflineIndicatorResult}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 *  对某个Indicator计算当前值与某些个历史值的移动加权平均数
 *  Formula：EWMA(n) = λInd(n) +（1-λ）*EWMA(n-1)，其中Ind(n)表示某个Indictaor的当前值，
 *  λ为系数，EWMA(n-1)表示该Indicator对应的上一个点的EWMA值
 */
class FdcOfflineEWMAProcessFunction extends KeyedProcessFunction[String,
  FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineEWMAProcessFunction])

  // 配置 {"tsdkId + indicatorid": "λ" : "IndicatorConfig"}}}
  var EWMAIndicatorByAll = new  TrieMap[String, TrieMap[String, OfflineIndicatorConfig]]()

  //数据缓存: tsdkId + indicatorid -> EWMAValueList)
  var EWMAValue = new TrieMap[String, List[Double]]()

  override def processElement(record: FdcData[OfflineIndicatorResult], ctx: KeyedProcessFunction[String,
    FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                              out: Collector[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "offlineIndicator" =>
          val indicatorResult = record.datas

          val indicatorId = indicatorResult.indicatorId.toString
          val taskId = indicatorResult.taskId.toString

          val offlineKey = taskId + "|" + indicatorId
          val offlineChamberKey = s"$taskId|${indicatorResult.toolName}|${indicatorResult.chamberName}|$indicatorId"

          // 保留配置信息
          if(!EWMAIndicatorByAll.contains(offlineKey)) {
            val driftMap = new TrieMap[String, OfflineIndicatorConfig]()
            val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

            for (offlineIndicatorConfig <- offlineIndicatorConfigList) {
              if (offlineIndicatorConfig.algoClass == "EWMA") {

                //key为原始indicator
                val algoParam = offlineIndicatorConfig.algoParam
                var key = ""
                var driftX = ""
                try {
                  val algoParamList = algoParam.split("\\|")
                  key = algoParamList(0)
                  if (algoParamList.size >= 2) {
                    driftX = algoParamList(1)
                  }
                } catch {
                  case ex: Exception => logger.info(s"flatMapOutputStream algoParamList")
                }

                if(key == indicatorId){
                  driftMap.put(driftX, offlineIndicatorConfig)
                }
              }
            }
            if(driftMap.nonEmpty) {
              EWMAIndicatorByAll.put(offlineKey, driftMap)
            }
          }

          if (!this.EWMAIndicatorByAll.contains(offlineKey)) {
            return
          }

          val EWMAMap = EWMAIndicatorByAll(offlineKey)

          val IndicatorList = ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()
          for ((λ, config) <- EWMAMap) {
            try {
              IndicatorList.append(EWMAFunction(λ.toDouble, indicatorResult, config, offlineChamberKey))
            } catch {
              case ex: Exception => logger.warn(s"EWMAFunction error $ex  indicatorResult:$indicatorResult ")

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
                IndicatorList.append(EWMAFunction(λ.toDouble, DriftIndicator, config, offlineChamberKey))
            }
          }
          out.collect(IndicatorList)

        case _ => logger.warn(s"EWMAIndicator processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(s"EWMA NumberFormatException $record \t $ex")
    }
  }

  /**
   * 获取缓存的历史数据，用于计算
   */
  def getCacheHistoryData(indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig, indicatorValueDouble: Double,
                          λ: Double, offlineChamberKey: String): Double = {


    val NowEWMAVale = λ * indicatorValueDouble
    var res = NowEWMAVale
    // 判断indicatorId 之前是否存在
    if (!this.EWMAValue.contains(offlineChamberKey)) {
      this.EWMAValue += (offlineChamberKey -> List(NowEWMAVale))
    } else {
      val EWMAList = this.EWMAValue(offlineChamberKey)
      res = NowEWMAVale + (1 - λ) * EWMAList.last

      val newEWMAList = EWMAList :+ res
      val dropEWMAList = if (newEWMAList.size > 4) {
        newEWMAList.drop(1)
      } else {
        newEWMAList
      }

      this.EWMAValue += (offlineChamberKey -> dropEWMAList)

    }
    res
  }


  /**
   *  计算EWMA值
   */
  def EWMAFunction(λ: Double, indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig, offlineChamberKey: String):
  (OfflineIndicatorResult, Boolean, Boolean, Boolean) = {

    val indicatorValueDouble = indicatorResult.indicatorValue.toDouble

    val NowEWMAVale = getCacheHistoryData(indicatorResult, config, indicatorValueDouble, λ, offlineChamberKey)

    val EWMAIndicator = OfflineIndicatorResult(
      isCalculationSuccessful = true,
      "",
      indicatorResult.batchId,
      indicatorResult.taskId,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.chamberName,
      NowEWMAVale.toString,
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

    (EWMAIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}


