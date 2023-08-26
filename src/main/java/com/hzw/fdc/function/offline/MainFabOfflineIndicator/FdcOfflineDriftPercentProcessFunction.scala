package com.hzw.fdc.function.offline.MainFabOfflineIndicator

import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorConfig, OfflineIndicatorResult}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

/**
 * Drift(Percent） = [Ind(N)-Ind(N-M)]/Ind(N). (以示区别，名字更改为Drift(Percent))
 * Formula：[Ind(n) – Ind(n-X)]/Ind(n)，即n是当前值，n-X是前面X个值
 */
class FdcOfflineDriftPercentProcessFunction extends KeyedProcessFunction[String,
  FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineDriftPercentProcessFunction])

  // 配置 {"taskId + indicatorid": {"driftX" : "IndicatorConfig"}}}}
  var driftPercentIndicatorByAll = new TrieMap[String, TrieMap[String, OfflineIndicatorConfig]]()

  //drift算法, 数据缓存: taskId + chamber + indicatorid -> driftValueList)
  var driftPercentValue = new concurrent.TrieMap[String,  List[Double]]()


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

          val indicatorValueDouble = indicatorResult.indicatorValue.toDouble

          // 保留配置信息
          if (!driftPercentIndicatorByAll.contains(offlineKey)) {
            val driftMap = new TrieMap[String, OfflineIndicatorConfig]()
            val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

            for (offlineIndicatorConfig <- offlineIndicatorConfigList) {
              if (offlineIndicatorConfig.algoClass == "driftPercent") {

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
              driftPercentIndicatorByAll.put(offlineKey, driftMap)
            }
          }

          if (!this.driftPercentIndicatorByAll.contains(offlineKey)) {
            return
          }


          val driftPercentMap = driftPercentIndicatorByAll(offlineKey)
          val IndicatorList = ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()

          try {
            //取出最大的dritfx
            val max = driftPercentMap.keys.map(_.toInt).max

            if (driftPercentValue.contains(offlineChamberKey)) {
              val newKeyList = driftPercentValue(offlineChamberKey) :+ indicatorValueDouble
              val dropKeyList = if (newKeyList.size > max + 1) {
                newKeyList.drop(1)
              } else {
                newKeyList
              }
              driftPercentValue += (offlineChamberKey -> dropKeyList)
            } else {
              driftPercentValue += (offlineChamberKey -> List(indicatorValueDouble))
            }

            for ((k, v) <- driftPercentMap) {
              try {
                val res = driftFunction(k, indicatorResult, v, offlineChamberKey)
                if (res._1 != null) {
                  IndicatorList.append(res)
                } else {
                  logger.warn(s"FdcDrift job null:" + record)
                }
              } catch {
                case ex: Exception => logger.warn(s"driftFunction error $ex indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          } catch {
            case ex: Exception => logger.warn(s"drift NumberFormatException $ex")
              for ((k, v) <- driftPercentMap) {
                val config = v
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
                IndicatorList.append((DriftIndicator, config.driftStatus, config.calculatedStatus, config.logisticStatus))
              }
              out.collect(IndicatorList)
          }

        case _ => logger.warn(s"DriftIndicator job DriftPercentProcessElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(s"drift NumberFormatException $record \t $ex")
    }
  }

  /**
   * Drift（X）：对某个Indicator计算当前值与历史值的差
   * Formula：Ind_n – Ind_(n-X)/Ind_n，其中Ind_n表示某个Indictaor的当前值， Ind_(n-X)表示当前值前的第X个历史值
   */
  def driftFunction(driftX: String, indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig,
                    offlineChamberKey: String):
  (OfflineIndicatorResult, Boolean, Boolean, Boolean) = {

    val DriftValueList: List[Double] = driftPercentValue(offlineChamberKey)

    var driftResult: Double = 0.00
    val driftXInt = driftX.toInt
    val size = DriftValueList.size
    val Index = size - 1 - driftXInt

    if (Index >= 0) {
      driftResult = (DriftValueList.last - DriftValueList(Index)) / DriftValueList.last
    }else{
      logger.warn(s"driftFunction Index >= 0 config:$config  indicatorResult:$indicatorResult")
      return (null, config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }

    val  DriftIndicator = OfflineIndicatorResult(
      isCalculationSuccessful = true,
      "",
      indicatorResult.batchId,
      indicatorResult.taskId,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.chamberName,
      driftResult.toString,
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

    (DriftIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}

