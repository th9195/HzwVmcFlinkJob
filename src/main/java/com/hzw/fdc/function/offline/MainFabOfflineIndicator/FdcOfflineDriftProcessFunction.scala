package com.hzw.fdc.function.offline.MainFabOfflineIndicator

import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorConfig, OfflineIndicatorResult}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-09-01-18:01
 *
 */
class FdcOfflineDriftProcessFunction extends KeyedProcessFunction[String,
  FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineDriftProcessFunction])

  // 配置 {"taskId + indicatorid": { driftX: "IndicatorConfig"}}}
  var driftIndicatorByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, OfflineIndicatorConfig]]()

  //drift算法, 数据缓存: taskId+indicatorid -> driftValueList)
  var driftValue = new concurrent.TrieMap[String,  List[Double]]()


  override def processElement(record: FdcData[OfflineIndicatorResult], ctx: KeyedProcessFunction[String, FdcData[OfflineIndicatorResult],
    ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]#Context,
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
          if(!driftIndicatorByAll.contains(offlineKey)){
            val driftMap = new TrieMap[String, OfflineIndicatorConfig]()
            val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

            for(offlineIndicatorConfig <- offlineIndicatorConfigList){
              if(offlineIndicatorConfig.algoClass == "drift"){

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
                }catch {
                  case ex: Exception => logger.info(s"flatMapOutputStream algoParamList")
                }

                if(key == indicatorId){
                  driftMap.put(driftX, offlineIndicatorConfig)
                }
              }
            }
            if(driftMap.nonEmpty) {
              driftIndicatorByAll.put(offlineKey, driftMap)
            }
          }

          if (!this.driftIndicatorByAll.contains(offlineKey)) {
            return
          }

          val driftMap = driftIndicatorByAll(offlineKey)
          val IndicatorList = ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()

          try {
            //取出最大的dritfx
            val max = driftMap.keys.map(_.toInt).max

            if (driftValue.contains(offlineChamberKey)) {
              val newKeyList = driftValue(offlineChamberKey) :+ indicatorValueDouble
              val dropKeyList = if (newKeyList.size > max + 1) {
                newKeyList.drop(1)
              } else {
                newKeyList
              }
              driftValue += (offlineChamberKey -> dropKeyList)
            } else {
              driftValue += (offlineChamberKey -> List(indicatorValueDouble))
            }

            for ((k, v) <- driftMap) {
              try {
                val res = driftFunction(k, indicatorResult, v, offlineChamberKey)
                if (res._1 != null) {
                  IndicatorList.append(res)
                } else {
                  logger.warn(s"FdcDrift job null: " + this.driftValue(offlineChamberKey) + "\t" + record)
                }
              } catch {
                case ex: Exception => logger.warn(s"driftFunction error $ex  config:$v  indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          }catch {
            case ex: Exception => logger.warn(s"drift NumberFormatException $ex")
              for ((k, v) <- driftMap) {
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
        case _ => logger.warn(s"DriftIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(s"drift NumberFormatException $record \t $ex")
    }
  }

  /**
   * Drift（X）：对某个Indicator计算当前值与历史值的差
   * Formula：Ind_n – Ind_(n-X)，其中Ind_n表示某个Indictaor的当前值， Ind_(n-X)表示当前值前的第X个历史值
   */
  def driftFunction(driftX: String, indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig,
                    offlineChamberKey: String):
  (OfflineIndicatorResult, Boolean, Boolean, Boolean) = {

    val DriftValueList = driftValue(offlineChamberKey)

    val driftXInt = driftX.toInt
    val size = DriftValueList.size
    val Index = size - 1 - driftXInt

    val DriftIndicator = if (Index >= 0) {
      val driftResult = DriftValueList.last - DriftValueList(Index)
      OfflineIndicatorResult(
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
    } else {
      null
    }
    (DriftIndicator, config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}

