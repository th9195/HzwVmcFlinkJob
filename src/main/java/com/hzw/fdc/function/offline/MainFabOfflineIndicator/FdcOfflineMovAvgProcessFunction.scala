package com.hzw.fdc.function.offline.MainFabOfflineIndicator


import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorConfig, OfflineIndicatorResult}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}


/**
 *  对某个Indicator计算当前值与某些个历史值得平均数
 *  Formula：[Ind(n) + Ind(n-1) + Ind(n-2)+…+ Ind(n-X)]/(X+1)，其中Ind(n)表示某个Indictaor的当前值，Ind(n-1) ，
 *  Ind(n-2)，… ，Ind(n-x)表示当前值前的X个历史值
 */
class FdcOfflineMovAvgProcessFunction extends KeyedProcessFunction[String, FdcData[OfflineIndicatorResult],
  ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]  {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineMovAvgProcessFunction])

  // 配置 {"taskId + indicatorid": { "MovAvgX" : "IndicatorConfig"}}}}
  var MovAvgIndicatorByAll = new TrieMap[String, TrieMap[String, OfflineIndicatorConfig]]()

  //数据缓存: taskId + chamber + indicatorid -> MovAvgValueList
  var MovAvgValue = new mutable.HashMap[String,  List[Double]]()

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
          if(!MovAvgIndicatorByAll.contains(offlineKey)){
            val driftMap = new TrieMap[String, OfflineIndicatorConfig]()
            val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

            for(offlineIndicatorConfig <- offlineIndicatorConfigList){
              if(offlineIndicatorConfig.algoClass == "MA"){

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
              MovAvgIndicatorByAll.put(offlineKey, driftMap)
            }
          }


          if (!this.MovAvgIndicatorByAll.contains(offlineKey)) {
            return
          }

          val driftMap = MovAvgIndicatorByAll(offlineKey)
          val IndicatorList = ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()

          try {
            //取出最大的dritfx
            val max = driftMap.keys.map(_.toInt).max

            if (MovAvgValue.contains(offlineChamberKey)) {
              val newKeyList = MovAvgValue(offlineChamberKey) :+ indicatorValueDouble
              val dropKeyList = if (newKeyList.size > max + 1) {
                newKeyList.drop(1)
              } else {
                newKeyList
              }
              MovAvgValue += (offlineChamberKey -> dropKeyList)
            } else {
              MovAvgValue += (offlineChamberKey -> List(indicatorValueDouble))
            }

            for ((k, v) <- driftMap) {
              try {
                val res = MovAvgFunction(k, indicatorResult, v, offlineChamberKey)
                if (res._1 != null) {
                  IndicatorList.append(res)
                } else {
                  logger.warn(s"FdcDrift job null: " + record)
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
        case _ => logger.warn(s"MovAvgIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(s"MovAvg NumberFormatException $record \t $ex")
    }
  }


  /**
   *  计算MovAvg值
   */
  def MovAvgFunction(movAvgX: String, indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig,
                     offlineChamberKey: String): (OfflineIndicatorResult, Boolean, Boolean, Boolean) = {

    val nowMovAvgList: List[Double] = MovAvgValue(offlineChamberKey)

    val max = movAvgX.toInt + 1
    if(max > nowMovAvgList.size){
      logger.warn(s"MovAvgFunction movAvgXInt > nowMovAvgList.size config:$config  indicatorResult:$indicatorResult")
      return (null,config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }

    //提取列表的后n个元素
    val mathList: List[Double] = nowMovAvgList.takeRight(max)
    val Result: Double = mathList.sum / max

    val movAvgIndicator = OfflineIndicatorResult(
      isCalculationSuccessful = true,
      "",
      indicatorResult.batchId,
      indicatorResult.taskId,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.chamberName,
      Result.toString,
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

    (movAvgIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }

}
