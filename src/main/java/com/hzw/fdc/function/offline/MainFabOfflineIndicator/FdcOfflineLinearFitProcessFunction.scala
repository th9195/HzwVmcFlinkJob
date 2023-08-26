package com.hzw.fdc.function.offline.MainFabOfflineIndicator


import com.hzw.fdc.engine.alg.AlgorithmUtils
import com.hzw.fdc.engine.alg.AlgorithmUtils.DataPoint
import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorConfig, OfflineIndicatorResult}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{FdcData, IndicatorConfig, IndicatorResult, OfflineIndicatorConfig, OfflineIndicatorResult}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer


/**
 *  对某个Indicator计算当前值与某些个历史值得平均数
 *  Formula：[Ind(n) + Ind(n-1) + Ind(n-2)+…+ Ind(n-X)]/(X+1)，其中Ind(n)表示某个Indictaor的当前值，Ind(n-1) ，
 *  Ind(n-2)，… ，Ind(n-x)表示当前值前的X个历史值
 */
class FdcOfflineLinearFitProcessFunction extends KeyedProcessFunction[String,
  FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineLinearFitProcessFunction])

  // 配置 {"taskId + indicatorid": {"LinearFitX" : "IndicatorConfig"}}}}
  var LinearFitIndicatorByAll = new TrieMap[String, TrieMap[String, OfflineIndicatorConfig]]()

  //数据缓存: taskId + chamber + indicatorid -> LinearFitList
  var LinearFitValue = new concurrent.TrieMap[String,  List[DataPoint]]()


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
          if (!LinearFitIndicatorByAll.contains(offlineKey)) {
            val driftMap = new TrieMap[String, OfflineIndicatorConfig]()
            val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

            for (offlineIndicatorConfig <- offlineIndicatorConfigList) {
              if (offlineIndicatorConfig.algoClass == "linearFit") {

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
              LinearFitIndicatorByAll.put(offlineKey, driftMap)
            }
          }


//          val indicatorKey = indicatorResult.indicatorId.toString

          if (!this.LinearFitIndicatorByAll.contains(offlineKey)) {
            return
          }

          val LinearFitMap = LinearFitIndicatorByAll(offlineKey)

          val IndicatorList = ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()

          try {
            //取出最大的dritfx
            val LinearFitX = LinearFitMap.keys.map(_.toInt).max

            val indicatorValueDouble = indicatorResult.indicatorValue.toDouble

            if(LinearFitValue.contains(offlineChamberKey)){
              val newKeyList = LinearFitValue(offlineChamberKey) :+ new DataPoint(indicatorResult.runStartTime,indicatorValueDouble)
              val dropKeyList = if (newKeyList.size > LinearFitX + 1) { newKeyList.drop(1) } else { newKeyList }
              LinearFitValue += (offlineChamberKey -> dropKeyList)
            }else{
              LinearFitValue += (offlineChamberKey -> List(new DataPoint(indicatorResult.runStartTime, indicatorValueDouble)))
            }


            for ((k, v) <- LinearFitMap) {
              try {
                val res = LinearFitFunction(k, indicatorResult, v, offlineChamberKey)
                if (res._1 != null) {
                  IndicatorList.append(res)
                } else {
                  logger.warn(s"FdcDrift job null:" + record)
                }
              } catch {
                case ex: Exception => logger.warn(s"driftFunction error $ex indicatorResult:$indicatorResult ")
              }
            }

            if(IndicatorList.nonEmpty){
              out.collect(IndicatorList)
            }
          } catch {
            case ex: Exception => logger.warn(s"drift NumberFormatException $ex")
              for ((k, v) <- LinearFitMap) {
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
        case _ => logger.warn(s"LinearFitIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(s"LinearFit NumberFormatException $ex")
    }
  }


  /**
   *  计算MovAvg值
   */
  def LinearFitFunction(linearFitX: String, indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig,
                        offlineChamberKey: String): (OfflineIndicatorResult, Boolean, Boolean, Boolean) = {

    val nowMovAvgList: List[DataPoint] = LinearFitValue(offlineChamberKey)
    try {

      val max = linearFitX.toInt + 1
      if (max > nowMovAvgList.size) {
        logger.warn(s"LinearFitFunction linearFit > linearFit.size indicatorResult:$indicatorResult " +
          s"nowMovAvgList: $nowMovAvgList")
        return (null, config.driftStatus, config.calculatedStatus, config.logisticStatus)
      }

      //提取列表的后n个元素
      val mathList: List[DataPoint] = nowMovAvgList.takeRight(max)
      val Result: Double = AlgorithmUtils.linearFit(mathList.toArray: _*)

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

      (movAvgIndicator, config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }catch {
      case ex: Exception => logger.warn(s"LinearFit error: $ex\t " + nowMovAvgList)
        val res = OfflineIndicatorResult(
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
        (res, config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }
  }

}
