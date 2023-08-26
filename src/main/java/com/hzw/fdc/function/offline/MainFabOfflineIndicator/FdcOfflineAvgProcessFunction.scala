package com.hzw.fdc.function.offline.MainFabOfflineIndicator

  import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorConfig, OfflineIndicatorResult}
  import org.apache.flink.util.Collector
  import org.slf4j.{Logger, LoggerFactory}
  import org.apache.flink.streaming.api.functions.KeyedProcessFunction

  import scala.collection.concurrent.TrieMap
  import scala.collection.concurrent
  import scala.collection.mutable.ListBuffer

  /**
   *
   * IndicatorAvg = [Ind(n)+Ind(n-X)]/2
   * Formula：[Ind-n + Ind-(n-X)]/2，即n是当前值，n-X是前面X个值，选择着算法之后，需要弹出对话框：
   * Parameter               Type            Value
   * InputIndicator          string      Ind (注：该地方点击之后列出所有已建立的Indicator，并选择其中之一)
   * X                                 Int            10
   */
  class FdcOfflineAvgProcessFunction extends KeyedProcessFunction[String,
    FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]] {

    private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineAvgProcessFunction])

    // 配置 {"taskId + indicatorid": {"AvgX" : "IndicatorConfig"}}}
    var AvgIndicatorByAll = new TrieMap[String, TrieMap[String, OfflineIndicatorConfig]]()

    //数据缓存: taskId + chamber+ indicatorid -> AvgValueList)
    var AvgValue = new concurrent.TrieMap[String, List[Double]]()


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
            if (!AvgIndicatorByAll.contains(offlineKey)) {
              val driftMap = new TrieMap[String, OfflineIndicatorConfig]()
              val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

              for (offlineIndicatorConfig <- offlineIndicatorConfigList) {
                if (offlineIndicatorConfig.algoClass == "indicatorAvg") {

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
              if(driftMap.nonEmpty){
                AvgIndicatorByAll.put(offlineKey, driftMap)
              }
            }


            if (!this.AvgIndicatorByAll.contains(offlineKey)) {
              return
            }

            val avgMap = AvgIndicatorByAll(offlineKey)

            val IndicatorList = ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()

            try {
              //取出最大的dritfx
              val max = avgMap.keys.map(_.toInt).max

              if (AvgValue.contains(offlineChamberKey)) {
                val newKeyList = AvgValue(offlineChamberKey) :+ indicatorValueDouble
                val dropKeyList = if (newKeyList.size > max + 1) {
                  newKeyList.drop(1)
                } else {
                  newKeyList
                }
                AvgValue += (offlineChamberKey -> dropKeyList)
              } else {
                AvgValue += (offlineChamberKey -> List(indicatorValueDouble))
              }

              for ((k, v) <- avgMap) {
                try {
                  val res = AvgFunction(k, indicatorResult, v, offlineChamberKey)
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
                for ((k, v) <- avgMap) {
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

          case _ => logger.warn(s"AvgIndicator processElement no mach type")
        }
      } catch {
        case ex: Exception => logger.warn(s"Avg NumberFormatException $record \t $ex")
      }
    }


    /**
     *  计算Avg值
     */
    def AvgFunction(avgX: String, indicatorResult: OfflineIndicatorResult, config: OfflineIndicatorConfig,
                    offlineChamberKey: String):
    (OfflineIndicatorResult, Boolean, Boolean, Boolean) = {

      val nowAvgList: List[Double] = AvgValue(offlineChamberKey)

      val max = avgX.toInt + 1
      if(max > nowAvgList.size){
        logger.warn(s"AvgFunction AvgXInt > nowAvgList.size config:$config  indicatorResult:$indicatorResult")
        return (null,config.driftStatus, config.calculatedStatus, config.logisticStatus)
      }

      //提取列表的后n个元素
      val mathList: List[Double] = nowAvgList.takeRight(max)
      val Result: Double = (mathList.head + mathList.last) / 2

      val avgIndicator = OfflineIndicatorResult(
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

      (avgIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }
  }

