package com.hzw.fdc.service.offline

import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabIndicatorDao
import com.hzw.fdc.function.offline.MainFabOfflineIndicator.{IndicatorCommFunction, MainFabOfflineIndicatorProcessFunction}
import com.hzw.fdc.function.online.MainFabIndicator.IndicatorAlgorithm
import com.hzw.fdc.scalabean.ALGO.ALGO
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-06-28-18:26
 *
 */
class MainFabOfflineIndicatorService extends TService with IndicatorCommFunction {

  private val dao = new MainFabIndicatorDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineIndicatorService])

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {

    // 数据流
    val KafkaRawDataStream: DataStream[FdcData[OfflineWindowListData]] = getDatas()


    val outputStream: DataStream[(ALGO, ListBuffer[OfflineRawData], OfflineIndicatorConfig,
      List[OfflineIndicatorConfig])] = KafkaRawDataStream
      .filter(_.`dataType` == "offlineFdcWindowDatas")
      .keyBy(_.datas.taskId.toString)
      .process(new MainFabOfflineIndicatorProcessFunction)
      .name("OfflineFdcGenerateIndicator")


    val indicatorOutputStream: DataStream[(OfflineIndicatorResult, Boolean, Boolean, Boolean)] = outputStream.map(
      input => {
        val algo = input._1
        val fdcRawDataList = input._2
        val fdcRawData = input._2.head
        val indicatorConfig = input._3
        val offlineIndicatorConfigList = input._4

        val runStartTime = fdcRawData.runStartTime
        val runEndTime = fdcRawData.runEndTime
        val windowStartTime = fdcRawData.windowStartTime
        val windowEndTime = fdcRawData.windowEndTime
        val windowDataCreateTime = fdcRawData.windowEndDataCreateTime
        val unit = fdcRawData.unit
        val indicatorId = indicatorConfig.indicatorId
        val indicatorName = indicatorConfig.indicatorName
        val driftStatus = indicatorConfig.driftStatus
        val calculatedStatus = indicatorConfig.calculatedStatus
        val logisticStatus = indicatorConfig.logisticStatus
        val algoClass = indicatorConfig.algoClass

        try {
          val algoParam = indicatorConfig.algoParam
          var ALGO_PARAM1 = ""
          var ALGO_PARAM2 = ""
          try {
            val algoParamList = algoParam.split("\\|")
            ALGO_PARAM1 = algoParamList(0)
            if (algoParamList.size >= 2) {
              ALGO_PARAM2 = algoParamList(1)
            }
          }catch { case ex: Exception => None }

          var startTimestamp = -1L

          var offlineIndicatorValueList: List[String] = List()
          for (elem <- fdcRawDataList) {

            val elemWindowStartTime = elem.windowStartTime
            val elemWindowEndTime = elem.windowEndTime

            val catWindowData = elem.data.to[ListBuffer]
            startTimestamp = catWindowData.last.timestamp.toLong
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

                case ALGO.TIMERANGE => value = IndicatorAlgorithm.TimeRangeAlgorithm(elemWindowStartTime,elemWindowEndTime)

                case ALGO.MeanT => value = IndicatorAlgorithm.meanT(catWindowData)

                case ALGO.StdDevT => value = IndicatorAlgorithm.stdDevT(catWindowData)

                case ALGO.AREA => value = IndicatorAlgorithm.area(catWindowData)

                case ALGO.CycleCount => value =  fdcRawData.cycleUnitCount.toString

                case _ => logger.warn("no match Algorithm" + input)
              }
            }else{
              logger.warn(s"catWindowData Empty: ")
            }
            if(value != "error"){
              // 特殊处理cycle count
              if(algo == ALGO.CycleCount){
                if(offlineIndicatorValueList.isEmpty){
                  offlineIndicatorValueList = offlineIndicatorValueList :+ value
                }
              }else{
                offlineIndicatorValueList = offlineIndicatorValueList :+ value
              }
            }
          }

          var isCalculationSuccessful = false
          var calculationInfo = ""
          var indicatorValue = ""

          if(!fdcRawData.isCalculationSuccessful || offlineIndicatorValueList.isEmpty){
            isCalculationSuccessful = false
            calculationInfo = s"值为空或者window计算失败"
            indicatorValue = ""
          }else{
            isCalculationSuccessful = true
            calculationInfo = ""
            indicatorValue = offlineIndicatorValueList.reduce(_ + "|" + _)
          }

          val res = OfflineIndicatorResult(
            isCalculationSuccessful,
            calculationInfo,
            fdcRawData.batchId,
            fdcRawData.taskId,
            fdcRawData.runId,
            fdcRawData.toolName,
            fdcRawData.chamberName,
            indicatorValue,
            indicatorId,
            indicatorName,
            algoClass,
            runStartTime,
            runEndTime,
            windowStartTime,
            windowEndTime,
            windowDataCreateTime,
            unit,
            offlineIndicatorConfigList
          )
          Try(res, driftStatus, calculatedStatus, logisticStatus)
        }catch {
          case ex: Exception => logger.warn(s"indicator job  error : ${ExceptionInfo.getExceptionInfo(ex)}")
            val res = OfflineIndicatorResult(
              isCalculationSuccessful = false,
              s"base indicator计算失败: $ex",
              fdcRawData.batchId,
              fdcRawData.taskId,
              fdcRawData.runId,
              fdcRawData.toolName,
              fdcRawData.chamberName,
              "",
              indicatorId,
              indicatorName,
              algoClass,
              runStartTime,
              runEndTime,
              windowStartTime,
              windowEndTime,
              windowDataCreateTime,
              unit,
              offlineIndicatorConfigList
            )
            Try(res, driftStatus, calculatedStatus, logisticStatus)
        }
      }
    )
      .filter(_.nonEmpty)
      .map(x => {parseOption(x)})

    writeKafka(indicatorOutputStream)
  }


  /**
   * 获取fdcWindowData数据
   */
  override protected def getDatas(): DataStream[FdcData[OfflineWindowListData]] = {

    getDao().getKafkaSource[FdcData[OfflineWindowListData]](
      ProjectConfig.KAFKA_OFFLINE_WINDOW_DATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_OFFLINE_INDICATOR_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_BASE_INDICATOR_JOB_INDICATOR_KAFKA_SOURCE_UID)
  }
}
