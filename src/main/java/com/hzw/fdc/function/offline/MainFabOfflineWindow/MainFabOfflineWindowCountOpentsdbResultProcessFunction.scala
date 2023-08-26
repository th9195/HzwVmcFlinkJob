package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * @author ：gdj
 * @date ：Created in 2021/7/29 11:04
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineWindowCountOpentsdbResultProcessFunction extends ProcessWindowFunction[(OfflineTask, OfflineOpentsdbResult), (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), String, GlobalWindow] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineWindowCountOpentsdbResultProcessFunction])

  override def process(key: String, context: Context, elements: Iterable[(OfflineTask, OfflineOpentsdbResult)], out: Collector[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData)]): Unit = {

    //聚合sensor
    try {
      val offlineTask = elements.head._1
      val results = elements.toList
      val headSensorRun = elements.head._2


      val timeKeyData = new mutable.HashMap[String, OfflineMainFabRawData]()


      for (one <- results) {
        val oneRun = one._2
        for (elem <- oneRun.values) {
          if (timeKeyData.contains(oneRun.toolName + oneRun.chamberName + elem.time)) {
            val list = timeKeyData.get(oneRun.toolName + oneRun.chamberName + elem.time).get.data
            val buffer = list :+ OfflineSensorData(oneRun.sensorAlias, oneRun.sensorAlias, elem.value, "unit")

            timeKeyData.put(oneRun.toolName + oneRun.chamberName + elem.time, OfflineMainFabRawData(
              dataType = MainFabConstants.rawData,
              toolName = oneRun.toolName,
              chamberName = oneRun.chamberName,
              timestamp = elem.time,
              runId = oneRun.runId,
              runStart = oneRun.runStart,
              runEnd = oneRun.runEnd,
              dataMissingRatio = oneRun.dataMissingRatio,
              stepId = elem.stepId,
              stepName = "N/A",
              data = buffer
            ))
          } else {
            timeKeyData.put(oneRun.toolName + oneRun.chamberName + elem.time, OfflineMainFabRawData(
              dataType = MainFabConstants.rawData,
              toolName = oneRun.toolName,
              chamberName = oneRun.chamberName,
              timestamp = elem.time,
              runId = oneRun.runId,
              runStart = oneRun.runStart,
              runEnd = oneRun.runEnd,
              dataMissingRatio = oneRun.dataMissingRatio,
              stepId = elem.stepId,
              stepName = "N/A",
              data = List[OfflineSensorData](
                OfflineSensorData(oneRun.sensorAlias, oneRun.sensorAlias, elem.value, "unit")
              )
            ))
          }
        }

      }


      val rawDataList = timeKeyData.map(_._2).toList

      //    logger.warn(s"OfflineTask ${offlineTask.toJson} rawDataList:${rawDataList.toJson}")

     val taskRunData= if(rawDataList.isEmpty){
        OfflineTaskRunData(toolName = headSensorRun.toolName,
          chamberName = headSensorRun.chamberName,
          dataMissingRatio = headSensorRun.dataMissingRatio,
          runId = headSensorRun.runId,
          runStart = headSensorRun.runStart,
          runEnd = headSensorRun.runEnd,
          isCalculationSuccessful = false,
          calculationInfo = headSensorRun.calculationInfo)

      }else{
        OfflineTaskRunData(toolName = headSensorRun.toolName,
          chamberName = headSensorRun.chamberName,
          dataMissingRatio = headSensorRun.dataMissingRatio,
          runId = headSensorRun.runId,
          runStart = headSensorRun.runStart,
          runEnd = headSensorRun.runEnd,
          isCalculationSuccessful = true,
          calculationInfo = "")
      }

      out.collect((offlineTask, rawDataList.sortBy(_.timestamp),taskRunData))
    } catch {
      case e:Exception =>logger.warn(s"MainFabOfflineWindowCountOpentsdbResultProcessFunction elements:${elements.toJson} error ${ExceptionInfo.getExceptionInfo(e)}")
    }

  }
}
