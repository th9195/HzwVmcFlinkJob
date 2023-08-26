package com.hzw.fdc.function.online.MainFabWindow

import com.hzw.fdc.scalabean.{FdcData, MainFabRawData, RunData, RunEventData}
import org.apache.flink.api.common.functions.MapFunction

import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：2021/11/27 17:12
 * @param $params
 * @return $returns
 */
class MainFabProcessEndRunDataMapFunction extends MapFunction[(RunEventData, RunEventData, List[MainFabRawData]),FdcData[RunData]]{
  override def map(event: (RunEventData, RunEventData, List[MainFabRawData])): FdcData[RunData] = {
    val runStart = event._1
    val runEnd = event._2
    val rawList = event._3
    val stepList = new ListBuffer[String]()

    for (elem <- rawList) {
      stepList.append(elem.data.map(s => (s.sensorAlias, s.sensorValue)).toMap.getOrElse("StepID","").toString)
    }
    val stepListString = stepList.toList.mkString(",")

   FdcData("rundata",RunData(runId = runStart.runId,
     toolName = runStart.toolName,
     chamberName = runStart.chamberName,
     recipe = runStart.recipeName,
     dataMissingRatio = Option(runEnd.dataMissingRatio),
     runStartTime = runStart.runStartTime,
     timeRange = Option(runEnd.timeRange),
     runEndTime = Option(runEnd.runEndTime),
     createTime = System.currentTimeMillis(),
     step = Option(stepListString),
     runDataNum = Option(rawList.size.toLong),
     traceId = runStart.traceId,
     DCType = runStart.dataType,
     completed = runStart.completed,
     materialName = runStart.materialName,
     pmStatus = runStart.pmStatus,
     pmTimestamp = runStart.pmTimestamp,
     dataVersion = runStart.dataVersion,
     lotMESInfo = runStart.lotMESInfo,
     errorCode = runStart.errorCode

   ))
  }
}
