package com.hzw.fdc.function.online.MainFabWindow
import com.hzw.fdc.scalabean.{FdcData, RunData, RunEventData}
import org.apache.flink.api.common.functions.MapFunction
/**
 * @author ：gdj
 * @date ：2021/11/27 17:12
 * @param $params
 * @return $returns
 */
class MainFabWindowEndRunDataMapFunction extends MapFunction[RunEventData,FdcData[RunData]]{
  override def map(event: RunEventData): FdcData[RunData] = {

    FdcData("rundata",RunData(runId = event.runId,
      toolName = event.toolName,
      chamberName = event.chamberName,
      recipe = event.recipeName,
      dataMissingRatio = None,
      runStartTime = event.runStartTime,
      timeRange = None,
      runEndTime = None,
      createTime = System.currentTimeMillis(),
      step = None,
      runDataNum = None,
      traceId = event.traceId,
      DCType = event.dataType,
      completed = event.completed,
      materialName = event.materialName,
      pmStatus = event.pmStatus,
      pmTimestamp = event.pmTimestamp,
      dataVersion = event.dataVersion,
      lotMESInfo = event.lotMESInfo,
      errorCode = event.errorCode

    ))

  }
}
