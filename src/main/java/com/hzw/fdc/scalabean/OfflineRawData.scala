package com.hzw.fdc.scalabean

case class OfflineRawData(batchId: Option[Long],
                          taskId: Long,
                          toolName: String,
                          chamberName: String,
                          cycleUnitCount: Long,
                          unit: String,
                          data: List[sensorDataList],
                          runId: String,
                          runStartTime: Long,
                          runEndTime: Long,
                          windowStartTime: Long,
                          windowEndTime: Long,
                          windowEndDataCreateTime: Long,
                          isCalculationSuccessful: Boolean,
                          calculationInfo: String
                           )
