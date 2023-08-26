package com.hzw.fdc.scalabean

/**
 * 离线wondow job输出结构
 */
case class OfflineWindowListData(batchId: Option[Long],
                                 taskId: Long,
                                 toolName: String,
                                 chamberName: String,
                                 runId: String,
                                 dataMissingRatio: Double,
                                 controlWindowId: Long,
                                 windowStart: String,
                                 windowStartTime: Long,
                                 windowEnd: String,
                                 windowEndTime: Long,
                                 windowTimeRange: Long,
                                 runStartTime: Long,
                                 runEndTime: Long,
                                 windowEndDataCreateTime: Long,
                                 windowDatasList: List[OfflineWindowData],
                                 offlineIndicatorConfigList: List[OfflineIndicatorConfig])

case class OfflineWindowData(sensorName: String,
                             sensorAlias: String,
                             unit: String,
                             indicatorId: List[Long],
                             cycleUnitCount: Int,
                             cycleIndex: Int,
                             windowStartTime:Long,
                             windowStopTime:Long,
                             isCalculationSuccessful: Boolean,
                             calculationInfo: String,
                             sensorDataList: List[sensorDataList])

case class OfflineIndicatorConfig(sensorAlias:String,
                                   indicatorId: Long,
                                  indicatorName: String,
                                  controlWindowId: Long,
                                  algoClass: String,
                                  algoParam: String,
                                  algoType: String,
                                  driftStatus: Boolean,
                                  calculatedStatus: Boolean,
                                  logisticStatus: Boolean
                                 )