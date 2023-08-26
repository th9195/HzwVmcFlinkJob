package com.hzw.fdc.scalabean

/**
 * <p>离线任务</p>
 *
 * @author liuwentao
 * @date 2021/5/14 11:02
 */
case class OfflineTask(dataType: String,
                       batchId: Long,
                       taskId: Long,
                       runData: List[OfflineRunData],
                       indicatorTree: IndicatorTree
                      )

case class OfflineRunData(runId: String,
                          runStart: Long,
                          runEnd: Long,
                          dataMissingRatio: Double)

case class OfflineMainFabRawData(dataType: String,
                                 toolName: String,
                                 chamberName: String,
                                 timestamp: Long,
                                 runId: String,
                                 runStart: Long,
                                 runEnd: Long,
                                 dataMissingRatio: Double,
                                 stepId: Long,
                                 stepName: String,
                                 data: List[OfflineSensorData])

case class OfflineSensorData(sensorName: String,
                             sensorAlias: String,
                             sensorValue: Double,
                             unit: String)

case class OfflineTaskRunData(toolName: String,
                              chamberName: String,
                              dataMissingRatio: Double,
                              runId: String,
                              runStart: Long,
                              runEnd: Long,
                              isCalculationSuccessful: Boolean,
                              calculationInfo: String)




