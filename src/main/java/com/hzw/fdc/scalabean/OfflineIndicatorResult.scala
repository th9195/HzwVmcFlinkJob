package com.hzw.fdc.scalabean

/**
 *  离线indicator result
 */
case class OfflineIndicatorResult(isCalculationSuccessful: Boolean
                                  , calculationInfo: String
                                  , batchId: Option[Long]
                                  , taskId: Long
                                  , runId: String
                                  , toolName: String
                                  , chamberName: String
                                  , indicatorValue: String
                                  , indicatorId: Long
                                  , indicatorName: String
                                  , algoClass: String
                                  , runStartTime: Long
                                  , runEndTime: Long
                                  , windowStartTime: Long
                                  , windowEndTime: Long
                                  , windowindDataCreateTime: Long
                                  , unit: String
                                  , offlineIndicatorConfigList: List[OfflineIndicatorConfig]
                                 )

case class kafkaOfflineIndicatorResult(dataType:String,
                                datas:OfflineIndicatorResult)
