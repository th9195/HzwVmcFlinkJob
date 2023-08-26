package com.hzw.fdc.scalabean


import scala.collection.concurrent.TrieMap


case class OfflineResultScala(taskId: Long,
                              batchId: Long,
                              status: String,
                              errorMsg: String,
                              dataType: String,
                              indicatorId: Long,
                              indicatorName: String
                             )


case class OfflineRunStatus(mathStatus: Boolean, runId: String)


case class OfflineIndicatorElem(taskId: Long,
                                batchId:Long,
                                  status: String,
                                  errorMsg: String,
                                  dataType: String,
                                  value: OfflineIndicatorValue
                                 )


case class OfflineIndicatorValue(RUN_ID: String,
                                 TOOL_ID: String,
                                 CHAMBER_ID: String,
                                 INDICATOR_ID: Long,
                                 INDICATOR_NAME: String,
                                 INDICATOR_VALUE: String,
                                 RUN_START_TIME: String,
                                 RUN_END_TIME: String,
                                 WINDOW_START_TIME: String,
                                 WINDOW_END_TIME: String)

case class OfflineRunIdConfig(runId: String,
                              indicatorName: String,
                              runStartTime: Long,
                              runEndTime: Long)