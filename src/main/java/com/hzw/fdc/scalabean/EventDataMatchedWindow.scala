package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

case class EventDataMatchedWindow(dataType: String,
                                  locationName: String,
                                  moduleName: String,
                                  toolName: String,
                                  chamberName: String,
                                  recipeName: String,
                                  runStartTime: Long,
                                  runEndTime: Long,
                                  runId: String,
                                  traceId: String,
                                  DCType: String,
                                  dataMissingRatio: Double,
                                  timeRange: Long,
                                  completed: String,
                                  materialName: String,
                                  @BeanProperty var pmStatus: String = "none",
                                  @BeanProperty var pmTimestamp: Long = 0L,
                                  @BeanProperty var dataVersion: String = "none",
                                  lotMESInfo: List[Option[Lot]],
                                  errorCode: Option[Long],
                                  matchedControlPlanId:Long,
                                  controlPlanVersion:Long,
                                  matchedWindowId:Long,
                                  windowPartitionId:String,
                                  matchedControlPlanConfig: ControlPlanConfig)

