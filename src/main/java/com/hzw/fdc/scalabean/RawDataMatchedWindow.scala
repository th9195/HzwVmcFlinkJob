package com.hzw.fdc.scalabean

case class RawDataMatchedWindow(dataType: String,
                                dataVersion: String,
                                toolName: String,
                                chamberName: String,
                                timestamp: Long,
                                traceId: String,
                                stepId: Long,
                                stepName: String,
                                data:  List[(String, Double, String)], // 1: sensorAlias, 2: sensorValue, 3: unit
                                matchedControlPlanId:Long,
                                controlPlanVersion:Long,
                                matchedWindowId:Long,
                                windowPartitionId:String)
