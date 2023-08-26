package com.hzw.fdc.scalabean

case class OfflineConfigScala(taskId: Long,
                              subTaskStatus: Boolean,
                              runData: List[runScala],
                              sensorAlias: String,
                              indicatorId: Long,
                              controlWindowId: Long,
                              algoClass: String,
                              algoParam: String,
                              algoType: String,
                              dataType: String
                             )

case class runScala(runId: String, runStart: Long, runEnd: Long, dataMissingRatio: Double)

