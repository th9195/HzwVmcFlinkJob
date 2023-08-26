package com.hzw.fdc.scalabean


case class AlarmStatistic(toolName: String,
                          chamberName: String,
                          time:Long,
                          indicatorName: String,
                          indicatorId: String,
                          level: Int,
                          trigger: Int,
                          version: Long)
