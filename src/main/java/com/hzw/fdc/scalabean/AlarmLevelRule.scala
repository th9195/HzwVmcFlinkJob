package com.hzw.fdc.scalabean

case class AlarmLevelRule(toolName: String,
                          chamberName: String,
                          recipeName: String,
                          controlPlnId: Long,
                          controlPlanName: String,
                          controlPlanVersion: Int,
                          runId: String,
                          dataMissingRatio: Double,
                          configMissingRatio: Double,
                          windowStartTime: Long,
                          windowEndTime: Long,
                          windowDataCreateTime: Long,
                          indicatorCreateTime: Long,
                          alarmCreateTime: Long,
                          runStartTime: Long,
                          runEndTime: Long,
                          locationId: Long,
                          locationName: String,
                          moduleId: Long,
                          moduleName: String,
                          toolGroupId: Long,
                          toolGroupName: String,
                          chamberGroupId: Long,
                          chamberGroupName: String,
                          pmStatus: String,
                          pmTimestamp: Long,
                          materialName: String,
                          area: String,
                          section: String,
                          mesChamberName: String,
                          dataVersion: String,
                          lotMESInfo: List[Option[Lot]],
                          alarm: List[indicatorAlarm],
                          micAlarm: List[micAlarm])


case class indicatorAlarm(indicatorId: Long,
                          indicatorType:String,
                          limit: String,
                          ruleTrigger: String,
                          indicatorValue: String,
                          indicatorName: String,
                          algoClass: String,
                          switchStatus: String,
                          unit: String,
                          alarmLevel: Int,
                          oocLevel: Int,
                          rule: List[Rule])

case class micAlarm(micId:Long,
                    micName: String,
                    x_of_total: Long,
                    operator:List[operatorScala],
                    action:List[Action])


/**
 * The data type stored in the state
 */
case class runAlarmTimestamp(runId: String, runEndTime: Long, lastModified: Long)

case class indicator0DownTime(runId: String, lastModified: Long)

case class indicatorTimeOutTimestamp(runId: String, runEndTime: Long, key:String, lastModified: Long)


case class IndicatorErrorConfig(
                                 toolGroupName: String,
                                 chamberGroupName:String,
                                 recipeGroupName: String,
                                 toolName: String,
                                 chamberName: String,
                                 recipeName: String,
                                 controlPlanName: String,
                                 indicatorName: String
                               )


case class IndicatorErrorReportData(
                               runEndTime: Long,
                               windowDataCreateTime: Long,
                               runId: String,
                               toolGroupName: String,
                               chamberGroupName:String,
                               recipeGroupName: String,
                               toolName: String,
                               chamberName: String,
                               recipeName: String,
                               controlPlanName: String,
                               indicatorName: String,
                               indicatorValue: String
                             )
