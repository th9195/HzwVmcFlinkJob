package com.hzw.fdc.scalabean

case class MicAlarmData(`dataType`:String, datas:AlarmMicRule)
case class AlarmMicRule(controlPlanId:Long,
                        controlPlanName: String,
                        controlPlanVersion:Long,
                        locationId: Long,
                        locationName: String,
                        moduleId: Long,
                        moduleName: String,
                        action:List[Action],
                        micId:Long,
                        micName: String,
                        operator:List[operatorScala],
                        runId:String,
                        toolName:String,
                        chamberName:String,
                        recipeName: String,
                        toolGroupName: String,
                        toolGroupId: Long,
                        chamberGroupName: String,
                        chamberGroupId: Long,
                        configMissingRatio: Double,
                        dataMissingRatio: Double,
                        materialName: String,
                        runStartTime:Long,
                        runEndTime:Long,
                        windowStartTime: Long,
                        windowEndTime: Long,
                        windowDataCreateTime: Long,
                        x_of_total: Long,
                        alarmCreateTime: Long,
                        area: String,
                        section: String,
                        mesChamberName: String,
                        lotMESInfo: List[Option[Lot]],
                        dataVersion: String
                       )

case class operatorScala(indicatorId: Long, indicatorType:String,indicatorName:String,  oocLevel: List[Long], triggerLevel: List[Long], limit:String, indicatorValue:String)



case class CacheMicDataScala(indicatorId: Long,
                             indicatorType:String,
                             indicatorName:String,
                             triggerLevel: List[Long],
                             limit:String,
                             indicatorValue:String,
                             recipeName: String,
                             toolGroupName: String,
                             toolGroupId: Long,
                             configMissingRatio: Double,
                             dataMissingRatio: Double,
                             materialName: String
                            )