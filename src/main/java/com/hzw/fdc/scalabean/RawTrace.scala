package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

case class RawTrace(locationName:String,
                    moduleName:String,
                    toolName: String,
                    toolId: String,
                    chamberName: String,
                    chamberId:String,
                    recipeName: String,
                    recipeId: String,
                    toolGroupName: String,
                    chamberGroupName: String,
                    recipeGroupName: String,
                    dataMissingRatio: String,
                    runStartTime: Long,
                    runEndTime: Long,
                    runId: String,
                    traceId:String,
                    materialName: String,
                    lotMESInfo: List[Option[Lot]],
                    ptSensor: List[SensorNameData]
                   )


case class SensorNameData(toolName:String,
                            chamberName:String,
                            timestamp:String,
                            @BeanProperty var data:List[PTSensorData])


case class IndicatorResultFileScala(
                    controlPlanVersion: Int,
                    controlPlanId: String,
                    controlPlanName:String,
                    locationName:String,
                    moduleName:String,
                    toolName: String,
                    toolId: String,
                    chamberName: String,
                    chamberId: String,
                    recipeName: String,
                    recipeId: String,
                    toolGroupName: String,
                    chamberGroupName: String,
                    recipeGroupName: String,
                    dataMissingRatio: String,
                    runStartTime: Long,
                    runEndTime: Long,
                    runId: String,
                    materialName: String,
                    lotMESInfo: List[Option[Lot]],
                    indicatorList: List[IndicatorValue]
                   )

case class IndicatorValue(timestamp:String,
                           indicatorName:String,
                           indicatorValue: String,
                           limit:String,
                          indicatorId: String,
                          alarmLevel: String
                         )
