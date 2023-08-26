package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2021-04-21-18:48
 *
 */
case class fdcWindowData(dataType: String, datas: windowListData)

case class windowListData(toolName: String,
                          toolId: Long,
                          chamberName: String,
                          chamberId: Long,
                          recipeName: String,
                          recipeId: Long,
                          runId: String,
                          dataMissingRatio: Double,
                          contextId: Long,
                          productName: List[String],
                          stage: List[String],
                          controlWindowId: Long,
                          controlPlanId: Long,
                          controlPlanVersion: Long,
                          missingRatio: Double,
                          windowStart: String,
                          windowStartTime: Long,
                          windowEnd: String,
                          windowEndTime: Long,
                          windowTimeRange: Long,
                          runStartTime: Long,
                          runEndTime: Long,
                          windowEndDataCreateTime: Long,
                          windowType:String,
                          DCType: String,
                          locationId: Long,
                          locationName: String,
                          moduleId: Long,
                          moduleName: String,
                          toolGroupId: Long,
                          toolGroupName: String,
                          chamberGroupId: Long,
                          chamberGroupName: String,
                          recipeGroupId: Long,
                          recipeGroupName: String,
                          limitStatus: Boolean,
                          materialName:String,
                          pmStatus:String,
                          pmTimestamp:Long,
                          area: String,
                          section: String,
                          mesChamberName: String,
                          lotMESInfo: List[Option[Lot]],
                          windowDatasList: List[WindowData],
                          dataVersion:String)

case class WindowData(windowId: Long,
                      sensorName: String,
                      sensorAlias: String,
                      unit: String,
                      indicatorId: List[Long],
                      cycleUnitCount:Int,
                      cycleIndex:Int,
                      startTime:Long,
                      stopTime:Long,
                      startTimeValue: Option[Double],
                      stopTimeValue: Option[Double],
                      sensorDataList: List[sensorDataList])

case class sensorDataList(sensorValue: Double,
                          timestamp: Long,
                          step: Long)

case class RawDataSensorData(sensorValue: Double,
                      timestamp: Long,
                      step: Long,
                      unit:String)
