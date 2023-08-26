package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

/**
 * @author gdj
 * @create 2021-04-19-13:45
 *
 */
case class RunEventData(dataType: String,
                        locationName: String,
                        moduleName: String,
                        toolName: String,
                        chamberName: String,
                        recipeName: String,
                        var recipeActual:Boolean = true,
                        runStartTime: Long,
                        runEndTime: Long,
                        runId: String,
                        traceId: String,
                        DCType: String,
                        dataMissingRatio: Double,
                        timeRange: Long,
                        completed: String,
                        materialName: String,
                        materialActual:Boolean = true,
                        @BeanProperty var pmStatus: String = "none",
                        @BeanProperty var pmTimestamp: Long = 0L,
                        @BeanProperty var dataVersion: String = "none",
                        lotMESInfo: List[Option[Lot]],
                        errorCode: Option[Long])

case class Lot(lotName: Option[String],
               carrier: Option[String],
               layer: Option[String],
               operation: Option[String],
               product: Option[String],
               route: Option[String],
               stage: Option[String],
               technology: Option[String],
               lotType: Option[String],
               wafers: List[Option[wafer]])

case class wafer(waferName: Option[String],
                 carrierSlot: Option[String])

case class MainFabPTRawData(dataType: String,
                            @BeanProperty var dataVersion: String = "none",
                            toolName: String,
                            chamberName: String,
                            timestamp: Long,
                            traceId: String,
                            @BeanProperty var data: List[PTSensorData])

case class PTSensorData(svid: String,
                        sensorName: String,
                        sensorAlias: String,
                        isVirtualSensor: Boolean = false,
                        sensorValue: Any,
                        unit: Option[String])

case class MainFabRawData(dataType: String,
                          dataVersion: String,
                          toolName: String,
                          chamberName: String,
                          timestamp: Long,
                          traceId: String,
                          stepId: Long,
                          stepName: String,
                          data: List[sensorData])

case class sensorData(svid: String,
                      sensorName: String,
                      sensorAlias: String,
                      isVirtualSensor: Boolean,
                      sensorValue: Double,
                      unit: String)

case class RunData(runId: String,
                   toolName: String,
                   chamberName: String,
                   recipe: String,
                   dataMissingRatio: Option[Double],
                   runStartTime: Long,
                   timeRange: Option[Long],
                   runEndTime: Option[Long],
                   createTime: Long,
                   step: Option[String],
                   runDataNum: Option[Long],
                   traceId: String,
                   DCType: String,
                   completed: String,
                   materialName: String,
                   pmStatus: String,
                   pmTimestamp: Long,
                   dataVersion: String,
                   lotMESInfo: List[Option[Lot]],
                   errorCode: Option[Long]
                  )


case class MainFabRawDataTuple(dataType: String,
                          timestamp: Long,
                          stepId: Long,
                          data: List[(String, Double, String)])   // 1: sensorAlias, 2: sensorValue, 3: unit


// add by toby
case class RunEventDataMatchWindow(dataType: String,
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
                        contextId:Long,
                        windowIdList:List[Long])
