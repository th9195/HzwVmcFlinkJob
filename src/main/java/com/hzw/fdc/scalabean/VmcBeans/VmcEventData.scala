package com.hzw.fdc.scalabean.VmcBeans


/**
 *
 * @author tanghui
 * @date 2023/9/5 10:34
 * @description VmcEventStart
 */
case class VmcEventData(dataType: String,
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
                        lotMESInfo: List[Option[VmcLot]],
                        errorCode: Option[Long])


case class VmcEventDataMatchControlPlan(dataType: String,
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
                                        lotMESInfo: List[Option[VmcLot]],
                                        errorCode: Option[Long],
                                        vmcControlPlanConfig:VmcControlPlanConfig)

case class VmcLot(lotName: Option[String],
                 carrier: Option[String],
                 layer: Option[String],
                 operation: Option[String],
                 product: Option[String],
                 route: Option[String],
                 stage: Option[String],
                 technology: Option[String],
                 lotType: Option[String],
                 wafers: List[Option[VmcWafer]])

case class VmcWafer(waferName: Option[String],
                    carrierSlot: Option[String])




