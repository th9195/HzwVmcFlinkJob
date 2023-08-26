package com.hzw.fdc.scalabean

import com.hzw.fdc.function.PublicFunction.limitCondition.ConditionEntity

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

/**
 * OfflineAutoLimitConfig
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/15 11:00
 * @since 1.0.0
 * @update 2022/8/15 11:00
 * */
case class OfflineAutoLimitSettings(dataType:String,
                                  status:Boolean,
                                  datas: OfflineAutoLimitConfig)


case class OfflineAutoLimitConfig(
                           controlPlanId: Long,
                           version: Long,
                           toolList:List[OfflineAutoLimitToolInfo],
                           recipeList:List[String],
                           controlPlanList:List[OfflineAutoLimitControlPlanInfo],
                           endTimestamp:Long,
                           maxTimeRange:Long,
                           removeOutlier: String,
                           maxRunNumber: String,
                           every: String,
                           triggerMethodName: String,
                           triggerMethodValue: String,
                           active: String,
                           deploymentInfo: List[OfflineDeploymentInfo])

case class OfflineAutoLimitToolInfo(toolName:String,
                                    chamberNameList:List[String])


case class OfflineDeploymentInfo(indicatorId: Long,
                                 specId: Long,
                                 isCalcLimit:Boolean,
                                 condition: OfflineCondition,
                                 limitMethod: String,
                                 limitValue: String,
                                 cpk: String,
                                 target: String)

case class OfflineCondition(tool: List[String],
                     chamber: List[String],
                     recipe: List[String],
                     product: List[String],
                     stage: List[String])

case class OfflineAutoLimitIndicatorConfig(
                                            controlPlanId: Long,
                                            version: Long,
                                            toolList:List[OfflineAutoLimitToolInfo],
                                            recipeList:List[String],
                                            @BeanProperty var indicatorId:String = "",
                                            startTimestamp:Long,
                                            runRangeTime:Long,
                                            endTimestamp:Long,
                                            maxTimeRange:Long,
                                            removeOutlier: String,
                                            maxRunNumber: String,
                                            every: String,
                                            triggerMethodName: String,
                                            triggerMethodValue: String,
                                            active: String,
                                            @BeanProperty var deploymentInfo: ListBuffer[OfflineDeploymentInfo])


case class QueryRunDataConfig(controlPlanId:Long,
                              version:Long,
                              toolList:List[OfflineAutoLimitToolInfo],
                              recipeList:List[String],
                              controlPlanList:List[OfflineAutoLimitControlPlanInfo],
                              endTimestamp:Long,
                              maxRunNumber: String,
                              maxTimeRange:Long)

//case class QueryIndicatorsDataConfig(controlPlanId:Long,
//                                     version:Long,
//                                     toolList:List[OfflineAutoLimitToolInfo],
//                                     indicatorIdList:List[Long],
//                                     startTimestamp:Long,
//                                     endTimestamp:Long,
//                                     every:Int)

case class QueryIndicatorIdDataConfig(controlPlanId:Long,
                                    version:Long,
                                    toolList:List[OfflineAutoLimitToolInfo],
                                    indicatorId:String,
                                    startTimestamp:Long,
                                    endTimestamp:Long,
                                    every:Int)


case class OfflineAutoLimitOneConfig(controlPlanId: Long,
                                    version: Long,
                                    toolList:List[OfflineAutoLimitToolInfo],
                                    recipeList:List[String],
                                    endTimestamp:Long,
                                    maxTimeRange:Long,
                                    removeOutlier: String,
                                    maxRunNumber: String,
                                    every: String,
                                    triggerMethodName: String,
                                    triggerMethodValue: String,
                                    active: String,
                                    indicatorId: Long,
                                    specId: Long,
                                    isCalcLimit:Boolean,
                                    condition: OfflineCondition,
                                    limitMethod: String,
                                    limitValue: String,
                                    cpk: String,
                                    target: String)
