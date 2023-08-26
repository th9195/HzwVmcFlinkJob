package com.hzw.fdc.scalabean

/**
 *
 * @param dataType
 * @param debugCode
 * @param controlPlanIdList
 * @param windowIdList
 */
case class DebugWindowJobConfig(dataType : String,
                                controlPlanList:List[DebugWindowJobInfo]
                                )

case class DebugWindowJobInfo(controlPlanId:Long,
                              windowIdList:List[Long])


case class DebugMatchControlPlan(dataType : String ,
                                 status:Boolean = false,
                                 toolName:String,
                                 chamberName:String,
                                 recipeName:String)


case class DebugCalcWindow(dataType : String ,
                           status:Boolean = false,
                           windowId:Long)


