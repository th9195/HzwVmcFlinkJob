package com.hzw.fdc.scalabean

/**
 *
 * @param controlPlanId
 * @param controlPlanVersion
 * @param optionCode
 * @param window2DataList
 *
 * 注意:
 *  optionCode:
 *    0:修改window 配置;
 *      默认都为0，全删全建;
 *    1:删除controlPlan;
 *      当删除controlPlan时，需要讲该controlPlanId下的所有window配置都删除
 */
case class Window2Config(controlPlanId:Long,
                         controlPlanVersion:Long,
                         optionCode:Long, // 0:修改window 配置;  1:删除controlPlan
                         window2DataList:List[Window2Data]
                        )
