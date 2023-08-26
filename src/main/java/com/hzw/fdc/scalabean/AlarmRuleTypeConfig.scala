package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-09-13-15:42
 *
 */
case class AlarmRuleTypeConfig(ruleTypeName:String,
                               specDefId:String,
                               criteriaProfileId:Int,
                               specLimitUp:Double,
                               specLimitLow:Double,
                               criteriaParamUp:String,
                               criteriaParamLow:String,
                               alarmLevel:Int,
                               rule:Int,
                               alarmActionIdsUp:List[String],
                               alarmActionIdsLow:List[String],
                               alarmActionTypesUp:List[String],
                               alarmActionTypesLow:List[String])
//AlarmRuleTypeConfig(
// 5,
// decrease_continuously,
// e5243f2f72e94216803ddb290a46f90c,
// b44153dc69f2454585cf176f3a9334ca,
// Hold Tool,Hold Tool,
// 63.982,
// 47.6098,
// 3,
// ,
// ,
// ,
// 1,
// 5)

