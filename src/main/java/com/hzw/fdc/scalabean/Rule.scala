package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-09-13-19:58
 *
 */
case class  Rule(alarmLevel: Int,
                rule: Int,
                ruleTypeName: String,
                limit:AlarmRuleLimit,
                alarmInfo:List[AlarmRuleParam]
               )
