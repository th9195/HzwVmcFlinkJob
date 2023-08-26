package com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules

import com.hzw.fdc.scalabean.{AlarmRuleLimit, AlarmRuleType, IndicatorLimitResult, IndicatorResult, Rule}

/**
 * AlarmProcessObject
 *
 * @desc:
 * @author tobytang
 * @date 2022/6/16 13:44
 * @since 1.0.0
 * @update 2022/6/16 13:44
 * */
@SerialVersionUID(1L)
object AlarmProcessObject {


  // todo 抽象出六套规则的统一接口
  def alarmProcessRule(alarmProcessRule:AlarmProcessRuleParent,
                       indicatorResult: IndicatorResult,
                       alarmRuleType: AlarmRuleType,
                       alarmRuleLimit: AlarmRuleLimit,
                       ruleConfigKey: String,
                       w2wType: String,
                       indicatorLimitResult: IndicatorLimitResult,
                       shouldByPass: Boolean): Option[List[Option[Rule]]] = {

    // todo 各自调用各自的processRule 方法
    alarmProcessRule.processRule(indicatorResult,
      alarmRuleType,
      alarmRuleLimit,
      ruleConfigKey,
      w2wType,
      indicatorLimitResult,
      shouldByPass
    )

  }

}
