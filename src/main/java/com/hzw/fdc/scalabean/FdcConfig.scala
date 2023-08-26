package com.hzw.fdc.scalabean
/**
 * @author ：gdj
 * @date ：Created in 2021/5/23 14:24
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
//case class ConfigData[T](dataType: String, status: Boolean, datas: T)
case class IndicatorConfig(contextId: Long,
                           controlPlanId: Long,
                           controlPlanName: String,
                           controlPlanVersion: Long,
                           indicatorId: Long,
                           indicatorName: String,
                           sensorAlias: String,
                           missingRatio: Double,
                           controlWindowId: Long,
                           algoClass: String,
                           algoParam: String,
                           algoType: String,
                           w2wType: String,
                           driftStatus: Boolean,
                           calculatedStatus: Boolean,
                           logisticStatus: Boolean,
                           bypassCondition: Option[ByPassCondition])


case class ByPassCondition(actionSwitch: Boolean,
                           notificationSwitch: Boolean,
                           details: List[Detail])

  case class Detail(condition: String,
                    recentRuns: String)

case class ByPassConditionPO(id: Long,
                             byPassName: String,
                             notificationSwitch: Boolean,
                             actionSwitch: Boolean,
                             SingleOcapId: Long,
                             alarmPassConditionId: Long,
                             code: String,
                             name: String,
                             hasParam: Boolean,
                             singleOcapByPassId: Long,
                             paramIndex: Int,
                             paramValue: String)

//0,
// 213,
// T13-14%CG13-14,
// 1,
// 677,
// All Time%CMPSEN002%Average,
// null,
// 3.33,0,
// null,
// null,
// null,
// false,
// false,
// false

//"indicatorName": "",
//		"sensorAlias": "",
//		"missingRatio": "",
//		"controlWindowId": 1800000,
//		"algoClass": "",
//		"algoParam": "1|2|3|4|5",
//		"algoType": "2",//indicator的类型 分为3种,1是基础的indicator，2是drift算法，3是calculated的算法
//		"driftStatus": true, //该indicator计算完的结果是否参加drift计算
//		"calculatedStatus": true //该indicator计算完的结果是否参加calculated计算