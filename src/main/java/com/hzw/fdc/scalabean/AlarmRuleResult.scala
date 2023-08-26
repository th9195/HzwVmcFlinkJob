package com.hzw.fdc.scalabean


import scala.beans.BeanProperty

/**
 * @author gdj
 * @create 2020-09-13-17:31
 *
 */
case class AlarmRuleResult(controlPlanVersion: Int,
                           chamberName: String,
                           chamberId: Long,
                           indicatorCreateTime: Long,
                           alarmCreateTime: Long,
                           indicatorId: Long,
                           runId: String,
                           toolName: String,
                           toolId: Long,
                           limit: String,
                           ruleTrigger: String,
                           indicatorValue: String,
                           indicatorName: String,
                           algoClass: String,
                           controlPlnId: Long,
                           controlPlanName: String,
                           dataMissingRatio: Double,
                           configMissingRatio: Double,
                           runStartTime: Long,
                           runEndTime: Long,
                           windowStartTime: Long,
                           windowEndTime: Long,
                           windowDataCreateTime: Long,
                           locationId: Long,
                           locationName: String,
                           moduleId: Long,
                           moduleName: String,
                           toolGroupId: Long,
                           toolGroupName: String,
                           chamberGroupId: Long,
                           chamberGroupName: String,
                           recipeGroupName: String,
                           recipeName: String,
                           recipeId: Long,
                           productName: List[String],
                           stage: List[String],
                           materialName: String,
                           pmStatus: String,
                           pmTimestamp: Long,
                           area: String,
                           section: String,
                           mesChamberName: String,
                           lotMESInfo: List[Option[Lot]],
                           @BeanProperty var RULE: List[Rule],
                           switchStatus: String,
                           unit: String,
                           alarmLevel: Int,
                           oocLevel: Int,
                           dataVersion: String,
                           configVersion: String,
                           cycleIndex: String,
                           specId:String,
                           limitConditionName:String,
                           indicatorType:String,
                           isEwma:Boolean
                          )

case class clearLinearFit(algoClass:String,
                          indicatorId:Long
                         )

case class ToHiveData[T](`dataType`:String, datas:T)