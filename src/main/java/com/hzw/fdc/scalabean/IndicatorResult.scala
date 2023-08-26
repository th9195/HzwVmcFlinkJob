package com.hzw.fdc.scalabean


import scala.beans.BeanProperty

/**
 * @author ：gdj
 * @date ：Created in 2021/5/23 15:34
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
case class IndicatorResult(controlPlanId: Long
                           , controlPlanName: String
                           , controlPlanVersion: Long
                           , locationId: Long
                           , locationName: String
                           , moduleId: Long
                           , moduleName: String
                           , toolGroupId: Long
                           , toolGroupName: String
                           , chamberGroupId: Long
                           , chamberGroupName: String
                           , recipeGroupName: String
                           , runId: String
                           , toolName: String
                           , toolId: Long
                           , chamberName: String
                           , chamberId: Long
                           , @BeanProperty var indicatorValue: String
                           , indicatorId: Long
                           , indicatorName: String
                           , algoClass: String
                           , indicatorCreateTime: Long
                           , missingRatio: Double
                           , configMissingRatio: Double
                           , runStartTime: Long
                           , runEndTime: Long
                           , windowStartTime: Long
                           , windowEndTime: Long
                           , windowDataCreateTime: Long
                           , limitStatus: Boolean
                           , materialName: String
                           , recipeName: String
                           , recipeId: Long
                           , product: List[String]
                           , stage: List[String]
                           , bypassCondition: Option[ByPassCondition]
                           //start end none
                           , pmStatus: String
                           , pmTimestamp: Long
                           , area: String
                           , section: String
                           , mesChamberName: String
                           , lotMESInfo: List[Option[Lot]]
                           , dataVersion: String
                           , unit: String
                           , cycleIndex: String
                          )


case class kafkaIndicatorResult(dataType: String,
                                datas: IndicatorResult)
