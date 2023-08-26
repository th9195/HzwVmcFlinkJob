package com.hzw.fdc.scalabean

import jdk.nashorn.internal.objects.annotations.Property

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-09-13-15:50
 *
 */
case class AlarmRuleConfig(controlPlanId: Int,
                           controlPlanVersion: Int,
                           toolName: Option[String],
                           chamberName: Option[String],
                           recipeName: Option[String],
                           productName: Option[String],
                           stage: Option[String],
                           indicatorId: String,
                           specDefId: String,
                           w2wType: String,
                           EwmaArgs: EwmaArgs,
                           var limit: AlarmRuleLimit,
                           rule: List[AlarmRuleType],
                           limitConditionName:String,
                           indicatorType:String,
                           isEwma:Boolean)

case class AlarmRuleLimit(@Property var USL: Option[Double],
                          @Property var UBL: Option[Double],
                          @Property var UCL: Option[Double],
                          @Property var LCL: Option[Double],
                          @Property var LBL: Option[Double],
                          @Property var LSL: Option[Double])

case class AlarmRuleType(ruleType: Int,
                         ruleTypeName: String,
                         @Property var USLorRule45: List[AlarmRuleParam],
                         @Property var UBL: List[AlarmRuleParam],
                         @Property var UCL: List[AlarmRuleParam],
                         @Property var LCL: List[AlarmRuleParam],
                         @Property var LBL: List[AlarmRuleParam],
                         @Property var LSL: List[AlarmRuleParam])

case class AlarmRuleParam(index: Int,
                          X: Option[Long],
                          N: Option[Long],
                          action: List[AlarmRuleAction])

case class AlarmRuleAction(id: Long,
                           `type`: Option[String],
                           param: Option[String],
                           sign: Option[String])


case class IndicatorLimitResult(runId: String,
                                toolName: String,
                                chamberName: String,
                                indicatorValue: String,
                                indicatorId: String,
                                indicatorName: String,
                                alarmLevel: Int,
                                oocLevel: Int,
                                createTime: String,
                                limit: String,
                                switchStatus: String,
                                unit: String)

case class CountData(@BeanProperty var count: Int,
                     @BeanProperty var value: Double)

case class UpLowData(@BeanProperty var leave1up: Int,
                     @BeanProperty var leave1low: Int,
                     @BeanProperty var leave2up: Int,
                     @BeanProperty var leave2low: Int,
                     @BeanProperty var leave3up: Int,
                     @BeanProperty var leave3low: Int,
                     @BeanProperty var ruleActionIndexUp1: Long,
                     @BeanProperty var ruleActionIndexLow1: Long,
                     @BeanProperty var ruleActionIndexUp2: Long,
                     @BeanProperty var ruleActionIndexLow2: Long,
                     @BeanProperty var ruleActionIndexUp3: Long,
                     @BeanProperty var ruleActionIndexLow3: Long)

case class Count_1_2_6(@BeanProperty var USL_1: Int,
                      @BeanProperty var UBL_1: Int,
                      @BeanProperty var UCL_1: Int,
                      @BeanProperty var LCL_1: Int,
                      @BeanProperty var LBL_1: Int,
                      @BeanProperty var LSL_1: Int,
                      @BeanProperty var USL_2: Int,
                      @BeanProperty var UBL_2: Int,
                      @BeanProperty var UCL_2: Int,
                      @BeanProperty var LCL_2: Int,
                      @BeanProperty var LBL_2: Int,
                      @BeanProperty var LSL_2: Int,
                      @BeanProperty var USL_3: Int,
                      @BeanProperty var UBL_3: Int,
                      @BeanProperty var UCL_3: Int,
                      @BeanProperty var LCL_3: Int,
                      @BeanProperty var LBL_3: Int,
                      @BeanProperty var LSL_3: Int)

case class Count_1_2_6_merge_by_level(
                                     @BeanProperty var CL_1: Int,
                                     @BeanProperty var BL_1: Int,
                                     @BeanProperty var SL_1: Int,
                                     @BeanProperty var CL_2: Int,
                                     @BeanProperty var BL_2: Int,
                                     @BeanProperty var SL_2: Int,
                                     @BeanProperty var CL_3: Int,
                                     @BeanProperty var BL_3: Int,
                                     @BeanProperty var SL_3: Int)


case class Count_3_merge_by_level(
                                   @BeanProperty var CL_1: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var BL_1: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var SL_1: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var CL_2: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var BL_2: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var SL_2: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var CL_3: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var BL_3: ListBuffer[Boolean]=new ListBuffer[Boolean](),
                                   @BeanProperty var SL_3: ListBuffer[Boolean]=new ListBuffer[Boolean]())



case class Rule45ActionIndex(@BeanProperty var ruleActionIndex1: Long,
                             @BeanProperty var ruleActionIndex2: Long,
                             @BeanProperty var ruleActionIndex3: Long)


case class AlarmRuleConfigAck(controlPlanId: Long,
                              controlPlanVersion: Long,
                              signalType: String)








