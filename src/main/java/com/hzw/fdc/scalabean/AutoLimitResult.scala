package com.hzw.fdc.scalabean

import scala.math.BigDecimal

/**
 * @author lwt
 * @date 2021/6/10 20:46
 */
case class AutoLimitResult(dataType: String,
                           specId: Long,
                           taskId: String,
                           timestamp: Long,
                           specLimit: SpecLimit,
                           settingInfo: AutoLimitOneConfig,
                           runNum:Long)


case class SpecLimit(usl: String,
                     lsl: String,
                     ubl: String,
                     lbl: String,
                     ucl: String,
                     lcl: String)


case class AutoLimitResult1(dataType: String,
                           specId: Long,
                           taskId: String,
                           timestamp: Long,
                           specLimit: SpecLimit1,
                           settingInfo: AutoLimitOneConfig)

case class SpecLimit1(usl: Double,
                     lsl: Double,
                     ucl: Double,
                     lcl: Double)
