package com.hzw.fdc.scalabean

import scala.annotation.meta.{getter, setter}
import scala.beans.BeanProperty

/**
 * OfflineAutoLimitControlPlanInfo
 *
 * @desc:
 * @author tobytang
 * @date 2022/10/17 19:34
 * @since 1.0.0
 * @update 2022/10/17 19:34
 * */

case class OfflineAutoLimitControlPlanInfo(controlPlanId: Long,
                                           products:List[String],
                                           stages:List[String],
                                           @BeanProperty var score:Int = -1
                                          )
