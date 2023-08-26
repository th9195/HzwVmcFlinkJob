package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

/**
 * AutoLimitIndicatorResult
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/11 15:10
 * @since 1.0.0
 * @update 2022/8/11 15:10
 * */
case class AutoLimitIndicatorResult(
                                     @BeanProperty var indicatorId: Long = 0l,
                                     @BeanProperty var indicatorValue: String = "",
                                     @BeanProperty var toolName: String = "",
                                     @BeanProperty var chamberName: String = "",
                                     @BeanProperty var recipeName:String = "",
                                     @BeanProperty var product: List[String] = null,
                                     @BeanProperty var stage: List[String] = null
                                   )
