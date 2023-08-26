package com.hzw.fdc.scalabean


import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

/**
 * ContextInfo
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/8 10:58
 * @since 1.0.0
 * @update 2022/11/8 10:58
 * */

case class ContextProductStageInfo(contextId : Long,
                                   @BeanProperty var productNameList: ListBuffer[String],
                                   @BeanProperty var stageNameList: ListBuffer[String],
                                   @BeanProperty var score :Int = -1)
