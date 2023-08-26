package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

/**
 * AutoLimitRunData
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/19 13:30
 * @since 1.0.0
 * @update 2022/8/19 13:30
 * */
case class OfflineAutoLimitRunData(@BeanProperty var rowkey: String = "",
                                   @BeanProperty var run_start_time: Long = 0l,
                                   @BeanProperty var run_end_time: Long = 0l,
                                   @BeanProperty var recipe: String = "",
                                   @BeanProperty var products: String = "",
                                   @BeanProperty var stages: String = "")


case class OfflineAutoLimitRunDataTimeRange(@BeanProperty var startTimestamp:Long,
                                            @BeanProperty var endTimestamp:Long)

