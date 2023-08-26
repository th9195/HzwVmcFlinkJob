package com.hzw.fdc.scalabean


/**
 *
 * @param stepId            最后一次切window时的stepId
 * @param timestamp         最后一次切window时的时间戳
 */


/**
 *
 * @param stepId            最后一次切window时的stepId 或者 第一次RawData的stepId
 * @param stepIdIsChanged   stepId是否改变过
 * @param timestamp         最后一次切window时的时间戳
 */
case class WindowEndLastCalcWindowInfo(stepId:Long,
                                       stepIdIsChanged:Boolean,
                                       timestamp:Long)
