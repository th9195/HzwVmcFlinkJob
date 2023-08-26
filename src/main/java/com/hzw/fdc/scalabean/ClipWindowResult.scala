package com.hzw.fdc.scalabean

/**
 * 根据StepId划window返回的结果数据结构定义
 * msgCode定义
 *  0000: 成功;
 *  1000: 窗口切分失败;
 *  1100: 划子窗口时,父窗口切分失败;
 *  1200: 划子窗口时,父窗口切分成功,子窗口切分失败;
 * @param msgCode
 * @param msg
 * @param windowTimeRangeList
 */
case class ClipWindowResult(msgCode: String, //0000 : 成功 ;  1000 : 失败 ; 1100 ： 父窗口切分失败；1200 ： 子窗口切分失败
                            msg:String,
                            windowTimeRangeList:List[ClipWindowTimestampInfo])

/**
 *
 * @param windowId
 * @param startTime     windowStartTime
 * @param startInclude  是否包含起始时间
 * @param endTime       windowEndTime
 * @param endInclude    是否包含结束时间
 */
case class ClipWindowTimestampInfo(windowId:Long,
                                   startTime:Long,
                                   startInclude:Boolean,
                                   endTime:Long,
                                   endInclude:Boolean)