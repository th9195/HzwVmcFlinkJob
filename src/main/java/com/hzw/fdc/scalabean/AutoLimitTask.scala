package com.hzw.fdc.scalabean

/**
 * @author ：gdj
 * @date ：Created in 2021/6/16 12:33
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
case class AutoLimitTask(dataType:String,
                         taskId:String,
                         controlPlanId:Long,
                         createTime:Long,
                         runNum:Int=0)
