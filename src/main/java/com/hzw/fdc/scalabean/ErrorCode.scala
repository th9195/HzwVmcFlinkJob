package com.hzw.fdc.scalabean

/**
 * @author ：gdj
 * @date ：Created in 2021/6/17 13:40
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
case class ErrorCode(mainFabErrorCode:String,
                     timestamp:Long,
                     input:Map[String,Any],
                     exception:String)
