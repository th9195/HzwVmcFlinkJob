package com.hzw.fdc.scalabean

/**
 * @author ：gdj
 * @date ：Created in 2021/5/23 16:04
 * @description：${description }
 * @modified By：
 * @version: $version$　
 */
case class FdcData[T](dataType:String,datas:T)

case class ConfigData[T](`dataType`:String,serialNo:String, status:Boolean, datas:T)
