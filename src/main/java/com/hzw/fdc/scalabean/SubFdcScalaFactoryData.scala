package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-08-28-15:39
 *
 */
case class SubFdcScalaFactoryData(timestamp: Long, tool: String,chamber:String, lot: String, recipe: String, datas: List[ScalaSensor])
