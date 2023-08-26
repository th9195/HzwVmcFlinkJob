package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-09-11-19:17
 *
 */
case class FdcRawDataScala(tool:String, chamber:String, sensor:String, data:List[OpenTSDBRawData], runId:String, recipe:String, startTime:String, stopTime:String, missingRatio:String, stage:String)
