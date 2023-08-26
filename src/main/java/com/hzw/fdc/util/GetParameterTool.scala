package com.hzw.fdc.util

import org.apache.flink.api.java.utils.ParameterTool

/**
 * @author gdj
 * @create 2020-09-15-10:31
 *
 */
object GetParameterTool {
  private var s:ParameterTool = null
  def createParameterTool(ages:Array[String])={
    s = ParameterTool.fromArgs(ages)
  }


  def getInstance():ParameterTool = s
}
