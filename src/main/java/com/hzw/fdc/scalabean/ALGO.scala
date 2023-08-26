package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-06-11-15:40
 *
 */
object ALGO extends Enumeration {
  type ALGO = Value

  val AVG = Value(1, "avg")
  val MAX = Value(2, "max")
  val MIN = Value(3, "min")
  val FIRSTPOINT = Value(4, "firstPoint")
  val HITTIME = Value(5, "hitTime")
  val HITCOUNT = Value(6, "hitCount")
  val RANGE = Value(7, "range")
  val SLOPE = Value(8, "slope")
  val STD = Value(9, "std")
  val SUM = Value(10, "sum")
  val TIMERANGE = Value(11, "timeRange")
  val DRIFT = Value(12, "drift")
  val dataMissingRatio = Value(13, "dataMissingRatio")
  val MeanT = Value(21, "meanT")
  val StdDevT = Value(22, "stdDevT")
  val AREA = Value(25,"area")
  val CycleCount = Value(26,"cycleCount")
  val UNKNOWN = Value(100, "UNKNOWN")


}
