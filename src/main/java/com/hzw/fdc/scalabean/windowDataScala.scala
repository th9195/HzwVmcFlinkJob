package com.hzw.fdc.scalabean

/**
 *  window 传输过来的列表
 */
case  class windowDataScala(sensorName:String,
                            sensorAlias:String,
                            unit:String,
                            indicatorId: List[Long],
                            cycleIndex: Long,
                            windowStartTime:Long,
                            windowEndTime:Long,
                            sensorDataList: List[RawDataTimeValue])
