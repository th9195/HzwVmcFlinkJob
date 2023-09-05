package com.hzw.fdc.scalabean.VmcBeans

import scala.beans.BeanProperty

/**
 *
 * @author tanghui
 * @date 2023/9/5 10:40
 * @description VmcRawData
 */
case class VmcRawData(dataType: String,
                      toolName: String,
                      chamberName: String,
                      timestamp: Long,
                      traceId: String,
                      @BeanProperty var data: List[VmcSensorData])


case class VmcSensorData(svid: String,
                        sensorName: String,
                        sensorAlias: String,
                        sensorValue: Any,
                        unit: Option[String])



