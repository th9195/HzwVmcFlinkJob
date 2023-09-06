package com.hzw.fdc.scalabean.VmcBeans

/**
 *
 * @author tanghui
 * @date 2023/9/6 9:54
 * @description VmcWindowData
 */
case class VmcWindowData()

case class VmcWindowRawData(stepId: Long,
                            timestamp: Long,
                            vmcWindowSensorDataList:List[VmcWindowSensorData])

case class VmcWindowSensorData(sensorAlias: String,
                               sensorValue: Any,
                               unit: Option[String])