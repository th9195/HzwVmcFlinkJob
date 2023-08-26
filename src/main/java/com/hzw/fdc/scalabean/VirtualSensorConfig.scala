package com.hzw.fdc.scalabean

/**
 * @author lwt
 * @date 2021/7/6 18:34
 */
case class VirtualSensorConfig(serialNo: String,
                               dataType: String,
                               status: Boolean,
                               datas: VirtualSensorConfigData)

case class VirtualSensorConfigData(controlPlanId: Long,
                                   controlPlanVersion: Long,
                                   svid: String,
                                   virtualSensorAliasName: String,
                                   virtualLevel: Int,
                                   algoName: String,
                                   algoClass: String,
                                   toolMsgList: List[VirtualSensorToolMsg],
                                   param: List[VirtualSensorAlgoParam])

case class VirtualSensorToolMsg(toolName: String,
                                chamberName: String)

case class VirtualSensorAlgoParam(paramIndex: Int,
                                  paramName: String,
                                  paramDataType: String,
                                  paramValue: String,
                                  svid: String)

case class VirtualSensorViewPO(virtualSensorId: Long,
                               controlPlanId: Long,
                               controlPlanVersion: Long,
                               toolName: String,
                               chamberName: String,
                               svid: String,
                               virtualSensorAliasName: String,
                               algoName: String,
                               algoClass: String)

case class SensorAliasPO(sensorAliasId: String,
                         sensorAliasName: String,
                         svid: String)

case class VirtualSensorParamPO(virtualSensorId: Long,
                                paramIndex: Int,
                                paramName: String,
                                paramDataType: String,
                                paramValue: String)

case class ParamConfig(virtualSvid: String, virtualSensorAliasName: String, virtualLevel: Long, algoName: String, algoClass: String, param: List[VirtualSensorAlgoParam])


case class Point(timestamp: Long, value: Double)