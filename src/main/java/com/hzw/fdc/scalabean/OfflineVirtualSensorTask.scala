package com.hzw.fdc.scalabean

/**
 * <p>离线任务</p>
 *
 * @author liuwentao
 * @date 2021/5/14 11:02
 */
case class OfflineVirtualSensorTask(taskId: Long,
                                    batchId: Long,
                                    dataType: String,
                                    runData: List[OfflineRunData],
                                    virtualConfigList: List[VirtualConfig]
                                    )

case class VirtualConfig(svid: String,
                         result: Boolean,
                         virtualSensorAliasName: String,
                         virtualSensorLevel: Long,
                         algoName: String,
                         algoClass: String,
                         needSave: Boolean,
                         param: List[OfflineVirtualSensorParam]
                        )

case class OfflineVirtualSensorParam(paramIndex: Long,
                                     paramName: String,
                                     paramDataType: String,
//                                    实际为sensorAliasName
                                     paramValue: String,
//                                     svid:String,
                                    // key -> toolName|chamberName  value -> svid
                                     svidMap:Map[String,String])

