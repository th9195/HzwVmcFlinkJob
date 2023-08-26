package com.hzw.fdc.scalabean

import scala.collection.mutable.ListBuffer

case class OfflineVirtualSensorOpentsdbResult(taskId: Long,
                                              toolName: String,
                                              chamberName: String,
                                              batchId: Long,
                                              timestamp: Long,
                                              runId: String,
                                              lastSensorStatus: Boolean,
                                              data: OfflineVirtualSensorOpentsdbResultValue,
                                              virtualConfigList: List[VirtualConfig],
                                              errorMsg: String)


case class OfflineVirtualSensorOpentsdbResultValue(svid: String,
                                                   sensorValue: Double,
                                                   sensorAlias: String,
                                                   stepName: String,
                                                   stepId: String,
                                                   unit: String)


/**
 *  虚拟sensor每计算完一个返回一个结果
 */
case class OfflineVirtualSensorElemResult(
                                           taskId: Long,
                                           dataType: String,
                                           status: String,
                                           errorMsg: String,
                                           value: ListBuffer[SensorElem])

case class SensorElem(metric: String,
                      runId: String,
                      tags: Map[String, String],
                      aggregateTags: List[String],
                      dps: Map[String, Double])



case class taskIdTimestamp(cacheKey: String, lastModified: Long)

case class windowEndTimeOutTimestamp(cacheKey: String, traceKey: String, lastModified: Long)

case class virtualSensorTimeOut(cacheKey: String, batchId:String, lastModified: Long)


case class VirtualResult(taskId: Long,
                         toolName: String,
                         chamberName: String,
                         sensorAliasName: String,
                         batchId: Long,
                         runId: String,
                         task: OfflineVirtualSensorTask,
                         dps: ListBuffer[VirtualSensorDps]
                        )

case class VirtualSensorDps(stepId: String,
               stepName: String,
               unit: String,
               timestamp: Long,
               sensorValue: Double
              )
