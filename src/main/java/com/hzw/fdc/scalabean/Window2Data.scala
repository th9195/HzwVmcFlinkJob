package com.hzw.fdc.scalabean

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

case class Window2Data(controlWindowId: Long,
                       parentWindowId:Long,
                       isTopWindows:Boolean,
                       isConfiguredIndicator:Boolean,
                       windowStart:String,
                       windowEnd:String,
                       splitWindowBy:String,
                       controlWindowType:String,
                       calculationTrigger:String,
                       extraSensorAliasNameList:List[String],
                       sensorAliasNameList:List[String],
                       windowSensorInfoList: List[WindowSensorInfo])

case class Window2DataPartition(controlWindowId: Long,
                                parentWindowId:Long,
                                isTopWindows:Boolean,
                                isConfiguredIndicator:Boolean,
                                windowStart:String,
                                windowEnd:String,
                                splitWindowBy:String,
                                controlWindowType:String,
                                calculationTrigger:String,
                                extraSensorAliasNameList:List[String],
                                sensorAliasNameListMap:TrieMap[String,ListBuffer[String]],
                                windowSensorInfoList: List[WindowSensorInfo])

case class WindowSensorInfo(sensorAliasId: Long,
                            sensorAliasName: String,
                            indicatorIdList: List[Long])

case class MatchWindowConfig(windowId:Long,
                             controlPlanId:Long,
                             controlPlanVersion:Long,
                             calculationTrigger:String,
                             windowAllSendorAliasNameList:List[String])  // 包含 切窗口时要使用的sensor , 以及indicator所使用的sensor

//case class MatchWindowPartitionConfig(windowId:Long,
//                                      partitionId:String,
//                                      controlPlanId:Long,
//                                      controlPlanVersion:Long,
//                                      calculationTrigger:String,
//                                      windowAllSendorAliasNameList:List[String])  // 包含 切窗口时要使用的sensor , 以及indicator所使用的sensor



case class Window2RawData(stepId: Long,
                          timestamp: Long,
                          sensorList: List[(String, Double, String)])

