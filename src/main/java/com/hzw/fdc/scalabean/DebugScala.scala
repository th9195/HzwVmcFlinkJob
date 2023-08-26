package com.hzw.fdc.scalabean

case class DebugScala(dataType: String,              // debugTest
                      function: String,              // setDebugStateForWindow 或 getWindowSnapshotInfo等
                      status: Boolean,               // ture 代表开 ， false 代表关
                      args: Map[String, String]      // controlWindowId --> 1
                     )

case class ControlPlanWindowConfigDebug(dataType:String,                // controlPlanWindowConfigDebug
                                        controlPlanInfoList:List[Long], // 打印controlPlan的基本信息 tool chamber recipe productList stageList
                                        windowInfoMap:Map[Long,Long]    // controlPlanId --> windowId  : 打印指定window的信息;
                                       )

