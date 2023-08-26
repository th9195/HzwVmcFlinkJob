package com.hzw.fdc.scalabean

/**
 *  离线计算indicator任务配置
 */
case class OfflineIndicatorTask(dataType: String,
                                batchId: Option[Long],
                                taskId: Long,
                                runData: List[OfflineRun],
                                indicatorTree: IndicatorTree)

case class OfflineRun(runId: String,
                          runStart: Long,
                          runEnd: Long)

case class IndicatorTree(current: TreeConfig,
                         nexts: List[IndicatorTree])


case class TreeConfig(windowTree:Option[WindowTree],
                      indicatorConfig: OfflineIndicatorConfig)


case class WindowTree(current: WindowConfigData,
                      next:Option[WindowTree])
