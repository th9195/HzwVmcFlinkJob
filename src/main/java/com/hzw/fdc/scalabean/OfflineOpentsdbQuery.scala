package com.hzw.fdc.scalabean


/**
 *
 * @param task 对应的离线任务
 * @param queryStr 查询语句
 */
case class OfflineOpentsdbQuery(task: OfflineTask,
                                toolName: String,
                                chamberName: String,
                                sensorAlias:String,
                                sensorList: List[WindowConfigAlias],
                                svid:String,
                                queryStr: String)

case class OfflineVirtualSensorOpentsdbQuery(task: OfflineVirtualSensorTask,
                                             queryStr: String)
