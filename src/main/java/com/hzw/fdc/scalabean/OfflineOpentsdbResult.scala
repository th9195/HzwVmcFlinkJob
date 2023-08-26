package com.hzw.fdc.scalabean

/**
 * [{
 * "metric": "RNITool_31.YMTC_chamber.RNI01SEN030",
 * "valeas": [{
 * "time": 1613706210066,
 * "value": 408.0,
 * "stepId": "1"
 * }, {
 * "time": 1613706210066,
 * "value": 408.0,
 * "stepId": "2"
 * }, {
 * "time": 1613706210066,
 * "value": 408.0,
 * "stepId": "3"
 * }],
 * "errorMsg":""
 * }]
 * 当errorMsg不为空时 查询出现错误
 */
case class OfflineOpentsdbResult(metric: String,
                                 toolName: String,
                                 chamberName: String,
                                 sensorAlias: String,
                                 runId: String,
                                 runStart: Long,
                                 runEnd: Long,
                                 dataMissingRatio: Double,
                                 taskSvidNum:Int,
                                 values: List[OfflineOpentsdbResultValue],
                                 isCalculationSuccessful: Boolean,
                                 calculationInfo: String)

case class OfflineOpentsdbResultValue(time: Long,
                                      value: Double,
                                      stepId: Long)
