package com.hzw.fdc.scalabean

/**
 * OfflineAutoLimitResult
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/15 17:49
 * @since 1.0.0
 * @update 2022/8/15 17:49
 * */
case class OfflineAutoLimitResult(dataType: String,
                                  specId: Long,
                                  taskId: String,
                                  calcTimestamp: Long,
                                  specLimit: SpecLimit,
                                  settingInfo: OfflineAutoLimitOneConfig,
                                  runNum:Long)
