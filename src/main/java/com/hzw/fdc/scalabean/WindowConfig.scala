package com.hzw.fdc.scalabean

/**
 * <p>描述</p>
 *
 * @author liuwentao
 * @date 2021/4/23 15:07
 */
case class WindowConfig(dataType: String, status: Boolean, datas: List[WindowConfigData])

case class WindowConfigData(contextId: Long,
                            controlWindowId: Long,
                            parentWindowId: Long,
                            controlWindowType: String,
                            isTopWindows: Boolean,
                            isConfiguredIndicator: Boolean,
                            windowStart: String,
                            windowEnd: String,
                            controlPlanId: Long,
                            controlPlanVersion: Long,
                            calcTrigger: String,
                            sensorAlias: List[WindowConfigAlias])

case class WindowConfigAlias(sensorAliasId: Long,
                             sensorAliasName: String,
                             svid:String,
                             indicatorId: List[Long],
                             chamberName: String,
                             toolName: String)


case class ElemWindowConfigAlias(controlWindowId: Long,
                                 sensorAliasId: Long,
                                 sensorAliasName: String,
                                 svid:String,
                                 indicatorId: List[Long],
                                 chamberName: String,
                                 toolName: String)