package com.hzw.fdc.function.online.MainFabAlarm

case class AlarmSwitchEventConfig(toolName: String,
                                  chamberName: String,
                                  action: String,
                                  timeStamp: Long,
                                  eventId: Int)
