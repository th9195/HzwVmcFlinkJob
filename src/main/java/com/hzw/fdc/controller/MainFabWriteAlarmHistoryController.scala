package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.{MainFabAlarmService, MainFabWriteAlarmHistoryService}

/**
 * @author gdj
 * @create 2020-06-15-19:52
 *
 */
class MainFabWriteAlarmHistoryController extends TController {
  private val fdcAlarmService = new MainFabWriteAlarmHistoryService

  override def execute(): Unit = fdcAlarmService.analyses()
}
