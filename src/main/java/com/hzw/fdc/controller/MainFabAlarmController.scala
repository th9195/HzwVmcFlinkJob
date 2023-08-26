package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabAlarmService

/**
 * @author gdj
 * @create 2020-06-15-19:52
 *
 */
class MainFabAlarmController extends TController {
  private val fdcAlarmService = new MainFabAlarmService

  override def execute(): Unit = fdcAlarmService.analyses()
}
