package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabAlarmWriteResultDataService


class MainFabAlarmWriteResultDataController extends TController{
  private val service = new MainFabAlarmWriteResultDataService

  override def execute(): Unit = service.analyses()
}
