package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.{MainFabAlarmService, MainFabWriteRedisDataService}

/**
 * @author gdj
 * @create 2020-06-15-19:52
 *
 */
class MainFabWriteRedisDataController extends TController {
  private val writeRedisDataService = new MainFabWriteRedisDataService

  override def execute(): Unit = writeRedisDataService.analyses()
}
