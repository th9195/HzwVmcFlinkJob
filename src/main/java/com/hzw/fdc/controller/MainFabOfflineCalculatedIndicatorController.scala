package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineCalculatedIndicatorService
import com.hzw.fdc.service.online.MainFabCalculatedIndicatorService

/**
 * @author gdj
 * @create 2020-09-07-11:46
 *
 */
class MainFabOfflineCalculatedIndicatorController extends TController {
  private val fdcService = new MainFabOfflineCalculatedIndicatorService

  override def execute(): Unit = fdcService.analyses()
}