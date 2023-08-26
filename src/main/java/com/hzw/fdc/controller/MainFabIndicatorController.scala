package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabIndicatorService

/**
 * @author gdj
 * @create 2020-06-28-18:25
 *
 */
class MainFabIndicatorController extends TController {
  private val service = new MainFabIndicatorService

  override def execute(): Unit = service.analyses()
}
