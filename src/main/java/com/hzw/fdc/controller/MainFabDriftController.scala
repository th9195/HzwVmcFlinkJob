package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabDriftService

/**
 * @author gdj
 * @create 2020-08-29-14:18
 *
 */
class MainFabDriftController extends TController {
  private val service = new MainFabDriftService

  override def execute(): Unit = service.analyses()
}
