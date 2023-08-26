package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabDataTransformWindowService

/**
 * @author gdj
 * @create 2020-05-31-20:03
 *
 */
class MainFabDataTransformWindowController extends TController {
  private val service = new MainFabDataTransformWindowService

  override def execute(): Unit = service.analyses()
}
