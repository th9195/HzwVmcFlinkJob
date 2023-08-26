package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabWindowService

/**
 * @author gdj
 * @create 2020-05-31-20:03
 *
 */
class MainFabWindowController extends TController {
  private val service = new MainFabWindowService

  override def execute(): Unit = service.analyses()
}
