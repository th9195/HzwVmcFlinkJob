package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabWindowEndWindowService

/**
  * @Package com.hzw.fdc.controller
  * @author wanghb
  * @date 2022-11-14 10:31
  * @desc
  * @version V1.0
  */
class MainFabWindowEndWindowController extends TController{
  private val service = new MainFabWindowEndWindowService

  override def execute(): Unit = service.analyses()
}
