package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabWindowEndWindow2Service

/**
 * MainFabWindowEndWindow2Controller
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/13 15:14
 * @since 1.0.0
 * @update 2022/12/13 15:14
 * */
class MainFabWindowEndWindow2Controller extends TController {

  private val service = new MainFabWindowEndWindow2Service

  override def execute(): Unit = service.analyses()

}
