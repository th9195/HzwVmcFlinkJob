package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabProcessEndWindow2Service

/**
 * MainFabProcessEndWindow2Controller
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/13 14:31
 * @since 1.0.0
 * @update 2022/12/13 14:31
 * */
class MainFabProcessEndWindow2Controller extends TController {

  private val service = new MainFabProcessEndWindow2Service

  override def execute(): Unit = service.analyses()

}
