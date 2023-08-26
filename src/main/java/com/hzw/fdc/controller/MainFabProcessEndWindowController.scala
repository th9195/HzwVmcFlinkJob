package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.{MainFabProcessEndWindowService}

/**
 * MainFabProcessEndWindowController
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/5 11:19
 * @since 1.0.0
 * @update 2022/11/5 11:19
 * */
class MainFabProcessEndWindowController extends TController {

  private val service = new MainFabProcessEndWindowService

  override def execute(): Unit = service.analyses()

}
