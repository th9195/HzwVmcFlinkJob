package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabVersionTagService

/**
 *   功能: 打升级版本应用程序
 */
class MainFabVersionTagController extends TController{
  private val service = new MainFabVersionTagService

  override def execute(): Unit = service.analyses()

}
