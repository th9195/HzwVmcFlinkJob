package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabFileService

class MainFabFileController extends TController {
  private val service = new MainFabFileService

  override def execute(): Unit = service.analyses()
}
