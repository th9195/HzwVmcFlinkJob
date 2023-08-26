package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineDriftService

class MainFabOfflineDriftController extends TController {
  private val service = new MainFabOfflineDriftService

  override def execute(): Unit = service.analyses()
}
