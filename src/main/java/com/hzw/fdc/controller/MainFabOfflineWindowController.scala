package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineWindowService

class MainFabOfflineWindowController extends TController{
  private val service = new MainFabOfflineWindowService

  override def execute(): Unit = service.analyses()
}
