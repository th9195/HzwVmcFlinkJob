package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.{MainFabOfflineVirtualSensorService, MainFabOfflineWindowService}

class MainFabOfflineVirtualSensorController extends TController{
  private val service = new MainFabOfflineVirtualSensorService

  override def execute(): Unit = service.analyses()
}
