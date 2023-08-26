package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineResultService

class MainFabOfflineResultController extends TController{
  private val service = new MainFabOfflineResultService

  override def execute(): Unit = service.analyses()
}
