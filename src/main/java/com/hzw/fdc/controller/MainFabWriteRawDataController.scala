package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.{MainFabWriteRawDataService, MainFabWriteRunDataService}


class MainFabWriteRawDataController extends TController{
  private val service = new MainFabWriteRawDataService

  override def execute(): Unit = service.analyses()
}
