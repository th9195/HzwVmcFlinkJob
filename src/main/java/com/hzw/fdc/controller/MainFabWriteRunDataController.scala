package com.hzw.fdc.controller


import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabWriteRunDataService


class MainFabWriteRunDataController extends TController{
  private val service = new MainFabWriteRunDataService

  override def execute(): Unit = service.analyses()
}
