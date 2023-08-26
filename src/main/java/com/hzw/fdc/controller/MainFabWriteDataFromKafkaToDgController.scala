package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.bi_report.{MainFabWriteDataFromKafkaToDgService, MainFabWriteRunDataToHDFSService}

/**
  * @Package com.hzw.fdc.controller
  * @author wanghb
  * @date 2023-04-29 3:18
  * @desc
  * @version V1.0
  */
class MainFabWriteDataFromKafkaToDgController extends TController{
  private val service = new MainFabWriteDataFromKafkaToDgService

  override def execute(): Unit = service.analyses()
}
