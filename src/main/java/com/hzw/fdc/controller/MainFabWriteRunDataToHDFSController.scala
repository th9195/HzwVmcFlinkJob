package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.bi_report.MainFabWriteRunDataToHDFSService

/**
  * @Package com.hzw.fdc.controller
  * @author wanghb
  * @date 2022-09-01 15:34
  * @desc
  * @version V1.0
  */
class MainFabWriteRunDataToHDFSController extends TController{
  private val service = new MainFabWriteRunDataToHDFSService

  override def execute(): Unit = service.analyses()
}
