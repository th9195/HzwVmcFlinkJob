package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabLogisticIndicatorService

/**
 * @author ：gdj
 * @date ：Created in 2021/7/21 11:24
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabLogisticIndicatorController
  extends TController {
  private val service = new MainFabLogisticIndicatorService

  override def execute(): Unit = service.analyses()
}
