package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineLogisticIndicatorService

/**
 * @author ：gdj
 * @date ：Created in 2021/7/21 11:24
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineLogisticIndicatorController
  extends TController {
  private val service = new MainFabOfflineLogisticIndicatorService

  override def execute(): Unit = service.analyses()
}
