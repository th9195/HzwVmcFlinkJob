package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineAutoLimitService


/**
 * MainFabOfflineAutoLimitController
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/10 13:53
 * @since 1.0.0
 * @update 2022/8/10 13:53
 * */
class MainFabOfflineAutoLimitController extends TController{
  private val service = new MainFabOfflineAutoLimitService

  override def execute(): Unit = service.analyses()
}
