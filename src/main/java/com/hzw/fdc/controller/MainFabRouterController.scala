package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabRouterService

/**
 * @author ：gdj
 * @date ：Created in 2021/7/21 11:24
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabRouterController extends TController {
  private val service = new MainFabRouterService

  override def execute(): Unit = service.analyses()
}
