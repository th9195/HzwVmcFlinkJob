package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.online.MainFabAutoLimitService

/**
 * @author ：gdj
 * @date ：Created in 2021/6/8 15:52
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitController extends TController{
  private val service = new MainFabAutoLimitService

  override def execute(): Unit = service.analyses()
}
