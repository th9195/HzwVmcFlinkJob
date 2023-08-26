package com.hzw.fdc.controller.Vmc

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.Vmc.{VmcIndicatorService}

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowController
 */
class VmcIndicatorController extends TController {
  private val vmcIndicatorService = new VmcIndicatorService

  override def execute(): Unit = vmcIndicatorService.analyses()
}
