package com.hzw.fdc.controller.Vmc

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.Vmc.VmcWindowService

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowController
 */
class VmcWindowController extends TController {
  private val vmcWindowService = new VmcWindowService

  override def execute(): Unit = vmcWindowService.analyses()
}
