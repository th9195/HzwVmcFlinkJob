package com.hzw.fdc.controller.Vmc

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.Vmc.VmcAllService

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowController
 */
class VmcAllController extends TController {
  private val vmcAllService = new VmcAllService

  override def execute(): Unit = vmcAllService.analyses()
}
