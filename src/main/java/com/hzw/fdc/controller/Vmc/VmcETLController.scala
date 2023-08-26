package com.hzw.fdc.controller.Vmc

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.Vmc.{VmcETLService}

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowController
 */
class VmcETLController extends TController {
  private val vmcETLService = new VmcETLService

  override def execute(): Unit = vmcETLService.analyses()
}
