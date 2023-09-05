package com.hzw.fdc.scalabean.VmcBeans

/**
 *
 * @author tanghui
 * @date 2023/9/5 10:04
 * @description VmcConfigData
 */
case class VmcConfig[T]()(`dataType`:String,
                          serialNo:String,
                          timestamp:Long,
                          status:String,
                          data:T)

case class VmcControlPlanConfig(controlPlanId:String,
                               toolNameList:List[String],
                               recipeNameList:List[String],
                               stageName:String,
                               route:String,
                               vmcSensorInfoList:List[VmcSensorInfo],
                               calcTypeList:List[String],
                               windowType:String)

case class VmcSensorInfo(vmcSensorMesName:String,
                         vmcSensorFdcName:String)

