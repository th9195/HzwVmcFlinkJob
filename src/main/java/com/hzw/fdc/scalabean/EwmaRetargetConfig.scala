package com.hzw.fdc.scalabean



case class EwmaRetargetConfig(retargetCreateTime:Long,
                              moduleName:String,
                              fdcToolGroupName:String,
                              fdcChamberGroupName:String,
                              controlPlanName:String,
                              controlPlanId:Long,
                              indicatorName:String,
                              indicatorId:Long,
                              specId:Long,
                              limitConditionName:String,
                              fdcToolName:String,
                              fdcChamberName:String,
                              mesToolName:String,
                              mesChamberName:String,
                              pmName:String,
                              retargetType:String,
                              rangeU:Double,
                              rangeL:Double,
                              retargetUser:String)

case class EwmaRetargetResult(retargetCreateTime:Long,
                              retargetTime:Long,
                              moduleName:String,
                              fdcToolGroupName:String,
                              fdcChamberGroupName:String,
                              controlPlanName:String,
                              controlPlanId:Long,
                              indicatorName:String,
                              indicatorId:Long,
                              specId:Long,
                              limitConditionName:String,
                              fdcToolName:String,
                              fdcChamberName:String,
                              mesToolName:String,
                              mesChamberName:String,
                              pmName:String,
                              retargetType:String,
                              rangeU:Double,
                              rangeL:Double,
                              retargetStatus:Boolean,
                              errorType:String,
                              errorMessage:String,
                              dataVersion:String,
                              configVersion:String,
                              runId:String,
                              retargetUser:String)



case class EwmaManualRetargetConfig(controlPlanId:Long,
                                    indicatorId:Long,
                                    specId:Long,
                                    limitConditionName:String,
                                    isEwma:Boolean,
                                    manualRetargetOnceTrigger:Boolean,
                                    rangeU:Double,
                                    rangeL:Double)
