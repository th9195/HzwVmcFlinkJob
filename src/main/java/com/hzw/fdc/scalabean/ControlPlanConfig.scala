package com.hzw.fdc.scalabean


case class ControlPlanConfig(locationId:Long,
                             locationName:String,
                             moduleId:Long,
                             moduleName:String,
                             toolName:String,
                             toolId:Long,
                             chamberName:String,
                             chamberId:Long,
                             recipeName: String,
                             recipeId:Long,
                             productName:String,
                             stageName:String,
                             controlPlanId:Long,
                             toolGroupId:Long,
                             toolGroupName:String,
                             chamberGroupId:Long,
                             chamberGroupName: String,
                             recipeGroupId:Long,
                             recipeGroupName:String,
                             limitStatus:Boolean,
                             area:String,
                             section:String,
                             mesChamberName:String)

case class ControlPlanConfig2(optionCode:Long,
                              locationId:Long,
                              locationName:String,
                              moduleId:Long,
                              moduleName:String,
                              controlPlanId:Long,
                              controlPlanVersion:Long,
                              toolGroupId:Long,
                              toolGroupName:String,
                              chamberGroupId:Long,
                              chamberGroupName: String,
                              recipeGroupId:Long,
                              recipeGroupName:String,
                              limitStatus:Boolean,
                              area:String,
                              section:String,
                              toolInfoMap:Map[String,ToolChamberInfo],
                              recipeInfoMap:Map[String,Long],
                              productNameList:List[String],
                              stageNameList:List[String])

//case class SingleControlPlanConfig2(locationId:Long,
//                                     locationName:String,
//                                     moduleId:Long,
//                                     moduleName:String,
//                                     toolName:String,
//                                     toolId:Long,
//                                     chamberName:String,
//                                     chamberId:Long,
//                                     recipeName: String,
//                                     recipeId:Long,
//                                     productName:String,
//                                     stageName:String,
//                                     controlPlanId:Long,
//                                     toolGroupId:Long,
//                                     toolGroupName:String,
//                                     chamberGroupId:Long,
//                                     chamberGroupName: String,
//                                     recipeGroupId:Long,
//                                     recipeGroupName:String,
//                                     limitStatus:Boolean,
//                                     area:String,
//                                     section:String)

case class ToolChamberInfo(toolId:Long,chamberInfoMap:Map[String,Long])




case class MatchedControlPlanScore(score:Int,
                                   matchedControlPlanConfigList:List[ControlPlanConfig])

case class MatchedControlPlanScore2(score:Int,
                                    matchControlPlanConfig:ControlPlanConfig)






