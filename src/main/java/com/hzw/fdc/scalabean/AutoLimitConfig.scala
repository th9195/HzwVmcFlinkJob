package com.hzw.fdc.scalabean

/**
 *   离线计算autoLimit 的配置结构
 */

case class autoLimitSettings(dataType:String,
                              status:Boolean,
                              datas: AutoLimitConfig)

case class AutoLimitConfig(dataType: String,
                           controlPlanId: Long,
                           version: Long,
                           removeOutlier: String,
                           maxRunNumber: String,
                           every: String,
                           triggerMethodName: String,
                           triggerMethodValue: String,
                           active: String,
                           deploymentInfo: List[DeploymentInfo])


case class DeploymentInfo(indicatorId: Long,
                          specId: Long,
                          condition: Condition,
                          limitMethod: String,
                          limitValue: String,
                          cpk: String,
                          target: String)


case class Condition(tool: List[String],
                     chamber: List[String],
                     recipe: List[String],
                     product: List[String],
                     stage: List[String])

case class AutoLimitOneConfig(controlPlanId: Long,
                           version: Long,
                           removeOutlier: String,
                           maxRunNumber: String,
                           every: String,
                           triggerMethodName: String,
                           triggerMethodValue: String,
                           active: String,
                           indicatorId: Long,
                           specId: Long,
                           condition: Condition,
                           limitMethod: String,
                           limitValue: String,
                           cpk: String,
                           target: String)

//AutoLimitConfig(null,986,1,1,1000,1,By Count,4,0,
// List(
// DeploymentInfo(4346,1061,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,3916.9823),
// DeploymentInfo(4350,1063,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,3589.6197),
// DeploymentInfo(4351,1064,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,0.0017),
// DeploymentInfo(4365,1065,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,6.4412),
// DeploymentInfo(4352,1066,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,3901.333),
// DeploymentInfo(4406,1092,Condition(List(),List(),List(),List(),List()),percent,1,null,3.33),
// DeploymentInfo(4366,1062,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,2764.5859),
// DeploymentInfo(4377,1072,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,9961.4628),
// DeploymentInfo(4378,1073,Condition(List(),List(),List(),List(),List()),sigma,1,0.6667,-0.4968))
// )
