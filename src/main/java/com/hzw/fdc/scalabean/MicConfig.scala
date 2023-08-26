package com.hzw.fdc.scalabean

case class MicConfig(controlPlanId:Long,
                     controlPlanName: String,
                     controlPlanVersion:Long,
                     actionId:String,
                     ocapActionId:String,
                     actionName:String,
                     micId:Long,
                     micName: String,
                     xofTotal: Long,
                     `def`:List[indicatorLimit],
                     actions:List[Action]
                    )

case class Action(id:Long,
                  `type`:String,
                  param:Option[String],
                  sign:String)

case class indicatorLimit(indicatorId: Long, indicatorName:String, oocLevel: List[Long])
