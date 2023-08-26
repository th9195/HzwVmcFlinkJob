package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

case class MESMessage(route: Option[String],
                      `type`: Option[String],
                      operation: Option[String],
                      layer: Option[String],
                      technology: Option[String],
                      stage: Option[String],
                      product: Option[String],
                      lotData: lotMessage)

case class lotMessage(locationName: String,
                      moduleName: String,
                      toolName: String,
                      chamberName: String,
                      lotName: Option[String],
                      carrier: Option[String])

case class toolMessage(locationName: String,
                       moduleName: String,
                       @BeanProperty var toolName: String,
                       chamberName: String,
                       sensors: List[sensorMessage],
                       recipeNames: List[String])

case class sensorMessage(svid:String,
                         sensorAlias: String,
                         sensorName: String,
                         unit:String)