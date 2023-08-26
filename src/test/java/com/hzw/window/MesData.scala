package com.hzw.window

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.ExceptionInfo
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：Created in 2021/11/17 13:47
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MesData {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MesData])
  def run(value: (RunEventData, RunEventData, List[MainFabRawData])): Unit = {

      val runEndEvent = value._2
      val runStartEvent = value._1
      val rawDataList = value._3
      val runId = s"${runEndEvent.toolName}--${runEndEvent.chamberName}--${runStartEvent.runStartTime}"
      try {
        /**
         * 获取sensor列表
         * map(sensorAlias->(svid,sensorName,sensorAlias,unit,isVS,data))
         */
        val sensorMap = scala.collection.mutable.Map[String, (String, String, String, String, Boolean, ListBuffer[sensorDataList])]()
        for (elem <- rawDataList) {

          if (sensorMap.isEmpty) {
            //第一次循环
            for (x <- elem.data) {
              sensorMap += (x.sensorAlias -> (
                x.svid,
                x.sensorName,
                x.sensorAlias,
                x.unit,
                x.isVirtualSensor,
                ListBuffer(sensorDataList(x.sensorValue, elem.timestamp, elem.stepId))))
            }

          } else {
            for (y <- elem.data) {
              if (sensorMap.contains(y.sensorAlias)) {
                val old = sensorMap.get(y.sensorAlias).get
                val sensorListBuffer = old._6
                sensorListBuffer.append(sensorDataList(y.sensorValue, elem.timestamp, elem.stepId))
                sensorMap += (y.sensorAlias -> (old._1,
                  old._2,
                  old._3,
                  old._4,
                  old._5,
                  sensorListBuffer))
              } else {
                logger.warn(s"debug runId$runId sensorAlias:${y.sensorAlias}")
                sensorMap += (y.sensorAlias -> (
                  y.svid,
                  y.sensorName,
                  y.sensorAlias,
                  y.unit,
                  y.isVirtualSensor,
                  ListBuffer(sensorDataList(y.sensorValue, elem.timestamp, elem.stepId))))
              }

            }
          }

        }
        /**
         * 往后台发送MES数据
         * 过滤掉VirtualSensor
         */
        val toolData = toolMessage(runStartEvent.locationName,
          runStartEvent.moduleName,
          toolName = runStartEvent.toolName,
          chamberName = runStartEvent.chamberName,
          //过滤掉VirtualSensor
          sensors = sensorMap.filter(s => s._2._5.equals(false)).map(s => sensorMessage(s._2._1, s._1, s._2._3, s._2._4)).toList,
          recipeNames = List(runStartEvent.recipeName))


        for (elem <- runStartEvent.lotMESInfo if elem.nonEmpty) {

          val MESData = MESMessage(route = elem.get.route,
            `type` = elem.get.lotType,
            operation = elem.get.operation,
            layer = elem.get.layer,
            technology = elem.get.technology,
            stage = elem.get.stage,
            product = elem.get.product,
            lotData = lotMessage(locationName = runStartEvent.locationName, moduleName = runStartEvent.moduleName, toolName = runEndEvent.toolName,
              chamberName = runEndEvent.chamberName,
              lotName = elem.get.lotName,
              carrier = elem.get.carrier))

          println(s"$MESData")

        }


      } catch {
        case e: Exception => logger.warn(ErrorCode("002008b001C", System.currentTimeMillis(), Map("rawDataList" -> rawDataList), ExceptionInfo.getExceptionInfo(e)).toJson)
      }


  }
}
