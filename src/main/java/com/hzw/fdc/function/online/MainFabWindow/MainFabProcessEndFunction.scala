package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{ErrorCode, FdcData, MESMessage, MainFabRawData, RunEventData, lotMessage, sensorMessage, toolMessage}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions.iterableAsScalaIterable

class MainFabProcessEndFunction extends KeyedProcessFunction[String, JsonNode, (RunEventData, RunEventData, List[MainFabRawData])] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabProcessEndFunction])

  private var eventStartState:ValueState[RunEventData] = _
  private var rawDataState: ListState[MainFabRawData] = _
  private var sensorMessageStateMap: MapState[String,sensorMessage] = _

  lazy val MESOutput = new OutputTag[JsonNode]("toolMessage")


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)


    val eventStartStateDescription = new
        ValueStateDescriptor[RunEventData]("eventStartState", TypeInformation.of(classOf[RunEventData]))
    eventStartState = getRuntimeContext.getState(eventStartStateDescription)
    val rawDataStateDescription: ListStateDescriptor[MainFabRawData] = new
        ListStateDescriptor[MainFabRawData]("ListRawTraceState", TypeInformation.of(classOf[MainFabRawData]))
    rawDataState = getRuntimeContext.getListState(rawDataStateDescription)
    val sensorMessageStateDescription: MapStateDescriptor[String,sensorMessage] = new
        MapStateDescriptor[String,sensorMessage]("sensorMessageState", TypeInformation.of(classOf[String]),TypeInformation.of(classOf[sensorMessage]))
    sensorMessageStateMap = getRuntimeContext.getMapState(sensorMessageStateDescription)

  }

  override def processElement(ptData: JsonNode, readOnlyContext: KeyedProcessFunction[String, JsonNode,
    (RunEventData, RunEventData, List[MainFabRawData])]#Context, collector: Collector[(RunEventData, RunEventData, List[MainFabRawData])]): Unit = {
    try {
      val dataType = ptData.get(MainFabConstants.dataType).asText()


      if(dataType == MainFabConstants.rawData){
        val RawData = toBean[MainFabRawData](ptData)
        rawDataState.add(RawData)
        //组装MES数据
        for (elem <- RawData.data) {
          //过滤VirtualSensor
          if(!elem.isVirtualSensor){
            sensorMessageStateMap.put(elem.svid,
              sensorMessage(svid = elem.svid,
                sensorAlias = elem.sensorAlias,
                sensorName = elem.sensorName,
                unit = elem.unit))
          }
        }


      }else if(dataType == MainFabConstants.eventStart){
        val RunEventStart = toBean[RunEventData](ptData)
        eventStartState.update(RunEventStart)


      } else if (dataType == MainFabConstants.eventEnd) {

        try {
          val RunEventStart = eventStartState.value()
          val RunEventEnd = toBean[RunEventData](ptData)

          //有没有event start
          if (RunEventStart != null) {
            val gap = RunEventEnd.runEndTime - RunEventStart.runStartTime
            //过滤长时间run
            if (gap >= ProjectConfig.RUN_MAX_LENGTH * 60 * 60 * 1000) {
              eventStartState.clear()
              rawDataState.clear()
              val traceId = ptData.get(MainFabConstants.traceId).asText()

              logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(),
                Map("runTimeTotal" -> gap.toString, "RunEventStart" -> RunEventStart, "traceId" -> traceId), "run超过3小时，清除数据").toJson)
            }else{

              val MainFabRawDataList = rawDataState.get().toList

              collector.collect((RunEventStart, RunEventEnd, MainFabRawDataList))



              val MESMessageList = RunEventStart.lotMESInfo.map(elem => MESMessage(route = elem.get.route,
                `type` = elem.get.lotType,
                operation = elem.get.operation,
                layer = elem.get.layer,
                technology = elem.get.technology,
                stage = elem.get.stage,
                product = elem.get.product,
                lotData = lotMessage(locationName = RunEventStart.locationName, moduleName = RunEventStart.moduleName, toolName = RunEventStart.toolName,
                  chamberName = RunEventStart.chamberName,
                  lotName = elem.get.lotName,
                  carrier = elem.get.carrier)))
              for (elem <- MESMessageList) {

                readOnlyContext.output(MESOutput,
                  beanToJsonNode[FdcData[MESMessage]](FdcData(dataType = MainFabConstants.MESMessage,
                  datas = elem)))
              }

              val toolMessageData=FdcData(dataType = MainFabConstants.toolMessage,
                datas = toolMessage(locationName = RunEventStart.locationName,
                moduleName = RunEventStart.moduleName,
                toolName = RunEventStart.toolName,
                chamberName = RunEventStart.chamberName,
                sensors = sensorMessageStateMap.values().toList,
                recipeNames = List(RunEventStart.recipeName)))
              //组装MES数据

              readOnlyContext.output(MESOutput,beanToJsonNode[FdcData[toolMessage]](toolMessageData))


            }
          } else {
            logger.warn(ErrorCode("002001b001C", System.currentTimeMillis(), Map("RunEventEnd" -> RunEventEnd, "MainFabRawDataListSize" -> rawDataState.get().size), "没有RunEventStart").toJson)
          }

        } catch {
          case exception: Exception =>logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(), Map("MainFabRawDataListSize" -> "","RunEventEnd" -> "RunEventEnd"), ExceptionInfo.getExceptionInfo(exception)).toJson)
        }finally {
          // 清除状态
          rawDataState.clear()
          eventStartState.clear()
          sensorMessageStateMap.clear()
        }
      }
    }catch {
      case exception: Exception =>logger.warn(ErrorCode("002003d001C", System.currentTimeMillis(), Map("MainFabRawDataListSize" -> "","RunEventEnd" -> ""), exception.toString).toJson)

    }
  }

  override def close(): Unit = {
    super.close()
    rawDataState.clear()
    eventStartState.clear()
    sensorMessageStateMap.clear()
  }


//  def addStep(rawData: JsonNode): MainFabRawData = {
//
//    val PTRawData = toBean[MainFabPTRawData](rawData)
//    val sensorList = PTRawData.data
//
//    //标注是否有重复的step
//    var isOnlyOneStepID: Boolean = true
//    var isOnlyOneStepName: Boolean = true
//    var stepID: Long = -99
//    var StepName: String = MainFabConstants.NotApplicable
//
//    for (i <- 1 until sensorList.size) {
//
//      if (sensorList(i).sensorAlias.toLowerCase.indexOf("stepid") != -1 || sensorList(i).sensorAlias.toLowerCase.indexOf("step_id") != -1) {
//        if (isOnlyOneStepID) {
//
//          try {
//            stepID = sensorList(i).sensorValue.asInstanceOf[Number].longValue()
//          } catch {
//            case exception: Exception =>
//
//              logger.warn(ErrorCode("002003b003C", System.currentTimeMillis(), Map("rawData" -> PTRawData, "sensor" -> sensorList(i)), exception.toString).toJson)
//
//          }
//
//        } else {
//          stepID = -99
//          isOnlyOneStepID = false
//          logger.warn(ErrorCode("002003b004C", System.currentTimeMillis(), Map("rawData" -> PTRawData, "sensor" -> sensorList(i)), "").toJson)
//        }
//      }
//
//      if (sensorList(i).sensorAlias.toLowerCase.indexOf("stepname") != -1 || sensorList(i).sensorAlias.toLowerCase.indexOf("step_name") != -1) {
//
//        if (isOnlyOneStepName) {
//          try {
//            StepName = sensorList(i).sensorValue.toString
//          } catch {
//            case exception: Exception =>
//              logger.warn(ErrorCode("002003b006C", System.currentTimeMillis(), Map("rawData" -> PTRawData, "sensor" -> sensorList(i)), exception.toString).toJson)
//          }
//        } else {
//          StepName = MainFabConstants.NotApplicable
//          isOnlyOneStepName = false
//          logger.warn(ErrorCode("002003b007C", System.currentTimeMillis(), Map("rawData" -> PTRawData, "sensor" -> sensorList(i)), "").toJson)
//        }
//        val listBuffer = PTRawData.getData.to[ListBuffer]
//        listBuffer.remove(i)
//        //删除stepname 存不进opentsdb
//        PTRawData.setData(listBuffer.toList)
//      }
//    }
//
//
//    val sensorListData=PTRawData.getData.map(
//      one=>try {
//        sensorData(svid=one.svid,
//          sensorName = one.sensorName,
//          sensorAlias = one.sensorAlias,
//          isVirtualSensor = one.isVirtualSensor,
//          sensorValue = one.sensorValue.toString.toDouble,
//          unit = one.unit.getOrElse(""))
//      } catch {
//        case exception: Exception =>
//          logger.warn(ErrorCode("002003b008C", System.currentTimeMillis(), Map("toolName" -> PTRawData.toolName,"chamberName" -> PTRawData.chamberName, "sensorName" -> one), exception.toString).toJson)
//          null
//      }
//    ).filter(one=>one!=null)
//
//
//
//    MainFabRawData(dataType = PTRawData.dataType,
//      toolName = PTRawData.toolName,
//      chamberName = PTRawData.chamberName,
//      timestamp = PTRawData.timestamp,
//      traceId = PTRawData.traceId,
//      stepId = stepID,
//      stepName = StepName,
//      data =sensorListData
//
//    )
//
//
//  }

}
