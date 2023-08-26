package com.hzw.fdc.function.PublicFunction

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{ErrorCode, MainFabPTRawData, MainFabRawData, PTSensorData, sensorData}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants}
import org.apache.flink.api.common.state.MapState
import org.slf4j.Logger

import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：2021/12/22 00:39
 * @param
 * @return
 */
object AddStep {


  def addStep(rawData: JsonNode, logger: Logger, isCacheFailSensor: Boolean,ptRawDataStateMap: MapState[String, Long]): Option[MainFabRawData] = {

    try {
      val PTRawData = toBean[MainFabPTRawData](rawData)
      val traceId: String = PTRawData.traceId
      val lastStepId: Long = try {
          if (ptRawDataStateMap.contains(traceId)) ptRawDataStateMap.get(traceId) else 0
        }catch {
          case e: Exception => logger.warn(s"${e.toString}")
            0
        }
      val sensorList = PTRawData.data

      //标注是否有重复的step
      var isOnlyOneStepID: Boolean = true
      var isOnlyOneStepName: Boolean = true
      var stepID: Long = if(lastStepId == 0) 1 else lastStepId
      var StepName: String = MainFabConstants.NotApplicable
//      var StepNameIndex: Int = 0

      for (elem <- sensorList) {

        if (isOnlyOneStepID) {
          if (elem.sensorAlias.toLowerCase.equals("stepid")) {

            try {

              val sensorValue = elem.sensorValue
              if(null != sensorValue){
                stepID = sensorValue.asInstanceOf[Number].longValue()
              }else{
                isOnlyOneStepID = false
              }

            } catch {
              case exception: Exception =>
                logger.warn(ErrorCode("002003b003C", System.currentTimeMillis(), Map("traceId" -> PTRawData.traceId,
                  "sensor" -> elem), exception.toString).toJson)
              //stepID = 1L
            }finally {
              isOnlyOneStepID = false
            }
          }
        }

        if (isOnlyOneStepName) {
          if (elem.sensorAlias.toLowerCase.indexOf("stepname") != -1 ||
            elem.sensorAlias.toLowerCase.indexOf("step_name") != -1) {

            try {
              val sensorValue = elem.sensorValue
              if(null != sensorValue){
                StepName = sensorValue.toString
              }else{
                StepName = MainFabConstants.NotApplicable
              }
            } catch {
              case exception: Exception =>
                logger.warn(ErrorCode("002003b006C", System.currentTimeMillis(), Map("rawData" -> PTRawData,
                  "sensor" -> elem), exception.toString).toJson)
                StepName = MainFabConstants.NotApplicable
            }finally {
              isOnlyOneStepName = false
            }
          }
//          StepNameIndex = i
        }
      }

      ptRawDataStateMap.put(traceId,stepID)

//      try{
//        if(!isOnlyOneStepName){
//          val listBuffer = PTRawData.getData.to[ListBuffer]
//          listBuffer.remove(StepNameIndex)
//          //删除stepname 存不进opentsdb
//          PTRawData.setData(listBuffer.toList)
//        }
//      }catch {
//        case exception: Exception =>
//          logger.warn(ErrorCode("002003b008C", System.currentTimeMillis(), Map("rawData" -> rawData,
//            "error" -> "删除stepname 错误"), exception.toString).toJson)
//      }

      // 获取sensorDataList
      val sensorListData = getSensorListData(PTRawData, isCacheFailSensor, logger)


      Option(MainFabRawData(dataType = PTRawData.dataType,
        dataVersion = PTRawData.dataVersion,
        toolName = PTRawData.toolName,
        chamberName = PTRawData.chamberName,
        timestamp = PTRawData.timestamp,
        traceId = PTRawData.traceId,
        stepId = stepID,
        stepName = StepName,
        data = sensorListData
      ))
    } catch {
      case exception: Exception =>
        logger.warn(ErrorCode("002003b008C", System.currentTimeMillis(), Map("rawData" -> rawData,
          "error" -> "add step 错误"), ExceptionInfo.getExceptionInfo(exception)).toJson)
        None
    }
  }


  // ===============需要处理一个问题： sensorValue 不为double类型， 但是sensorname需要写入mes信息 ========================

  def getSensorListData(PTRawData: MainFabPTRawData, isCacheFailSensor: Boolean, logger: Logger): List[sensorData] = {
    // 保留sensor 不为double类型的sensorName
    if(isCacheFailSensor){
      val sensorListData = PTRawData.getData.map(
        one => try {
          sensorData(svid = one.svid,
            sensorName = one.sensorName,
            sensorAlias = one.sensorAlias,
            isVirtualSensor = one.isVirtualSensor,
            sensorValue = one.sensorValue.toString.toDouble,
            unit = one.unit.getOrElse(""))
        } catch {
          case exception: Exception =>
            // 这里处理不一样
            sensorData(svid = one.svid,
              sensorName = one.sensorName,
              sensorAlias = one.sensorAlias,
              isVirtualSensor = one.isVirtualSensor,
              sensorValue = Double.NaN,
              unit = one.unit.getOrElse(""))
        }
      ).filter(one => one != null)
      sensorListData
    }else{
      // 不保留
      val sensorListData = PTRawData.getData.map(
        one => try {
          sensorData(svid = one.svid,
            sensorName = one.sensorName,
            sensorAlias = one.sensorAlias,
            isVirtualSensor = one.isVirtualSensor,
            sensorValue = one.sensorValue.toString.toDouble,
            unit = one.unit.getOrElse(""))
        } catch {
          case exception: Exception =>
            None
            //            logger.warn(s"sensorValue数字类型错误：${PTRawData.toolName}|${PTRawData.chamberName}|${one.sensorName} " +
            //              s"${one.sensorValue}: ${exception.toString}")
            null
        }
      ).filter(one => one != null)
      sensorListData
    }
  }

}
