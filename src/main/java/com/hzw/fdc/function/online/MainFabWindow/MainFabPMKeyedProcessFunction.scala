package com.hzw.fdc.function.online.MainFabWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants}
import org.apache.commons.httpclient.util.DateUtil
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

/**
 * @author ：gdj
 * @date ：Created in 2021/9/29 18:26
 *
 */
class MainFabPMKeyedProcessFunction extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode] {

  val PMStatus = new concurrent.TrieMap[String, (PMData,Boolean)]()
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabPMKeyedProcessFunction])

  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  val job_fun_DebugCode:String = "022001"
  val jobName:String = "MainFabDataTransformWindowService"
  val optionName : String = "MainFabPMKeyedProcessFunction"

  override def processElement(value: JsonNode,
                              ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext,
                              out: Collector[JsonNode]): Unit = {

    try {
      value.path(MainFabConstants.dataType).asText() match {
        case MainFabConstants.eventStart => {

          val runStart = toBean[RunEventData](value)
          val key = s"${runStart.toolName}|${runStart.chamberName}"
          if (PMStatus.contains(key)) {

            if (PMStatus(key)._1.PMStatus == MainFabConstants.pmEnd) {
              runStart.setPmStatus(MainFabConstants.pmEnd)
              runStart.setPmTimestamp(PMStatus(key)._1.timestamp)
              val jsonNode = beanToJsonNode(runStart)
              PMStatus.remove(key)
              out.collect(jsonNode)
            } else if (PMStatus(key)._1.PMStatus == MainFabConstants.pmStart) {

              runStart.setPmStatus(MainFabConstants.pmStart)
              runStart.setPmTimestamp(PMStatus(key)._1.timestamp)
              val jsonNode = beanToJsonNode(runStart)
//              PMStatus.remove(key)
              out.collect(jsonNode)

            } else {
              out.collect(value)
            }

          } else {
            out.collect(value)
          }

        }

        case MainFabConstants.eventEnd => {
          out.collect(value)

        }

        case MainFabConstants.rawData => {

          out.collect(value)
        }
        case _ => {

          out.collect(value)
        }
      }
    } catch {
      case e: Exception => logger.error(s"MainFabPMKeyedProcessFunction :${ExceptionInfo.getExceptionInfo(e)} data :$value")
        out.collect(value)
    }
  }

  override def processBroadcastElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context, out: Collector[JsonNode]): Unit = {
    try {
      val dataType = value.path(MainFabConstants.dataType).asText()
      dataType match {
        case MainFabConstants.PM => {
          val pmData = toBean[FdcData[PMData]](value)
          if (pmData.datas.toolName.isEmpty && pmData.datas.chamberName.isEmpty) {
            logger.warn(s"数据异常,toolName,chamberName或为空：$value")
            return
          }
          val key = s"${pmData.datas.toolName}|${pmData.datas.chamberName}"
          val lastPmStatus = PMStatus.getOrElse(key, (null, false))._2
          val nowPmStatus = pmData.datas.PMStatus match {
            case "on" => true
            case "off" => false
            case null => lastPmStatus
            case _ =>
              logger.warn(s"PMStatus 异常:${pmData.datas.PMStatus}")
              lastPmStatus
          }
          if (nowPmStatus) {
            pmData.datas.setPMStatus(MainFabConstants.pmStart)
          } else if (!nowPmStatus && lastPmStatus) {
            pmData.datas.setPMStatus(MainFabConstants.pmEnd)
          }
          PMStatus.put(key, (pmData.datas, nowPmStatus))
        }
        case _ => {
          logger.warn(s"未知数据状态: $value")
        }
      }
    }catch {
      case e: Exception => logger.error(s"MainFabPMKeyedProcessFunction 配置报错:${ExceptionInfo.getExceptionInfo(e)} " +
        s"data :$value")
    }
  }


  /**
   * 生成日志信息
   * @param debugCode
   * @param message
   * @param paramsInfo
   * @param dataInfo
   * @param exception
   * @return
   */
  def generateMainFabLogInfo(debugCode:String,funName:String,message:String,paramsInfo:Map[String,Any]=null,dataInfo:String="",exception:String = ""): MainFabLogInfo = {
    MainFabLogInfo(mainFabDebugCode = job_fun_DebugCode + debugCode,
      jobName = jobName,
      optionName = optionName,
      functionName = funName,
      logTimestamp = DateTimeUtil.getCurrentTimestamp,
      logTime = DateTimeUtil.getCurrentTime(),
      message = message,
      paramsInfo = paramsInfo,
      dataInfo = dataInfo,
      exception = exception)
  }
}
