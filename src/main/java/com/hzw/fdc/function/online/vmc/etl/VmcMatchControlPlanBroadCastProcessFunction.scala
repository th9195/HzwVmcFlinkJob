package com.hzw.fdc.function.online.vmc.etl

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.VmcBeans.{VmcConfig, VmcControlPlanConfig}
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

class VmcMatchControlPlanBroadCastProcessFunction() extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcMatchControlPlanBroadCastProcessFunction])

  private val matchControlPlanMap:TrieMap[String,String] = new TrieMap[String,String]()


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val toolNameConfigListStateDescription: ListStateDescriptor[String] = new
        ListStateDescriptor[String]("toolNameConfigListState", TypeInformation.of(classOf[String]))
  }

  override def processElement(inputValue: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]): Unit = {
    try {


    }catch {
      case ex: Exception => logger.error(ErrorCode("0000000001", System.currentTimeMillis(),
        Map("msg" -> "VmcFilterToolError->processElement"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 1- 解析controlPlan 配置信息
   * 2- 解析window配置信息
   *
   * @param inputData
   * @param context
   * @param collector
   */
  override def processBroadcastElement(inputDimValue: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType = inputDimValue.get(VmcConstants.DATA_TYPE).asText()

      if(VmcConstants.VMC_CONTROLPLAN_CONFIG == dataType ){

        try {

          val vmcConfig: VmcConfig[VmcControlPlanConfig] = toBean[VmcConfig[VmcControlPlanConfig]](inputDimValue)
          parseVmcControlPlanConfig(vmcConfig)

        } catch {
          case e: Exception => logger.error(s"parseVmcControlPlanConfig error; " +
            s"inputDimValue == ${inputDimValue}  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      }
    }catch {
      case e:Exception => {
        logger.warn(s"processBroadcastElement error data:$inputDimValue  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
      }
    }

  }


  def parseVmcControlPlanConfig(vmcConfig: VmcConfig[VmcControlPlanConfig]) = {



  }


}
