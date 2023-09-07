package com.hzw.fdc.function.online.vmc.all

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.VmcBeans.{VmcConfig, VmcControlPlanConfig, VmcEventData, VmcLot}
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer


class VmcAllReadConfigFromOracleProcessFunction() extends KeyedProcessFunction[String, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllReadConfigFromOracleProcessFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // todo 初始化oracle
    OracleUtil.getConnection()

  }


  override def processElement(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType= inputValue.findPath(VmcConstants.DATA_TYPE).asText()

      if(dataType == VmcConstants.EVENT_START){

        processEventStart(inputValue,context,collector)

      }else if (dataType == VmcConstants.EVENT_END){

        collector.collect(inputValue)

      }else if (dataType == VmcConstants.RAWDATA){

        collector.collect(inputValue)

      }

    }catch  {
      case e:Exception => {
        logger.error(s"解析源数据失败！\n " +
          s"inputValue == ${inputValue}")
      }
    }
  }

  def processEventStart(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val eventStartData = toBean[VmcEventData](inputValue)
    val toolName = eventStartData.toolName
    val lotMESInfo = eventStartData.lotMESInfo
    if(null != lotMESInfo && lotMESInfo.nonEmpty){
      val configs: List[VmcControlPlanConfig] = lotMESInfo.flatMap((lot: Option[VmcLot]) => {
        val lotInfo = lot.get
        val stageName = lotInfo.stage
        val route = lotInfo.route
        if (!route.isEmpty && !stageName.isEmpty) {
          // todo 点查oracle
          OracleUtil.queryVmcOracle(toolName, route.get, stageName.get)

        } else {
          new ListBuffer[VmcControlPlanConfig]()
        }
      }).filter((elem: VmcControlPlanConfig) => {
        null != elem
      })
    }else{
      logger.error(s"eventStart lotMESInfo is null ! \n " +
        s"eventStartData == ${eventStartData.toJson}")
    }

  }

}
