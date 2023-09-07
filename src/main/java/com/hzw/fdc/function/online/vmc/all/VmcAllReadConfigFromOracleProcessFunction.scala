package com.hzw.fdc.function.online.vmc.all

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.VmcBeans.{VmcConfig, VmcControlPlanConfig, VmcEventData, VmcEventDataMatchControlPlan, VmcLot, VmcRawData}
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}


class VmcAllReadConfigFromOracleProcessFunction() extends KeyedProcessFunction[String, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllReadConfigFromOracleProcessFunction])

  private var vmcEventDataMatchControlPlanListState: ListState[VmcEventDataMatchControlPlan] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // todo 初始化oracle
    OracleUtil.getConnection()

    // 26小时过期
    val hour26TTLConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    val vmcEventDataMatchControlPlanListStateDescription: ListStateDescriptor[VmcEventDataMatchControlPlan] = new
        ListStateDescriptor[VmcEventDataMatchControlPlan]("vmcEventDataMatchControlPlanListState", TypeInformation.of(classOf[VmcEventDataMatchControlPlan]))

    vmcEventDataMatchControlPlanListStateDescription.enableTimeToLive(hour26TTLConfig)

    vmcEventDataMatchControlPlanListState = getRuntimeContext.getListState(vmcEventDataMatchControlPlanListStateDescription)

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
    val lotMESInfo = eventStartData.lotMESInfo
    if(null != lotMESInfo && lotMESInfo.nonEmpty){
      // 匹配vmcControlPlan
      matchVmcControlPlan(eventStartData,context,collector)
      // 分发eventStart
      collectEventStartData(eventStartData,context,collector)

    }else{
      logger.error(s"eventStart lotMESInfo is null ! \n " +
        s"eventStartData == ${eventStartData.toJson}")
    }

  }



  def matchVmcControlPlan(vmcEventData: VmcEventData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val toolName = vmcEventData.toolName
    val lotMESInfo = vmcEventData.lotMESInfo
    val recipeName = vmcEventData.recipeName

    lotMESInfo.foreach((lot: Option[VmcLot]) => {
      val lotInfo = lot.get
      val stageName = lotInfo.stage
      val route = lotInfo.route
      if (!route.isEmpty && !stageName.isEmpty) {
        // todo 点查oracle
        val vmcControlPlanConfigList = OracleUtil.queryVmcOracle(toolName, route.get, stageName.get)

        vmcControlPlanConfigList.foreach(vmcControlPlanConfig => {
          val recipeSubName = vmcControlPlanConfig.recipeSubName
          if(recipeName.contains(recipeSubName)){
            // todo 匹配上vmcControlPlanConfig
            cacheStateMatchedEventStartData(vmcEventData,vmcControlPlanConfig,context,collector)
          }
        })
      }
    })

  }



  def cacheStateMatchedEventStartData(vmcEventData: VmcEventData,
                                      vmcControlPlanConfig: VmcControlPlanConfig,
                                      context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,
                                      collector: Collector[JsonNode]) = {
    val vmcEventDataMatchControlPlan = generateVmcEventDataMatchControlPlan(vmcEventData, vmcControlPlanConfig)
    vmcEventDataMatchControlPlanListState.add(vmcEventDataMatchControlPlan)

  }

  def generateVmcEventDataMatchControlPlan(vmcEventData: VmcEventData, vmcControlPlanConfig: VmcControlPlanConfig) = {
    VmcEventDataMatchControlPlan(dataType = vmcEventData.dataType,
      locationName = vmcEventData.locationName,
      moduleName = vmcEventData.moduleName,
      toolName = vmcEventData.toolName,
      chamberName = vmcEventData.chamberName,
      recipeName = vmcEventData.recipeName,
      recipeActual = vmcEventData.recipeActual,
      runStartTime = vmcEventData.runStartTime,
      runEndTime = vmcEventData.runEndTime,
      runId = vmcEventData.runId,
      traceId = vmcEventData.traceId,
      DCType = vmcEventData.DCType,
      dataMissingRatio = vmcEventData.dataMissingRatio,
      timeRange = vmcEventData.timeRange,
      completed = vmcEventData.completed,
      materialName = vmcEventData.materialName,
      materialActual = vmcEventData.materialActual,
      lotMESInfo = vmcEventData.lotMESInfo,
      errorCode = vmcEventData.errorCode,
      vmcControlPlanConfig = vmcControlPlanConfig)
  }


  def collectEventStartData(eventStartData: VmcEventData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val vmcEventDataMatchControlPlanList: List[VmcEventDataMatchControlPlan] = vmcEventDataMatchControlPlanListState.get.toList
    vmcEventDataMatchControlPlanList.foreach(vmcEventDataMatchControlPlan => {
      collector.collect(beanToJsonNode[VmcEventDataMatchControlPlan](vmcEventDataMatchControlPlan))
    })
  }

  def collectRawData(rawData: VmcRawData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {

  }

  def collectEventEndData(eventEndData: VmcEventData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {

  }
}
