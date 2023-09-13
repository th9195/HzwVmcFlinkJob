package com.hzw.fdc.function.online.vmc.all

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.ErrorCode
import com.hzw.fdc.scalabean.VmcBeans.{VmcRawDataMatchedControlPlan, _}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions.iterableAsScalaIterable

class VmcAllAddStepProcessFunction() extends KeyedProcessFunction[String, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllAddStepProcessFunction])

  // 记录当前stepId
  var vmcRawDataCurrentStepIdState: ValueState[Long] = _

  // 记录所有的stepId集合 ; 注意: 需要去重. 有多少个stepId 就需要发送多少个eventStart 和 evnetEnd
  var vmcRawDataStepIdListState: ValueState[String] = _

  // 记录rawData的序号 index
  var vmcRawDataIndexState: ValueState[Long] = _

  // 记录eventStart
  var vmcEventDataMatchControlPlanState: ValueState[JsonNode] = _

  // 记录匹配上的controlPlan 中配置了哪些sensor信息
  var vmcControlPlanConfigSensorListState: ListState[String] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()


    val vmcRawDataStepIdStateDescription: ValueStateDescriptor[Long] = new
        ValueStateDescriptor[Long]("vmcRawDataStepIdState", TypeInformation.of(classOf[Long]))
    // 设置过期时间
    vmcRawDataStepIdStateDescription.enableTimeToLive(ttlConfig)
    vmcRawDataCurrentStepIdState = getRuntimeContext.getState(vmcRawDataStepIdStateDescription)

    val vmcRawDataStepIdListStateDescription = new
        ValueStateDescriptor[String]("vmcRawDataStepIdListState", TypeInformation.of(classOf[String]))
    vmcRawDataStepIdListStateDescription.enableTimeToLive(ttlConfig)
    vmcRawDataStepIdListState = getRuntimeContext.getState(vmcRawDataStepIdListStateDescription)


    val vmcRawDataIndexStateDescription: ValueStateDescriptor[Long] = new
        ValueStateDescriptor[Long]("vmcRawDataIndexState", TypeInformation.of(classOf[Long]))
    // 设置过期时间
    vmcRawDataIndexStateDescription.enableTimeToLive(ttlConfig)
    vmcRawDataIndexState = getRuntimeContext.getState(vmcRawDataIndexStateDescription)


    val vmcEventDataMatchControlPlanStateDescription: ValueStateDescriptor[JsonNode] = new
        ValueStateDescriptor[JsonNode]("vmcEventDataMatchControlPlanState", TypeInformation.of(classOf[JsonNode]))
    // 设置过期时间
    vmcEventDataMatchControlPlanStateDescription.enableTimeToLive(ttlConfig)
    vmcEventDataMatchControlPlanState = getRuntimeContext.getState(vmcEventDataMatchControlPlanStateDescription)


    val vmcControlPlanConfigSensorListStateDescription: ListStateDescriptor[String] = new
        ListStateDescriptor[String]("vmcControlPlanConfigSensorListState", TypeInformation.of(classOf[String]))
    vmcControlPlanConfigSensorListStateDescription.enableTimeToLive(ttlConfig)
    vmcControlPlanConfigSensorListState = getRuntimeContext.getListState(vmcControlPlanConfigSensorListStateDescription)

  }


  override def processElement(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType= inputValue.findPath(VmcConstants.DATA_TYPE).asText()

      if(dataType == VmcConstants.EVENT_START){

        processEventStart(inputValue,context,collector)

      }else if (dataType == VmcConstants.EVENT_END){

        processEventEnd(inputValue,context,collector)

      }else if (dataType == VmcConstants.RAWDATA){

        processRawData(inputValue,context,collector)
      }

    }catch  {
      case e:Exception => {
        logger.error(s"添加StepId失败！\n " +
          s"inputValue == ${inputValue}")
      }
    }
  }

  /**
   * 处理 eventStart 数据
   * @param inputValue
   * @param context
   * @param collector
   */
  def processEventStart(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    try{
      updateVmcControlPlanConfigSensorListState(inputValue)
      updateVmcEventDataMatchedControlPlan(inputValue)
    }catch {
      case e:Exception => {
        logger.error(s"processEventStart error ! ")
      }
    }

  }

  /**
   * 处理eventEnd 数据
   * @param inputValue
   * @param context
   * @param collector
   */
  def processEventEnd(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    try{
      collectEventEnd(inputValue,context,collector)
      clearAllState()

    }catch{
      case e:Exception => {
        logger.error(s"processEventEnd error ! inputValue == ${inputValue}")
      }
    }
  }

  /**
   * 处理 rawData数据
   * @param inputValue
   * @param context
   * @param collector
   */
  def processRawData(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    try{
      updateVmcRawDataIndexState()
      addStep(inputValue,context,collector)
    }catch{
      case e:Exception => {
        logger.error(s"processRawData error ! inputValue == ${inputValue}")
      }
    }
  }

  /**
   * 更新状态变量 : vmcEventDataMatchControlPlanState
   * @param inputValue
   */
  def updateVmcEventDataMatchedControlPlan(inputValue: JsonNode) = {
    vmcEventDataMatchControlPlanState.update(inputValue)
  }

  /**
   * 更新状态变量 : vmcControlPlanConfigSensorListState
   * @param inputValue
   */
  def updateVmcControlPlanConfigSensorListState(inputValue: JsonNode) = {
    val vmcEventDataMatchControlPlan = toBean[VmcEventDataMatchControlPlan](inputValue)
    val vmcControlPlanConfig: VmcControlPlanConfig = vmcEventDataMatchControlPlan.vmcControlPlanConfig

    vmcControlPlanConfig.vmcSensorInfoList.foreach(elem => {
      vmcControlPlanConfigSensorListState.add(elem.vmcSensorFdcName)
    })

  }

  /**
   * 更新状态变量 : vmcRawDataIndexState
   */
  def updateVmcRawDataIndexState() = {
    val vmcRawDataIndex = vmcRawDataIndexState.value()
    if(null == vmcRawDataIndex) {
      vmcRawDataIndexState.update(1)
    }else{
      vmcRawDataIndexState.update( vmcRawDataIndex + 1)
    }
  }

  /**
   * 更新状态变量 : vmcRawDataStepIdListState
   * @param currentStepId
   * @return
   */
  def updateVmcRawDataStepIdListState(currentStepId: Long): Boolean = {
    val stepIdStr: String = vmcRawDataStepIdListState.value()

    if(null == stepIdStr){
      vmcRawDataStepIdListState.update(currentStepId.toString)
      true
    }else if(!stepIdStr.split("\\|").contains(currentStepId)){
      var stepIdList: List[String] = stepIdStr.split("\\|").toList

      stepIdList = stepIdList :+ currentStepId.toString
      val newStepIdStr = stepIdList.mkString("|")
      vmcRawDataStepIdListState.update(newStepIdStr)
      true
    }else{
      false
    }

  }

  /**
   * 添加 stepId stepName
   * @param inputValue
   * @param context
   * @param collector
   */
  def addStep(inputValue: JsonNode,
              context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,
              collector: Collector[JsonNode]) = {
    try {
      val vmcRawDataMatchedControlPlan = toBean[VmcRawDataMatchedControlPlan](inputValue)

      val stepIdName = getStepIdAndStepName(vmcRawDataMatchedControlPlan)

      // 获取sensorDataList
      val sensorListData: List[VmcSensorData] = getSensorListData(vmcRawDataMatchedControlPlan, logger)

      // 生成VmcRawDataAddStep
      val stepId = stepIdName._1
      val stepName = stepIdName._2
      val vmcRawDataAddStep: VmcRawDataAddStep = generateVmcRawDataAddStep(vmcRawDataMatchedControlPlan, stepId, stepName, sensorListData)

      val isSendEventStart:Boolean = updateVmcRawDataStepIdListState(stepId)
      // 输出 eventStart
      if(isSendEventStart){
        collectEventStart(stepId,collector)
      }

      // 输出 rawData
      collectRawData(vmcRawDataAddStep,collector)
    } catch {
      case exception: Exception =>
        logger.warn(ErrorCode("002003b008C", System.currentTimeMillis(), Map("rawData" -> inputValue,
          "error" -> "add step 错误"), ExceptionInfo.getExceptionInfo(exception)).toJson)
    }
  }

  /**
   * 中rawData数据中 获取 stepId stepName
   * @param vmcRawDataMatchedControlPlan
   * @return
   */
  def getStepIdAndStepName(vmcRawDataMatchedControlPlan:VmcRawDataMatchedControlPlan) = {
    //标注是否有重复的step
    var isOnlyOneStepID: Boolean = true
    var isOnlyOneStepName: Boolean = true

    // 获取上次的stepId
    val lastStepId: Long = try {
      if(null == vmcRawDataCurrentStepIdState.value()){
        logger.warn(s"null == vmcRawDataStepIdState.value()")
        0
      } else {
        vmcRawDataCurrentStepIdState.value()
      }
    }catch {
      case e: Exception => logger.warn(s"${e.toString}")
        0
    }

    // 如果上次没有stepId 就 从1 开始   (就是第一个rawData)
    var stepID: Long = if(lastStepId == 0) 1 else lastStepId
    var stepName: String = VmcConstants.NA

    val sensorList = vmcRawDataMatchedControlPlan.data

    for (elem: VmcSensorData <- sensorList) {

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
              logger.warn(ErrorCode("002003b003C", System.currentTimeMillis(), Map("traceId" -> vmcRawDataMatchedControlPlan.traceId,
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
              stepName = sensorValue.toString
            }else{
              stepName = VmcConstants.NA
            }
          } catch {
            case exception: Exception =>
              logger.warn(ErrorCode("002003b006C", System.currentTimeMillis(), Map("rawData" -> vmcRawDataMatchedControlPlan.toJson,
                "sensor" -> elem), exception.toString).toJson)
              stepName = VmcConstants.NA
          }finally {
            isOnlyOneStepName = false
          }
        }
      }
    }
    (stepID,stepName)
  }

  /**
   * 1- 过滤正常的sensorValue的数据
   * 2- 保留controlPlan中配置的sensorAlias
   * @param vmcRawDataMatchedControlPlan
   * @param logger
   * @return
   */
  def getSensorListData(vmcRawDataMatchedControlPlan: VmcRawDataMatchedControlPlan, logger: Logger) = {

    // 不保留
    val sensorListData = vmcRawDataMatchedControlPlan.getData.map(
      one => try {
        VmcSensorData(svid = one.svid,
          sensorName = one.sensorName,
          sensorAlias = one.sensorAlias,
          sensorValue = one.sensorValue.toString.toDouble,
          unit = one.unit)
      } catch {
        case exception: Exception =>
          null
      }
    ).filter(one => {
      // todo 只保留策略中使用到的sensor
      val configSensorList: List[String] = vmcControlPlanConfigSensorListState.get().toList
      one != null && configSensorList.contains(one.sensorAlias)
    })
    sensorListData
  }

  /**
   * 生成 对象 VmcRawDataAddStep
   * @param vmcRawDataMatchedControlPlan
   * @param stepID
   * @param StepName
   * @param sensorListData
   * @return
   */
  def generateVmcRawDataAddStep(vmcRawDataMatchedControlPlan: VmcRawDataMatchedControlPlan, stepID: Long, StepName: String, sensorListData: List[VmcSensorData]) = {
    VmcRawDataAddStep(dataType = vmcRawDataMatchedControlPlan.dataType,
      toolName = vmcRawDataMatchedControlPlan.toolName,
      chamberName = vmcRawDataMatchedControlPlan.chamberName,
      timestamp = vmcRawDataMatchedControlPlan.timestamp,
      traceId = vmcRawDataMatchedControlPlan.traceId,
      index = vmcRawDataIndexState.value(),
      data = sensorListData,
      controlPlanId = vmcRawDataMatchedControlPlan.controlPlanId,
      stepId = stepID,
      stepName = StepName)
  }


  /**
   * 输出eventStart
   *  如果有新的stepId 就输出一次eventStart
   * @param currentStepId
   * @param collector
   */
  def collectEventStart(currentStepId: Long, collector: Collector[JsonNode]) = {

    val eventStartJson: JsonNode = vmcEventDataMatchControlPlanState.value()
    val vmcEventDataMatchControlPlan = toBean[VmcEventDataMatchControlPlan](eventStartJson)
    vmcEventDataMatchControlPlan.stepId = currentStepId

    collector.collect(beanToJsonNode[VmcEventDataMatchControlPlan](vmcEventDataMatchControlPlan))

  }

  /**
   * 输出rawData
   * @param vmcRawDataAddStep
   * @param collector
   */
  def collectRawData(vmcRawDataAddStep: VmcRawDataAddStep, collector: Collector[JsonNode]) = {
    collector.collect(beanToJsonNode[VmcRawDataAddStep](vmcRawDataAddStep))
  }

  /**
   * 输出eventEnd
   * @param inputValue
   * @param context
   * @param collector
   */
  def collectEventEnd(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val stepIdStr: String = vmcRawDataStepIdListState.value()

    if(null == stepIdStr){
      logger.error(s"输出EventEnd error ! vmcRawDataStepIdListState 状态中为空！")
    }else {
      val stepIdList: List[String] = stepIdStr.split("\\|").toList
      stepIdList.foreach(stepId => {
        val vmcEventDataMatchControlPlan = toBean[VmcEventDataMatchControlPlan](inputValue)
        vmcEventDataMatchControlPlan.stepId = stepId.toLong
        vmcEventDataMatchControlPlan.indexCount = vmcRawDataIndexState.value()

        collector.collect(beanToJsonNode[VmcEventDataMatchControlPlan](vmcEventDataMatchControlPlan))

      })
    }
  }

  /**
   * 清理所有的状态变量
   */
  def clearAllState() = {

    logger.warn(s"clearAllState")

    // 记录当前stepId
    vmcRawDataCurrentStepIdState.clear()

    // 记录所有的stepId集合 ; 注意: 需要去重. 有多少个stepId 就需要发送多少个eventStart 和 evnetEnd
    vmcRawDataStepIdListState.clear()

    // 记录rawData的序号 index
    vmcRawDataIndexState.clear()

    // 记录eventStart
    vmcEventDataMatchControlPlanState.clear()

    // 记录匹配上的controlPlan 中配置了哪些sensor信息
    vmcControlPlanConfigSensorListState.clear()
  }

}
