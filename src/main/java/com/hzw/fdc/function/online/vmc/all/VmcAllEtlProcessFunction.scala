package com.hzw.fdc.function.online.vmc.all

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.VmcBeans.{VmcEventData, VmcRawData}
import com.hzw.fdc.util.{ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}


class VmcAllEtlProcessFunction() extends KeyedProcessFunction[String, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllEtlProcessFunction])

  private var eventStartState:ValueState[VmcEventData] = _

  private var earlyRawDataMapState :MapState[Long,JsonNode]=_

  // 缓存eventEnd :  用于 eventEnd 比 eventStart 早到, 先缓存eventEnd
  private var earlyEventEndState:ValueState[JsonNode] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 26小时过期
    val hour26TTLConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    //   初始化状态变量 processEndEventStartState
    val processEndEventStartStateDescription = new
        ValueStateDescriptor[VmcEventData]("processEndEventStartState", TypeInformation.of(classOf[VmcEventData]))
    // 设置过期时间
    processEndEventStartStateDescription.enableTimeToLive(hour26TTLConfig)
    eventStartState = getRuntimeContext.getState(processEndEventStartStateDescription)

    // 初始化 earlyRawDataMapState
    val earlyRawDataMapStateDescription: MapStateDescriptor[Long,JsonNode] = new
        MapStateDescriptor[Long,JsonNode]("earlyRawDataMapState", TypeInformation.of(classOf[Long]),TypeInformation.of(classOf[JsonNode]))
    // 设置过期时间
    earlyRawDataMapStateDescription.enableTimeToLive(hour26TTLConfig)
    earlyRawDataMapState = getRuntimeContext.getMapState(earlyRawDataMapStateDescription)

    //   初始化状态变量 processEndEventEndState
    val earlyEventEndStateDescription = new
        ValueStateDescriptor[JsonNode]("earlyEventEndState", TypeInformation.of(classOf[JsonNode]))
    // 设置过期时间
    earlyEventEndStateDescription.enableTimeToLive(hour26TTLConfig)
    earlyEventEndState = getRuntimeContext.getState(earlyEventEndStateDescription)

  }

  override def processElement(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType= inputValue.findPath(VmcConstants.DATA_TYPE).asText()
      val traceId= inputValue.findPath(VmcConstants.TRACE_ID).asText()
      val toolName= inputValue.findPath(VmcConstants.TOOL_NAME).asText()
      val chamberName= inputValue.findPath(VmcConstants.CHAMBER_NAME).asText()

      if(!dataType.isEmpty &&
      !traceId.isEmpty &&
      !toolName.isEmpty &&
      !chamberName.isEmpty){
        if(dataType == VmcConstants.EVENT_START){

          // 处理eventStart
          processEventStart(inputValue,context,collector)

          // 处理早到的rawData
          processEarlyRawData(context,collector)

          // 处理早到的eventEnd
          processEarlyEventEnd(context,collector)

        }else if (dataType == VmcConstants.EVENT_END){

          // 处理eventEnd
          processEventEnd(inputValue,context,collector)

        }else if (dataType == VmcConstants.RAWDATA){

          // 处理eventEnd
          processRawData(inputValue,context,collector)
        }
      }else{
        logger.error(s"该数据基本信息有误！\n" +
          s"dataType = ${dataType} \n " +
          s"traceId = ${traceId} \n " +
          s"toolName = ${toolName} \n " +
          s"chamberName = ${chamberName}")
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
    // 缓存状态
    eventStartState.update(eventStartData)
    collector.collect(inputValue)
  }

  def processEventEnd(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    val runEventStartData = eventStartState.value()
    if(null == runEventStartData){
      collector.collect(inputValue)
      clearAllState()
    }else{
      // 缓存早到的 eventEnd
      earlyEventEndState.update(inputValue)
    }
  }

  def processRawData(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    val runEventStartData = eventStartState.value()
    if(null != runEventStartData){
      collector.collect(inputValue)
    }else{
      val rawData = toBean[VmcRawData](inputValue)
      val timestamp = rawData.timestamp
      earlyRawDataMapState.put(timestamp,inputValue)
    }
  }

  def processEarlyRawData(context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,collector: Collector[JsonNode]) = {
    var count: Int = 0
    val currentKey: String = context.getCurrentKey
    if(null != earlyRawDataMapState.values()){
      val it = earlyRawDataMapState.values().iterator()
      while(it.hasNext){
        val inputValue: JsonNode = it.next()
        processRawData(inputValue,context,collector)
        count += 1
      }
      if(count > 0){
        logger.warn(s"处理早到的RawData 数据: traceId == ${currentKey} ; count == ${count} ")
      }
    }
  }

  def processEarlyEventEnd(context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,collector: Collector[JsonNode]): Unit = {
    val runEventEndData = earlyEventEndState.value()
    val currentKey = context.getCurrentKey
    if(null != runEventEndData){
      logger.warn(s"处理早到的eventEnd 数据: traceId == ${currentKey}")
      processEventEnd(runEventEndData,context,collector)
    }
  }

  def clearAllState() = {
    eventStartState.clear()
    earlyRawDataMapState.clear()
    earlyEventEndState.clear()
  }
}
