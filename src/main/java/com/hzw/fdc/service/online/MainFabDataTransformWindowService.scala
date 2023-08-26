package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.{MainFabDataTransformWindowDao, MainFabWindowDao}
import com.hzw.fdc.function.PublicFunction.{FdcJsonNodeSchema, FdcKafkaSchema}
import com.hzw.fdc.function.online.MainFabVirtualSensor.{MainFabAddVirtualSensorKeyedBroadcastProcessFunction, MainFabVirtualSensorKeyedBroadcastProcessFunction}
import com.hzw.fdc.function.online.MainFabWindow._
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 *
 * @desc: 主要有三大功能：
 *         1： 计算虚拟sensor
 *         2： 计算run信息
 *         3： 输出rawdata信息
 * @author tobytang
 * @date 2022/11/5 11:21
 * @since 1.0.0
 * */
class MainFabDataTransformWindowService extends TService {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabDataTransformWindowService])
  lazy val MESOutput = new OutputTag[JsonNode]("toolMessage")
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  private val Dao = new MainFabDataTransformWindowDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = Dao

  /**
   * 处理数据
   *
   * @return
   */
  override def analyses(): Any = {

    // TODO 1- 读取源数据: TRANSFORM 数据
    val PTTransformDataStream_all = getDatas()


    val ifConsumerByTime = ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP
    val list = new ListBuffer[String]

    // 是否要消费历史数据，指定特殊的Tool，以及时间段
    val PTTransformDataStream = if(ifConsumerByTime){
      val filterKafkaEndTime: String = ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP_ENDTIME
      val filterKafkaTool: String = ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP_TOOLS

      try {
        if (!filterKafkaTool.isEmpty) {
          filterKafkaTool.split(",").foreach(x => {
            list.append(x)
          })
        }
        //开启指定时间段(指定tool)消费kafka数据
        logger.warn("指定的开始消费时间为:" + ProjectConfig.KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_JOB_FROM_TIMESTAMP)
        logger.warn("指定的终止消费时间为:" + filterKafkaEndTime)
        logger.warn(s"指定消费的tool为: $filterKafkaTool \t $list")
      }catch {
        case e :Exception=> logger.warn(s"filterKafkaTool Exception :${ExceptionInfo.getExceptionInfo(e)}")
      }

      PTTransformDataStream_all.filter(x =>{
        try {
          if (ifConsumerByTime) {
            if (filterKafkaTool.isEmpty) {
              //tool为空,消费所有在指定时间以前的数据
              if (!x.findPath(MainFabConstants.dataType).asText().isEmpty) {
                val dataType: String = x.findPath(MainFabConstants.dataType).asText()
                if(dataType.equals("rawData")){
                  val timestamp = x.findPath(MainFabConstants.timestamp).asText().toLong
                  if(timestamp <= filterKafkaEndTime.toLong){
                    true
                  }else{
                    false
                  }
                }else if(dataType.equals("eventStart")){
                  val runStartTime = x.findPath(MainFabConstants.runStartTime).asText().toLong
                  if(runStartTime <= filterKafkaEndTime.toLong){
                    true
                  }else{
                    false
                  }
                }else if(dataType.equals("eventEnd")){
                  val runEndTime = x.findPath(MainFabConstants.runEndTime).asText().toLong
                  if(runEndTime <= filterKafkaEndTime.toLong){
                    true
                  }else{
                    false
                  }
                } else{
                  true
                }
              } else {
                true
              }
            } else {
              //tool不为空,只消费特定的tool在指定时间以前的数据
              if (!x.findPath(MainFabConstants.dataType).asText().isEmpty) {
                val dataType: String = x.findPath(MainFabConstants.dataType).asText()
                val toolName: String = x.findPath(MainFabConstants.toolName).asText()
                if(dataType.equals("rawData")){
                  val timestamp = x.findPath(MainFabConstants.timestamp).asText().toLong
                  if(list.contains(toolName) && timestamp <= filterKafkaEndTime.toLong ){
                    true
                  }else{
                    false
                  }
                }else if(dataType.equals("eventStart")){
                  val runStartTime = x.findPath(MainFabConstants.runStartTime).asText().toLong
                  if(list.contains(toolName) && runStartTime <= filterKafkaEndTime.toLong){
                    true
                  }else{
                    false
                  }
                }else if(dataType.equals("eventEnd")){
                  val runEndTime = x.findPath(MainFabConstants.runEndTime).asText().toLong
                  if(list.contains(toolName) && runEndTime <= filterKafkaEndTime.toLong){
                    true
                  }else{
                    false
                  }
                } else{
                  true
                }
              } else {
                true
              }
            }
          } else {
            true
          }
        } catch {
          case e :Exception=> logger.warn(s"Exception :${ExceptionInfo.getExceptionInfo(e)} data $x")
            false
        }
      })
    }else{
      // 不做任何特殊处理, 从topic消费的原始数据输出
      PTTransformDataStream_all
    }



    // todo 2- 读取维表数据
    // 维表: PM状态数据
    val PMStatusDataStream = getDimDatas(ProjectConfig.KAFKA_PM_TOPIC,
      MainFabConstants.MAIN_FAB_PM_KAFKA_SOURCE_UID)

    // 注册广播变量
    val pMStatusDataMapStateDescriptor= new MapStateDescriptor[String, JsonNode](
      "PMStatusData",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val pmConfigBroadcastDataDimStream = PMStatusDataStream.broadcast(pMStatusDataMapStateDescriptor)

    // 维表6: 读取virtualSensorConfig配置
    val virtualSensorConfigDataStream = getDimDatas(ProjectConfig.KAFKA_VIRTUAL_SENSOR_CONFIG_TOPIC,
      MainFabConstants.MAIN_FAB_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID)

    // 注册广播变量
    val virtualSensorConfigMapStateDescriptor = new MapStateDescriptor[String,JsonNode](
      MainFabConstants.virtualSensorConfig,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val virtualSensorConfigBroadcastDataDimStream = virtualSensorConfigDataStream.broadcast(virtualSensorConfigMapStateDescriptor)


    //todo 4- 和PM状态数据merge的数据
//    val PTUnionPMStatusDataStream: DataStream[JsonNode] = PTTransformDataStream
//      .filter(ptData => {
//        try {
//          val toolNameStatus = ptData.findPath(MainFabConstants.toolName).asText().isEmpty
//          val chamberNameStatus = ptData.findPath(MainFabConstants.chamberName).asText().isEmpty
//          //过滤dataType为空的
//          if (toolNameStatus && chamberNameStatus) {
//            logger.warn(ErrorCode("0220030001C", System.currentTimeMillis(), Map("data" -> ptData.asText()), "dataType is null").toJson)
//          }
//          !(toolNameStatus && chamberNameStatus) && ptData.has(MainFabConstants.traceId) && !ptData.path(MainFabConstants.traceId).isNull
//        } catch {
//          case e: Exception => logger.warn(s"Exception :${ExceptionInfo.getExceptionInfo(e)} data $ptData")
//            false
//        }
//      })
//      .keyBy(data => {
//        data.findPath(MainFabConstants.traceId).asText()
//      })
//      .connect(pmConfigBroadcastDataDimStream)
//      .process(new MainFabPMKeyedProcessFunction)
//      .name("pm")
//      .uid("pm")

//    val mainFabLogInfo_01: DataStream[MainFabLogInfo] = PTUnionPMStatusDataStream.getSideOutput(mainFabLogInfoOutput)

    //todo 5- 添加 virtual sensor
    val virtualSensorDataStream: DataStream[JsonNode] = PTTransformDataStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .connect(virtualSensorConfigBroadcastDataDimStream)
      .process(new MainFabAddVirtualSensorKeyedBroadcastProcessFunction)   // 计算虚拟sensor
      .uid("add virtual sensor")
      .name("add virtual sensor")

    val mainFabLogInfo_02 = virtualSensorDataStream.getSideOutput(mainFabLogInfoOutput)

    // todo 6- 补全stepId
    val SensorDataStream = virtualSensorDataStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .process(new MainFabWindowAddStepProcessFunction(true)) // 补全stepId
      .filter(_.nonEmpty)
      .map(_.get)
      .name("run addStep")
      .uid("run addStep")

    // todo 6- 处理 runData和 mes信息
    val processEndRunDataStream: DataStream[FdcData[RunData]] = SensorDataStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .process(new MainFabProcessRunFunction)
      .name("run Task")
      .uid("run Task")

    val mainFabLogInfo_03 =  processEndRunDataStream.getSideOutput(mainFabLogInfoOutput)

    //todo 6-1 Sink MES数据
    val MESDataStream = processEndRunDataStream.getSideOutput(MESOutput)
    //ToolMessage 写到 kafka
    MESDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC,
      new FdcJsonNodeSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("DataTransform window sink toolMessage to kafka")
      .uid("DataTransform window sink toolMessage to kafka")


    //todo 6-2 Sink processEnd Rundata 写入 kafka
    processEndRunDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
      new FdcKafkaSchema[FdcData[RunData]](ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
        elem => {elem.datas.traceId}
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("DataTransform window sink RunData to kafka")
      .uid("DataTransform window sink RunData to kafka")


    // 7- 计算完virtualSensor的数据 添加 stepId stepName  (isCacheFailSensor=false)
    val virtualSensorKeyByDataStream = virtualSensorDataStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .process(new MainFabWindowAddStepProcessFunction(false))
      .filter(_.nonEmpty)
      .map(_.get)
      .name("virtualSensor addStep")
      .uid("virtualSensor addStep")

    //todo 8 raw data写 kafka
    virtualSensorKeyByDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC,
        elem => {elem.findPath(MainFabConstants.traceId).asText()}
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("DataTransform window sink rawData to kafka")
      .uid("DataTransform window sink rawData to kafka")

    //todo 9 raw data写 kafka 用于切窗口
    virtualSensorKeyByDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_WINDOW_RAWDATA_TOPIC,
        elem => {elem.findPath(MainFabConstants.traceId).asText()}
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("rawData sink to kafka by window")
      .uid("rawData sink to kafka by window")


    // todo 10 日志信息写入kafka
//    val mainFabLogInfos: DataStream[MainFabLogInfo] = mainFabLogInfo_01.union(mainFabLogInfo_02, mainFabLogInfo_03)
    val mainFabLogInfos: DataStream[MainFabLogInfo] = mainFabLogInfo_02.union(mainFabLogInfo_03)
    mainFabLogInfos.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_LOGINFO_TOPIC,
      new FdcKafkaSchema[MainFabLogInfo](ProjectConfig.KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_LOGINFO_TOPIC
        , (mainFabLogInfo: MainFabLogInfo) => mainFabLogInfo.mainFabDebugCode
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("DataTransform window sink MainFabLogInfo to kafka")
      .uid("DataTransform window sink MainFabLogInfo to kafka")

  }


  /**
   * 获取PT数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {


    if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      val timestamp = ProjectConfig.KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_JOB_FROM_TIMESTAMP

      getDao.getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_DATA_TRANSFORM_WINDOW_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_PT_DATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_PT_DATA_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_DATA_TRANSFORM_WINDOW_JOB_FROM_TIMESTAMP)
    }else{

      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_DATA_TRANSFORM_WINDOW_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_PT_DATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_DATA_TRANSFORM_WINDOW_JOB_PT_DATA_KAFKA_SOURCE_UID)
    }


  }



  /**
   * 根据topic + 时间戳 获取实时的维表数据
   * @param topic
   * @return
   */
  def getDimDatas(topic:String,name_uid:String):DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      topic,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_DATA_TRANSFORM_WINDOW_JOB,
      MainFabConstants.latest,
      name_uid,
      name_uid)
  }

  /**
    * 截取traceId中的时间戳
    */
  def splitDate(s:String): Long ={
    val str: String = s.split("--")(1)
    val dateStr = str.substring(0,4) + "-" +str.substring(4,6) + "-" +str.substring(6,8) + " " + str.substring(8,10) + ":" + str.substring(10,12) + ":" + str.substring(12,14) + "." + str.substring(14,17)
    val endTime = DateTimeUtil.getTimestampByTime(dateStr,"yyyy-MM-dd HH:mm:ss","ms")
    endTime
  }

}
