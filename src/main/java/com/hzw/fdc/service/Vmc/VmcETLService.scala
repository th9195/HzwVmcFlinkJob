package com.hzw.fdc.service.Vmc

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.Vmc.VmcETLDao
import com.hzw.fdc.function.online.vmc.etl.{VmcFilterToolBroadCastProcessFunction, VmcMatchControlPlanBroadCastProcessFunction}
import com.hzw.fdc.util.{ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowService
 */
class VmcETLService extends TService {
  private val dao = new VmcETLDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcETLService])




  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {

    //todo 1- 读业务数据流
    val vmcSourceDataTream: DataStream[JsonNode] = getDatas()

    // todo 读取维表数据
    val vmcControlPlanConfigDataStream = getDimDatas(ProjectConfig.KAFKA_VMC_CONTROLPLAN_CONFIG_TOPIC,
      VmcConstants.VMC_ETL_JOB_CONTROLPLAN_CONFIG_KAFKA_SOURCE_UID)

    val controlPlanDimConfigBroadcastDataStream = generaterBroadcastDataStream(vmcControlPlanConfigDataStream)

    val filterToolDataTream = vmcSourceDataTream.keyBy(inputValue => {
      val traceId = inputValue.get(VmcConstants.TRACE_ID).asText("-1")
      traceId
    }).connect(controlPlanDimConfigBroadcastDataStream)
      .process(new VmcFilterToolBroadCastProcessFunction)
      .name("vmc filter tool")
      .uid("vmc filter tool")

    filterToolDataTream.keyBy(inputValue => {
      val traceId = inputValue.get(VmcConstants.TRACE_ID).asText("-1")
      traceId
    }).connect(controlPlanDimConfigBroadcastDataStream)
      .process(new VmcMatchControlPlanBroadCastProcessFunction)
      .name("vmc match controlPlan")
      .uid("vmc match controlPlan")



  }

  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_DATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_VMC_ETL_JOB,
      VmcConstants.latest,
      VmcConstants.VMC_ETL_JOB_KAFKA_SOURCE_UID,
      VmcConstants.VMC_ETL_JOB_KAFKA_SOURCE_UID)

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
      ProjectConfig.KAFKA_CONSUMER_GROUP_VMC_ETL_JOB,
      VmcConstants.latest,
      name_uid,
      name_uid)

  }

  /**
   * 注册广播变量
   * @param dimConfigDataStream
   * @return
   */
  def generaterBroadcastDataStream(dimConfigDataStream: DataStream[JsonNode]): BroadcastStream[JsonNode] = {
    val config = new MapStateDescriptor[String, JsonNode](
      VmcConstants.CONFIG,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val broadcastDataStream: BroadcastStream[JsonNode] = dimConfigDataStream.broadcast(config)
    broadcastDataStream
  }


}
