package com.hzw.fdc.service.Vmc

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.Vmc.VmcAllDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.vmc.all.{VmcAllAddStepProcessFunction, VmcAllEtlProcessFunction, VmcAllMatchControlPlanProcessFunction}
import com.hzw.fdc.function.online.vmc.etl.{VmcFilterToolBroadCastProcessFunction, VmcMatchControlPlanBroadCastProcessFunction}
import com.hzw.fdc.util.{ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}


/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWindowService
 */
class VmcAllService extends TService {
  private val dao = new VmcAllDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllService])




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


    /**
     * 1- 过滤出正常的数据
     * 2- 顺序矫正 保证下游的数据顺序是 eventStart -> rawData(n条) -> eventEnd
     *  处理比eventStart早到的rawData
     *  处理比eventStart早到的eventEnd
     * 3- 注意: 如果rawData比 eventStart eventEnd都晚到，这样的rawData无法矫正
     *  因为有了eventStart 和 eventEnd 就表示一个Run结束了
     */
    val etlDataTream = vmcSourceDataTream.keyBy(inputValue => {
      val traceId = inputValue.findPath(VmcConstants.TRACE_ID).asText("-1")
      traceId
    }).process(new VmcAllEtlProcessFunction)
      .name("vmc all etl")
      .uid("vmc all etl")

    if(ProjectConfig.VMC_ETL_DEBUG_EANBLE){
      etlDataTream.addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_VMC_ETL_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_VMC_ETL_TOPIC
          //按照tool分区
          , (elem: JsonNode) => {
            val traceId = elem.findPath(VmcConstants.TRACE_ID).asText("-1")
            traceId
          }
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("etlDataTream dataStream sink to kafka")
        //添加uid用于监控
        .uid("etlDataTream dataStream sink to kafka")
    }


    /**
     * 1- 从oracle 中读取策略信息
     * 2- 校验策略信息
     * 3- 匹配controlplan
     * 4- run 的数据 与 controlPlan 是一对多的关系
     *    一个run 可以匹配到多个controlPlan
     * 5- 如果一个run匹配了多个contorlplan, 整个run 会copy N倍
     */
    val matchedControlPlanDataTream = etlDataTream.keyBy(inputValue => {
      val traceId = inputValue.findPath(VmcConstants.TRACE_ID).asText("-1")
      traceId
    }).process(new VmcAllMatchControlPlanProcessFunction)
      .name("vmc all match controlplan")
      .uid("vmc all match controlplan")

    if(ProjectConfig.VMC_MATCH_CONTROLPLAN_DEBUG_EANBLE){
      matchedControlPlanDataTream.addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_VMC_MATCH_CONTROLPLAN_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_VMC_MATCH_CONTROLPLAN_TOPIC
          //按照tool分区
          , (elem: JsonNode) => {
            val traceId = elem.findPath(VmcConstants.TRACE_ID).asText("-1")
            val controlPlanId = elem.findPath(VmcConstants.CONTROLPLAN_ID).asText("-1")
            traceId + controlPlanId
          }
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("matchedControlPlanDataTream dataStream sink to kafka")
        //添加uid用于监控
        .uid("matchedControlPlanDataTream ProcessEnd fdcWindowData sink")
    }

    /**
     * 1- keyby traceId + controlPlanId  : 因为一个run匹配上了多个controlPlan ,数据copy份;
     * 2- 给每个rawData 数据添加stepId stepName
     * 3- 给每个rawData 数据添加index编号
     * 4- rawData中的sensorList 信息只保留controlPlanConfig中使用到的sensorAlias
     * 5- 讲一个Run 拆分成多份数据：以stepId为单位
     * 6- 在eventStart/eventEnd中添加stepId字段
     */
    val addStepDataTream = matchedControlPlanDataTream.keyBy(inputValue => {
      val traceId = inputValue.findPath(VmcConstants.TRACE_ID).asText("-1")
      val controlPlanId = inputValue.findPath(VmcConstants.CONTROLPLAN_ID).asText("-1")
      traceId + controlPlanId
    }).process(new VmcAllAddStepProcessFunction)
      .name("vmc all add step index ")
      .uid("vmc all add step index ")


    if(ProjectConfig.VMC_ADD_STEP_DEBUG_EANBLE){
      addStepDataTream.addSink(new FlinkKafkaProducer(
        ProjectConfig.KAFKA_VMC_ADD_STEP_TOPIC,
        new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_VMC_ADD_STEP_TOPIC
          //按照tool分区
          , (elem: JsonNode) => {
            val traceId = elem.findPath(VmcConstants.TRACE_ID).asText("-1")
            val controlPlanId = elem.findPath(VmcConstants.CONTROLPLAN_ID).asText("-1")
            val stepId = elem.findPath(VmcConstants.STEP_ID).asText("-1")
            traceId + controlPlanId + stepId
          }
        )
        , ProjectConfig.getKafkaProperties()
        , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )).name("addStepDataTream dataStream sink to kafka")
        //添加uid用于监控
        .uid("addStepDataTream ProcessEnd fdcWindowData sink")
    }




  }

  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_VMC_DATA_TOPIC,
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
      ProjectConfig.KAFKA_CONSUMER_GROUP_VMC_ALL_JOB,
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
