package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabVersionTagDao
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.function.online.MainFabRouter.MainFabRouterKeyedBroadcastProcessFunction
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.ErrorCode
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

/**
 *  功能: 打升级版本的标签
 */
class MainFabVersionTagService extends TService {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabVersionTagService])

  /**
   * 获取
   */
  override def getDao(): TDao = new MainFabVersionTagDao

  /**
   * 获取PT数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    //是否根据时间戳消费
    val DataStream = if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
      getDao().getKafkaJsonSourceByTimestamp(
        ProjectConfig.KAFKA_MAINFAB_VERSION_TAG_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_VERSION_TAG_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_ROUTER_JOB_VERSION_TAG_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_ROUTER_JOB_VERSION_TAG_KAFKA_SOURCE_UID,
        ProjectConfig.KAFKA_MAINFAB_ROUTER_JOB_FROM_TIMESTAMP)
    }else {
      getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_MAINFAB_VERSION_TAG_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_VERSION_TAG_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_ROUTER_JOB_VERSION_TAG_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_ROUTER_JOB_VERSION_TAG_KAFKA_SOURCE_UID)
    }
    DataStream
  }

  /**
   * 分析
   */
  override def analyses(): Any = {

    //升版本配置 广播流
    val UpDataStream = getDao().getKafkaJsonSource(
        ProjectConfig.KAFKA_UP_DATE_TOPIC,
        ProjectConfig.KAFKA_QUORUM,
        ProjectConfig.KAFKA_CONSUMER_GROUP_VERSION_TAG_JOB,
        MainFabConstants.latest,
        MainFabConstants.MAIN_FAB_ROUTER_JOB_UP_DATA_KAFKA_SOURCE_UID,
        MainFabConstants.MAIN_FAB_ROUTER_JOB_UP_DATA_KAFKA_SOURCE_UID)


    //注册广播变量
    val config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val routerConfigBroadcastDataStream = UpDataStream.broadcast(config)

    val addVsersionDataStream:DataStream[JsonNode] = getDatas()
      .filter(f => {
        try {
          val toolNameStatus = f.findPath(MainFabConstants.toolName).asText().isEmpty
          val chamberNameStatus = f.findPath(MainFabConstants.chamberName).asText().isEmpty
          //过滤dataType为空的
          if (toolNameStatus && chamberNameStatus) {
            logger.warn(ErrorCode("002003b009C", System.currentTimeMillis(), Map("data" -> f.asText()), "dataType is null").toJson)
          }
          !(toolNameStatus && chamberNameStatus)
        } catch {
          case e: Exception => logger.warn(s"Exception :${ExceptionInfo.getExceptionInfo(e)} data $f")
            false
        }
      })
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .connect(routerConfigBroadcastDataStream)
      .process(new MainFabRouterKeyedBroadcastProcessFunction)


    addVsersionDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC,
        elem => {
          elem.findPath(MainFabConstants.traceId).asText()
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaRouterSink)
      .uid(MainFabConstants.KafkaRouterSink)
  }


}

