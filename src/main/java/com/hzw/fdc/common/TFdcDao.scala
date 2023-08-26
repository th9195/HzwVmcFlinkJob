package com.hzw.fdc.common

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.PublicFunction.{AlarmHistoryKafkaDataSchema, MainFabKafkaDataJSONSchema, MainFabKafkaDataSchema, MainFabKafkaOriginalDataSchema}
import com.hzw.fdc.function.online.MainFabWindow.MainFabConfigDataSchema
import com.hzw.fdc.util.{FdcFlinkStreamEnv, FdcProjectConfig, MainFabConstants}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * TFdcDao
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/14 13:32
 * @since 1.0.0
 * @update 2022/12/14 13:32
 * */
trait TFdcDao extends Serializable {
  /**
   * 读取文件
   */
  def readTextFile(implicit path: String) = {
    FdcFlinkStreamEnv.get().readTextFile(path)
  }


  /**
   * 读取MainFabPTKafka数据
   */
  def readMainFabWindowConfigKafka(dataType:String,topics: String, ip: String, consumerGroup: String,autoOffsetReset:String,name:String,uid:String): DataStream[(String,String)] = {

    val kafkaSource = new FlinkKafkaConsumer[(String,String)](
      topics,
      new MainFabConfigDataSchema(dataType),
      FdcProjectConfig.getKafkaConsumerProperties(ip,consumerGroup,autoOffsetReset)
    )

    //设置uid用于监控
    FdcFlinkStreamEnv.get().addSource(kafkaSource).name(name).uid(uid)

  }

  /**
   * 读取kafka数据,如果解析错误返回null
   * @param topic
   * @param ip
   * @param consumer_group
   * @tparam T
   * @return
   */
  def getKafkaSource[T :  TypeInformation](topic: String, ip: String, consumerGroup: String,autoOffsetReset:String,name:String,uid:String)(implicit m: Manifest[T]) = {

    // 从kafka中获取数据
    FdcFlinkStreamEnv.get()
      .addSource[T](
        new FlinkKafkaConsumer[T](
          topic,
          new MainFabKafkaDataSchema[T](),
          FdcProjectConfig.getKafkaConsumerProperties(ip,consumerGroup,autoOffsetReset)))
      .name(name)
      .uid(uid)
    //过滤解析错误的数据

  }

  //kafka中获取原始数据
  def getKafkaOriginalSource(topic: String, ip: String, consumerGroup: String,autoOffsetReset:String,name:String,uid:String) = {

    // 从kafka中获取数据
    FdcFlinkStreamEnv.get()
      .addSource[String](
        new FlinkKafkaConsumer[String](
          topic,
          new MainFabKafkaOriginalDataSchema(),
          FdcProjectConfig.getKafkaConsumerProperties(ip,consumerGroup,autoOffsetReset)))
      .name(name)
      .uid(uid)
    //过滤解析错误的数据

  }

  def getAlarmHistoryKafkaSource(topic: String, ip: String, consumerGroup: String,autoOffsetReset:String,name:String,uid:String) = {

    // 从kafka中获取数据
    FdcFlinkStreamEnv.get()
      .addSource[String](
        new FlinkKafkaConsumer[String](
          topic,
          new AlarmHistoryKafkaDataSchema(),
          FdcProjectConfig.getKafkaConsumerProperties(ip,consumerGroup,autoOffsetReset)))
      .name(name)
      .uid(uid)
    //过滤解析错误的数据

  }

  /**
   * 读取kafka数据,如果解析错误返回空JsonNode
   * @param topic
   * @param ip
   * @param consumerGroup
   * @param autoOffsetReset 第一次新加的消费者，从头消费还是从当前消费
   * @param name
   * @param uid
   * @return
   */
  def getKafkaJsonSource(topic: String, ip: String, consumerGroup: String,autoOffsetReset:String,name:String,uid:String) = {

    // 从kafka中获取数据
    FdcFlinkStreamEnv.get()
      .addSource(
        new FlinkKafkaConsumer(
          topic,
          new MainFabKafkaDataJSONSchema(),
          FdcProjectConfig.getKafkaConsumerProperties(ip,consumerGroup,autoOffsetReset)))
      .filter(x=> !x.isNull)
      .name(name)
      .uid(uid)
    //过滤解析错误的数据

  }


  /**
   * 根于TimeStamp 从kafka中获取数据
   * @param topic
   * @param ip
   * @param consumerGroup
   * @param autoOffsetReset
   * @param name
   * @param uid
   * @param offset_timestamp
   * @return
   */
  def getKafkaJsonSourceByTimestamp(topic: String,
                                    ip: String,
                                    consumerGroup: String,
                                    autoOffsetReset:String,
                                    name:String,
                                    uid:String,
                                    offset_timestamp:Long) = {

    // 1- kafka 参数设置
    val properties: Properties = FdcProjectConfig.getKafkaConsumerProperties(ip, consumerGroup, autoOffsetReset)

    // 2- 创建 FlinkKafkaConsumer
    val kafkaConsumer = new FlinkKafkaConsumer[JsonNode](
      topic,
      new MainFabKafkaDataJSONSchema(),
      properties
    ).setStartFromTimestamp(offset_timestamp)

    // 3- 获取数据
    FdcFlinkStreamEnv.get()
      .addSource(kafkaConsumer)
      .filter(x=> !x.isNull)
      .name(name)
      .uid(uid)
  }




  /**
   * 读取kafka数据, 注册广播变量，通常用来读取配置
   * @param topic
   * @param ip
   * @param consumer_group
   * @param name
   * @param uid
   * @return 广播流
   */
  def getKafkaBroadcastConfigJsonSource(topic: String, ip: String, consumerGroup: String,autoOffsetReset:String,name:String,uid:String):BroadcastStream[JsonNode] = {

    // 从kafka中获取数据
    val ConfigDataStream= FdcFlinkStreamEnv.get()
      .addSource(
        new FlinkKafkaConsumer(
          topic,
          new MainFabKafkaDataJSONSchema(),
          FdcProjectConfig.getKafkaConsumerProperties(ip,consumerGroup,autoOffsetReset)))
      .filter(x=> !x.isNull)
      .name(name)
      .uid(uid)
    //过滤解析错误的数据

    //注册广播变量
    val config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val ConfigBroadcastDataStream = ConfigDataStream.broadcast(config)

    //返回广播流
    ConfigBroadcastDataStream
  }

  /**
   * 读取Socket网络数据
   */
  def readSocket() = {
    FdcFlinkStreamEnv.get().socketTextStream("fdc01", 9999)
  }


}
