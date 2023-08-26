package com.hzw.fdc.function.PublicFunction

import com.hzw.fdc.json.MarshallableImplicits._
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

/**
 * @author gdj
 * @create 2020-09-06-11:59
 * @param topic
 * @param f kafka分区的key的方法
 * @tparam IN
 */
class FdcJsonNodeSchema[IN](topic:String) extends KafkaSerializationSchema[IN] {
  override def serialize(element: IN, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {

    new ProducerRecord(topic, element.toJson.getBytes())

  }
}
