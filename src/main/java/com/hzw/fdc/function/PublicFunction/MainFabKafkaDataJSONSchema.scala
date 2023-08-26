package com.hzw.fdc.function.PublicFunction


import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toJsonNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}


/**
 * @author gdj
 * @create 2021-04-19-13:35
 *
 */
class MainFabKafkaDataJSONSchema extends KafkaDeserializationSchema[JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabKafkaDataJSONSchema])

  override def isEndOfStream(nextElement: JsonNode): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): JsonNode = {

          toJsonNode(record,logger)

  }

  override def getProducedType: TypeInformation[JsonNode] = TypeInformation.of(classOf[JsonNode])
}