package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.json.MarshallableImplicits.Unmarshallable
import com.hzw.fdc.scalabean.kafkaIndicatorResult
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset

/**
 * @author gdj
 * @create 2021-04-19-13:35
 *
 */
class MainFabIndicatorSchema extends KafkaDeserializationSchema[kafkaIndicatorResult] {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabIndicatorSchema])

  override def isEndOfStream(nextElement: kafkaIndicatorResult): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): kafkaIndicatorResult = {

    try {
      val charset = Charset.forName("UTF-8")
      val str = new String(record.value(), charset)
      str.fromJson[kafkaIndicatorResult]
    } catch {
      case ex:Exception => logger.warn(s"MainFabIndicatorSchema error $ex data:$record ")
        null
    }
  }

  override def getProducedType: TypeInformation[kafkaIndicatorResult] = {
    TypeInformation.of(new TypeHint[kafkaIndicatorResult] {})
  }
}