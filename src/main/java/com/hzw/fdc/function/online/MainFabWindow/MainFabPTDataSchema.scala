package com.hzw.fdc.function.online.MainFabWindow

import com.hzw.fdc.json.MarshallableImplicits.Unmarshallable
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
class MainFabPTDataSchema extends KafkaDeserializationSchema[Map[String,Any]] {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabPTDataSchema])

  override def isEndOfStream(nextElement: Map[String,Any]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Map[String,Any] = {

    try {
      val charset = Charset.forName("UTF-8")
      val str = new String(record.value(), charset)
      str.fromJson[Map[String,Any]]
    } catch {
      case ex:Exception => logger.warn(s"MainFabPTDataSchema error $ex data:$record ")
        null
    }
  }

  override def getProducedType: TypeInformation[Map[String,Any]] = {
    TypeInformation.of(new TypeHint[Map[String,Any]] {})
  }
}