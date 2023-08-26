package com.hzw.fdc.function.online.MainFabWindow

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
class MainFabConfigDataSchema(dataType:String) extends KafkaDeserializationSchema[(String,String)] {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabConfigDataSchema])

  override def isEndOfStream(nextElement: (String,String)): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String,String) = {

    try {
      val charset = Charset.forName("UTF-8")
      val str = new String(record.value(), charset)
      (dataType,str)
    } catch {
      case ex:Exception => logger.warn(s"MainFabPTDataSchema error $ex data:$record ")
        null
    }
  }

  override def getProducedType: TypeInformation[(String,String)] = {
    TypeInformation.of(new TypeHint[(String,String)] {})
  }
}