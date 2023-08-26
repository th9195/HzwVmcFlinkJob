package com.hzw.fdc.function.PublicFunction

import java.nio.charset.Charset

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.reflect.classTag

/**
  * @Package com.hzw.fdc.function.PublicFunction
  * @author wanghb
  * @date 2022-09-01 16:12
  * @desc
  * @version V1.0
  */
class MainFabKafkaOriginalDataSchema extends KafkaDeserializationSchema[String]{

  override def isEndOfStream(nextElement: String): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    val charset = Charset.forName("UTF-8")
    val str = new String(record.value(), charset)
    str
  }

  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classTag[String].runtimeClass).asInstanceOf[TypeInformation[String]]
  }
}
