package com.hzw.fdc.function.PublicFunction



import com.hzw.fdc.json.MarshallableImplicits.Unmarshallable
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.Charset
import scala.reflect.classTag

/**
 * @author gdj
 * @create 2021-04-19-13:35
 *
 */
class MainFabKafkaDataSchema[T : TypeInformation](implicit m: Manifest[T]) extends KafkaDeserializationSchema[T] {


  override def isEndOfStream(nextElement: T): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): T = {
        val charset = Charset.forName("UTF-8")
        val str = new String(record.value(), charset)
            str.fromJson[T]
  }

  override def getProducedType: TypeInformation[T] = {
    TypeInformation.of(classTag[T].runtimeClass).asInstanceOf[TypeInformation[T]]
  }
}