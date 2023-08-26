package com.hzw.fdc.function.PublicFunction

import com.hzw.fdc.json.MarshallableImplicits.Unmarshallable
import com.hzw.fdc.scalabean.{AlarmRuleResult, FdcData}
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
class AlarmHistoryKafkaDataSchema extends KafkaDeserializationSchema[String] {


  override def isEndOfStream(nextElement: String): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    val charset = Charset.forName("UTF-8")
    val str = new String(record.value(), charset)
    if (str.contains("dac_alarm_msg")) {
      ""
    } else {
      str
    }
  }

  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classTag[String].runtimeClass).asInstanceOf[TypeInformation[String]]
  }
}