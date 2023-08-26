package com.hzw.fdc.json

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.ErrorCode
import com.hzw.fdc.util.ExceptionInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger

import java.nio.charset.Charset


/**
 * @author gdj
 * @create 2020-08-18-15:39
 *
 */
object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  //支持解析成long类型
  mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true)

  val demo: JsonNode=null

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  /**
   * 转String
   * @param value
   * @return
   */
  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  /**
   * String 转换成 map
   * @param json
   * @param m
   * @tparam V
   * @return
   */
  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  /**
   * String 转换成 bean
   * @param json
   * @param m
   * @tparam T
   * @return
   */
  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)


  }

  /**
   * map 转换成 bean
   * @param map
   * @param m
   * @tparam M
   * @return
   */
  def fromMap[M](map: Map[String, Any])(implicit m: Manifest[M]): M = {
    mapper.convertValue(map)
  }


  /**
   * String 转换成 JsonNode
   * @param map
   * @param m
   * @tparam M
   * @return
   */
  def toJsonNode[M](str:String,logger:Logger): JsonNode= {
    try {
      mapper.readTree(str)
    } catch {
      case e:Exception =>logger.warn(ErrorCode("002008d001C", System.currentTimeMillis(), Map("data" -> str), ExceptionInfo.getExceptionInfo(e)).toJson)
        mapper.createObjectNode()
    }
  }

  /**
   * Byte 转换成 JsonNode
   * @param map
   * @param m
   * @tparam M
   * @return
   */
  def toJsonNode[M](record:ConsumerRecord[Array[Byte], Array[Byte]],logger:Logger): JsonNode= {
    try {
      val charset = Charset.forName("UTF-8")
      val str = new String(record.value(), charset)
      mapper.readTree(str)
    } catch {
      case e:Exception =>logger.warn(ErrorCode("002008d001C", System.currentTimeMillis(), Map("data" -> record), ExceptionInfo.getExceptionInfo(e)).toJson)
        mapper.createObjectNode()
    }
  }


  /**
   * JsonNode 转换成 javabean
   * @param jsonNode
   * @tparam M
   * @return
   */
  def toBean[M: Manifest](jsonNode:JsonNode): M= {
    mapper.convertValue[M](jsonNode)



  }

  /**
   * javabean 转换成 JsonNode
   * @param jsonNode
   * @tparam M
   * @return
   */
  def beanToJsonNode[M: Manifest](bean:M): JsonNode= {
    mapper.valueToTree[JsonNode](bean)

  }


  /**
   * String 转换成 JsonNode
   * @param map
   * @param m
   * @tparam M
   * @return
   */
  def toJsonNode[M](str:String): JsonNode= {

      mapper.readTree(str)

  }



}
