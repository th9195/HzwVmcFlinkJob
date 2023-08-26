package com.hzw.fdc.json

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.hzw.fdc.json.JsonUtil.mapper

/**
 * @author gdj
 * @create 2020-07-24-14:52
 *
 */
object JacksonMapper {
  private var s:ScalaObjectMapper = null
  def getInstance = {
    if(s == null) {
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
      s = mapper
    }
    s}

}



