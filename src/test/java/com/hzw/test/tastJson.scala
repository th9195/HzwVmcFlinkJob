package com.hzw.test

import com.hzw.fdc.json.JsonUtil.{toBean, toJsonNode}
import com.hzw.fdc.scalabean.{MainFabPTRawData, PTSensorData}

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

/**
 * @author ：gdj
 * @date ：2022/1/13 11:28
 * @param $params
 * @return $returns
 */
case class TestAl(var test: String, tt: String)


object tastJson {


  var indicatorRuleByStage = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, TestAl]]]

  def main(args: Array[String]): Unit = {
    val str = "{\n\t\"svid\": \"CMPTool_69\",\n\t\"sensorName\": \"YMTC_chamber\",\n\t\"sensorAlias\": \"recipe69\",\n\t\"isVirtualSensor\": 1603,\n\t\"sensorValue\": null ,\n\t\"unit\": 33\n}"
    val j=toJsonNode(str)
    val ss=toBean[PTSensorData](j)
    println()

    val a_LIst = ListBuffer("12", "23", "45")
    val b_list = List("33", "555")
    a_LIst ++= b_list

    println(a_LIst)

    val tt = concurrent.TrieMap[String, TestAl]("tt" -> TestAl("123", "567"))
    val rr = concurrent.TrieMap[String, concurrent.TrieMap[String, TestAl]]("444" -> tt)
    indicatorRuleByStage.put("000", rr)

    val fff = indicatorRuleByStage("000")
    val ssss = fff("444")
    val ssaaa = ssss("tt")
    ssaaa.test = "ffff"

    println(indicatorRuleByStage)

  }

}
