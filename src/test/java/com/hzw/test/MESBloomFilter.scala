//package com.hzw.test
//
//import com.fasterxml.jackson.databind.JsonNode
//import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
//import com.hzw.fdc.scalabean.{FdcData, MESMessage, toolMessage}
//import com.hzw.fdc.util.MainFabConstants
//import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
//
///**
// * @author ：gdj
// * @date ：2021/12/20 17:14
// * @param $params
// * @return $returns
// */
//class MESBloomFilter {
//
//  private var toolMessageBloomFilter: (Long, BloomFilter[String]) = _
//  private var MESMessageBloomFilter: (Long, BloomFilter[String]) = _
//
//  def main(data: JsonNode): Unit = {
//
//
//    //布隆过滤器一小时清理一次 为了DAC功能
//    val nowTime = System.currentTimeMillis()
//    if (toolMessageBloomFilter == null) {
//      toolMessageBloomFilter =((System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 1000000)))
//    } else if (nowTime - toolMessageBloomFilter._1 >= 1 * 60 * 60 * 1000) {
//      toolMessageBloomFilter=((System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 1000000)))
//    }
//
//    if (MESMessageBloomFilter == null) {
//      MESMessageBloomFilter=((System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 100)))
//    } else if (nowTime - MESMessageBloomFilter._1 >= 1 * 60 * 60 * 1000) {
//      MESMessageBloomFilter=((System.currentTimeMillis(), BloomFilter.create(Funnels.unencodedCharsFunnel, 100)))
//    }
//
//
//    if(data.findPath(MainFabConstants.dataType).asText().equals(MainFabConstants.toolMessage)){
//      val myToolMessage = toBean[FdcData[toolMessage]](data).datas
//      val sensorList = myToolMessage.sensors.filter(Sensor => {
//        val mightData = s"${myToolMessage.locationName}|${myToolMessage.moduleName}|${myToolMessage.toolName}|${myToolMessage.chamberName}|${myToolMessage.recipeNames}|$Sensor"
//        val isRepeat = toolMessageBloomFilter._2.mightContain(mightData)
//        if (!isRepeat) {
//          toolMessageBloomFilter._2.put(mightData)
//        }
//        !isRepeat
//      })
//
//      if (sensorList.nonEmpty) {
//        val node = beanToJsonNode[toolMessage](toolMessage(locationName = myToolMessage.locationName,
//          moduleName = myToolMessage.moduleName,
//          toolName = myToolMessage.toolName,
//          chamberName = myToolMessage.chamberName,
//          sensors = sensorList,
//          recipeNames = myToolMessage.recipeNames
//        ))
//        println(node)
//      }
//    }else if(data.findPath(MainFabConstants.dataType).asText().equals(MainFabConstants.MESMessage)){
//      val myMESMessage = toBean[FdcData[MESMessage]](data)
//      if (!MESMessageBloomFilter._2.mightContain(myMESMessage.datas.toString)) {
//        MESMessageBloomFilter._2.put(myMESMessage.datas.toString)
//        val node = beanToJsonNode[MESMessage](myMESMessage.datas)
//        println(node)
//      }
//    }
//
//    println("")
//  }
//
//}
