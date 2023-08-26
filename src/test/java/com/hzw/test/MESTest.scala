//package com.hzw.test
//
//import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, fromJson, toJsonNode}
//import com.hzw.fdc.scalabean.{FdcData, MESMessage, toolMessage}
//import com.hzw.fdc.util.MainFabConstants
//
///**
// * @author ：gdj
// * @date ：2021/12/21 10:02
// * @param $params
// * @return $returns
// */
//object MESTest {
//  def main(args: Array[String]): Unit = {
//    val tm0="{\n\t\"locationName\": \"FAB\",\n\t\"moduleName\": \"FUR\",\n\t\"toolName\": \"TTOOL_Thread-0\",\n\t\"chamberName\": \"TUBE\",\n\t\"sensors\": [{\n\t\t\"svid\": \"4943\",\n\t\t\"sensorAlias\": \"TempSpikeZone4\",\n\t\t\"sensorName\": \"TempSpikeZone4\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"5513\",\n\t\t\"sensorAlias\": \"TempSpikeZone3\",\n\t\t\"sensorName\": \"TempSpikeZone3\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"0518\",\n\t\t\"sensorAlias\": \"TempSpikeZone6\",\n\t\t\"sensorName\": \"TempSpikeZone6\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"1824\",\n\t\t\"sensorAlias\": \"TempPaddleZone5\",\n\t\t\"sensorName\": \"TempPaddleZone5\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"2598\",\n\t\t\"sensorAlias\": \"TempSpikeZone2\",\n\t\t\"sensorName\": \"TempSpikeZone2\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"7595\",\n\t\t\"sensorAlias\": \"TempSpikeZone5\",\n\t\t\"sensorName\": \"TempSpikeZone5\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"2756\",\n\t\t\"sensorAlias\": \"TempPaddleZone4\",\n\t\t\"sensorName\": \"TempPaddleZone4\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"9962\",\n\t\t\"sensorAlias\": \"TempPaddleZone6\",\n\t\t\"sensorName\": \"TempPaddleZone6\",\n\t\t\"unit\": \"\"\n\t}, {\n\t\t\"svid\": \"7275\",\n\t\t\"sensorAlias\": \"TempSpikeZone1\",\n\t\t\"sensorName\": \"TempSpikeZone1\",\n\t\t\"unit\": \"\"\n\t}],\n\t\"recipeNames\": [\"JS0800BSG0032\"]\n}"
//
//    val tm="{\"locationName\":\"FAB\",\"moduleName\":\"FUR\",\"toolName\":\"TTOOL_Thread-0\",\"chamberName\":\"TUBE\",\"sensors\":[{\"svid\":\"4943\",\"sensorAlias\":\"TempSpikeZone4\",\"sensorName\":\"TempSpikeZone4\",\"unit\":\"\"},{\"svid\":\"5513\",\"sensorAlias\":\"TempSpikeZone3\",\"sensorName\":\"TempSpikeZone3\",\"unit\":\"\"},{\"svid\":\"0518\",\"sensorAlias\":\"TempSpikeZone6\",\"sensorName\":\"TempSpikeZone6\",\"unit\":\"\"},{\"svid\":\"1824\",\"sensorAlias\":\"TempPaddleZone5\",\"sensorName\":\"TempPaddleZone5\",\"unit\":\"\"},{\"svid\":\"2598\",\"sensorAlias\":\"TempSpikeZone2\",\"sensorName\":\"TempSpikeZone2\",\"unit\":\"\"},{\"svid\":\"7595\",\"sensorAlias\":\"TempSpikeZone5\",\"sensorName\":\"TempSpikeZone5\",\"unit\":\"\"},{\"svid\":\"2756\",\"sensorAlias\":\"TempPaddleZone4\",\"sensorName\":\"TempPaddleZone4\",\"unit\":\"\"},{\"svid\":\"8461\",\"sensorAlias\":\"WaferTransMappingData\",\"sensorName\":\"WaferTransMappingData\",\"unit\":\"\"},{\"svid\":\"9962\",\"sensorAlias\":\"TempPaddleZone6\",\"sensorName\":\"TempPaddleZone6\",\"unit\":\"\"},{\"svid\":\"7275\",\"sensorAlias\":\"TempSpikeZone1\",\"sensorName\":\"TempSpikeZone1\",\"unit\":\"\"}],\"recipeNames\":[\"JS0800BSG0032\"]}"
//
//    val mm="{\"route\":\"035805.1.037005\",\"type\":null,\"operation\":\"AND128PP01_1\",\"layer\":\"JS0800BSG0032\",\"technology\":\"N.A.\",\"stage\":\"BSG-SACOX\",\"product\":\"Y006B_0\",\"lotData\":{\"locationName\":\"FAB\",\"moduleName\":\"FUR\",\"toolName\":\"FUR_NAURA_THEORIS_Thread-0\",\"chamberName\":\"TUBE\",\"lotName\":\"A166259\",\"carrier\":\"FP031240\"}}"
//
//
//    val tmb= fromJson[toolMessage](tm)
//
//    val mmb= beanToJsonNode[FdcData[MESMessage]](FdcData(MainFabConstants.MESMessage,fromJson[MESMessage](mm)))
//
//
//
//    val start=System.currentTimeMillis()
//    val mes=new MESBloomFilter
//
//    for(d <- 0 to 100000){
////      val ddd=fromJson[toolMessage](tm0)
////      ddd.setToolName(d.toString)
////      val tm0b= beanToJsonNode[FdcData[toolMessage]](FdcData(MainFabConstants.toolMessage,ddd))
//
//
//      val mmb= beanToJsonNode[FdcData[MESMessage]](FdcData(MainFabConstants.MESMessage,fromJson[MESMessage](mm)))
//
//
//      println(d)
//      mes.main(mmb)
//    }
//
////    mes.main(mmb)
////    for(d <- 0 to 100000){
////
////      println(d)
////      mes.main(mmb)
////    }
//    val end= System.currentTimeMillis()
//
//    println(end - start)
//    println("")
//  }
//
//}
