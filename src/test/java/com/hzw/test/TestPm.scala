//package com.hzw.test
//
//import com.hzw.fdc.function.PublicFunction.MainFabKafkaDataJSONSchema
//import com.hzw.fdc.json.JsonUtil.{toBean, toJsonNode}
//import com.hzw.fdc.json.MarshallableImplicits.Marshallable
//import com.hzw.fdc.scalabean.{IndicatorTree, OfflineOpentsdbQuery, OfflineTask, TreeConfig, WindowConfigAlias, WindowTree}
//import org.slf4j.{Logger, LoggerFactory}
//
//import java.util
//
//
///**
// * @author ：gdj
// * @date ：Created in 2021/10/16 14:59
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//object TestPm {
//  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabKafkaDataJSONSchema])
//  def main(args: Array[String]): Unit = {
//
//
//    val taskStr="{\n  \"dataType\" : \"offlineIndicatorTask\",\n  \"taskId\" : 982,\n  \"runData\" : [ {\n    \"runId\" : \"CMPTool_88--YMTC_chamber--1635301114158\",\n    \"runStart\" : 1635301114158,\n    \"runEnd\" : 1635301184387\n  }, {\n    \"runId\" : \"CMPTool_88--YMTC_chamber--1635301041865\",\n    \"runStart\" : 1635301041865,\n    \"runEnd\" : 1635301112053\n  }, {\n    \"runId\" : \"CMPTool_88--YMTC_chamber--1635300969541\",\n    \"runStart\" : 1635300969541,\n    \"runEnd\" : 1635301039764\n  }, {\n    \"runId\" : \"CMPTool_88--YMTC_chamber--1635300897230\",\n    \"runStart\" : 1635300897230,\n    \"runEnd\" : 1635300967429\n  } ],\n  \"indicatorTree\" : {\n    \"current\" : {\n      \"windowTree\" : null,\n      \"indicatorConfig\" : {\n        \"sensorAlias\" : null,\n        \"svid\" : null,\n        \"indicatorId\" : 3981,\n        \"indicatorName\" : \"drift2%All Time%CMPSEN001%Max\",\n        \"missingRatio\" : 20,\n        \"controlWindowId\" : 1184,\n        \"algoClass\" : \"drift\",\n        \"algoParam\" : \"3971|2\",\n        \"algoType\" : \"2\",\n        \"algoName\" : \"Drift\",\n        \"driftStatus\" : \"false\",\n        \"calculatedStatus\" : \"false\",\n        \"logisticStatus\" : \"false\",\n        \"w2wType\" : \"By Tool-Chamber-Recipe\",\n        \"bypassCondition\" : null\n      }\n    },\n    \"nexts\" : [ {\n      \"current\" : {\n        \"windowTree\" : {\n          \"current\" : {\n            \"contextId\" : 904,\n            \"controlWindowId\" : 1184,\n            \"parentWindowId\" : 0,\n            \"isTopWindows\" : true,\n            \"isConfiguredIndicator\" : true,\n            \"windowStart\" : \"_START\",\n            \"windowEnd\" : \"_END\",\n            \"controlPlanId\" : 884,\n            \"controlPlanVersion\" : 2,\n            \"controlWindowType\" : \"TimeBasedWindow\",\n            \"calcTrigger\" : \"ProcessEnd\",\n            \"sensorAlias\" : [ {\n              \"sensorAliasId\" : 41,\n              \"sensorAliasName\" : \"CMPSEN001\",\n              \"svid\" : \"1357\",\n              \"indicatorId\" : [ 3971 ]\n            } ]\n          },\n          \"nexts\" : null,\n          \"next\" : null\n        },\n        \"indicatorConfig\" : {\n          \"sensorAlias\" : \"CMPSEN001\",\n          \"svid\" : \"1357\",\n          \"indicatorId\" : 3971,\n          \"indicatorName\" : \"All Time%CMPSEN001%Max\",\n          \"missingRatio\" : 20,\n          \"controlWindowId\" : 1184,\n          \"algoClass\" : \"max\",\n          \"algoParam\" : \"1\",\n          \"algoType\" : \"1\",\n          \"algoName\" : \"Max\",\n          \"driftStatus\" : \"true\",\n          \"calculatedStatus\" : \"false\",\n          \"logisticStatus\" : \"false\",\n          \"w2wType\" : \"By Tool-Chamber-Recipe\",\n          \"bypassCondition\" : null\n        }\n      },\n      \"nexts\" : [ ],\n      \"next\" : null\n    } ],\n    \"next\" : null\n  }\n}"
//
//    val text1=toJsonNode(taskStr,logger)
//    val str = text1.findPath("dataType").asText()
//
////    toJsonNode(str,logger)
//
//    val task:OfflineTask= toBean[OfflineTask](text1)
//
//   val indicatorlist= getIndicatorConfigList(task.indicatorTree)
//
//
//    //递归获取 Tree 中的sensorList
//    val sensorList = getSensorList(task.indicatorTree)
//
//
//
//    for (run <- task.runData) {
//      val runIdSpit = run.runId.split("--", -1)
//      val toolName = runIdSpit(0)
//      val chamberName = runIdSpit(0)
//
//      for (sensor <- sensorList) {
//        val tags = new util.HashMap[String, String]()
//        tags.put("stepId", "*")
//        val qualityQuery = Map(
//          "tags" -> tags,
//          "metric" -> String.format("%s.%s.%s", toolName, chamberName, sensor.svid),
//          "aggregator" -> "none"
//        )
//        val query = Map(
//          "start" -> run.runStart.toString,
//          "end" -> run.runEnd.toString,
//          "msResolution" -> true,
//          "queries" -> List(qualityQuery)
//        )
//        val queryStr = query.toJson
//        OfflineOpentsdbQuery(task = task,
//          toolName = toolName,
//          chamberName = chamberName,
//          sensorAlias = sensor.sensorAliasName,
//          taskSvidNum = sensorList.size,
//          svid = sensor.svid,
//          runId = run.runId,
//          runStart = run.runStart,
//          runEnd = run.runEnd,
//          dataMissingRatio = run.dataMissingRatio,
//          queryStr = queryStr)
//        logger.warn(s"queryStr: $queryStr")
//        println(s"queryStr: $queryStr")
//      }
//      println("fasdfa")
//    }
//
//
//    println("fasdfa")
//  }
//  /**
//   * 获取 indicatorNode的 sensor list
//   * @param indicatorNode
//   * @return
//   */
//  def getSensorList(indicatorTree:IndicatorTree): List[WindowConfigAlias] ={
//
//    if(indicatorTree.nexts.nonEmpty) {
//      val list2=for (elem <- indicatorTree.nexts) yield {
//        getSensorList(elem) ++ getWindowTreeSensorList(elem.current.windowTree.get)
//      }
//
//      val list= list2.reduceLeft(_ ++ _)
//      list
//    }else{
//      if(indicatorTree.current.windowTree.nonEmpty){
//        getWindowTreeSensorList(indicatorTree.current.windowTree.get)
//      }else Nil
//    }
//
//  }
//
//  /**
//   * 遍历 window Tree的 sensor list 并且拼接
//   * @param windowNode
//   * @return
//   */
//  def getWindowTreeSensorList(windowNode:WindowTree): List[WindowConfigAlias] ={
//
//
//    if(windowNode.next.nonEmpty){
//      //有下一个节点，递归调用并且拼接
//      windowNode.current.sensorAlias ++ getWindowTreeSensorList(windowNode.next.get)
//    }else {
//      //没有下一个节点，返回
//      windowNode.current.sensorAlias
//    }
//
//  }
//
//
//  /**
//   * 获取 indicatorNode的 sensor list
//   * @param indicatorNode
//   * @return
//   */
//  def getIndicatorConfigList(indicatorTree:IndicatorTree): List[TreeConfig] ={
//
//    if(indicatorTree.nexts.nonEmpty) {
//
//      val list2=for (elem <- indicatorTree.nexts) yield {
//        getIndicatorConfigList(elem) :+ indicatorTree.current
//      }
//
//      val list= list2.reduceLeft(_ ++ _)
//      list
//    }else{
//      //      if(indicatorTree.current.windowTree.nonEmpty){
//      //        List(indicatorTree.current)
//      //      }
//      List(indicatorTree.current)
//    }
//
//  }
//
//}
