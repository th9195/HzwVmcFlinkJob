package com.hzw.fdc.function.PublicFunction

import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import org.slf4j.{Logger, LoggerFactory}

object SpecialJsonToObject {
//  private val logger: Logger = LoggerFactory.getLogger(classOf[SpecialJsonToObject])


//  /**
//   * 解析rundata 数据
//   */
//  def parseRundata(kafkaDatas: KafkaDatas): RunDataScala = {
//    val myDatas = kafkaDatas.datas
//    RunDataScala(myDatas("runId").asInstanceOf[String]
//      , myDatas.getOrElse("toolId", "").asInstanceOf[String]
//      , myDatas.getOrElse("chamberId", "").asInstanceOf[String]
//      , myDatas.getOrElse("subRecipe", "").asInstanceOf[String]
//      , myDatas.getOrElse("missingRatio", "").asInstanceOf[String]
//      , myDatas.getOrElse("processId", "").asInstanceOf[String]
//      , myDatas.getOrElse("processStepId", "").asInstanceOf[String]
//      , myDatas.getOrElse("lotId", "").asInstanceOf[String]
//      , myDatas.getOrElse("lotname", "").asInstanceOf[String]
//      , myDatas.getOrElse("waferId", "").asInstanceOf[String]
//      , myDatas.getOrElse("slotId", "").asInstanceOf[String]
//      , myDatas.getOrElse("runStartTime", "").asInstanceOf[Long]
//      , myDatas.getOrElse("processTime", "").asInstanceOf[Int]
//      , myDatas.getOrElse("runEndTime", "").asInstanceOf[Long]
//      , myDatas.getOrElse("createTime", "").asInstanceOf[Long]
//      , myDatas.getOrElse("field1", "").asInstanceOf[String]
//      , myDatas.getOrElse("field2", "").asInstanceOf[String]
//      , myDatas.getOrElse("field3", "").asInstanceOf[String]
//      , myDatas.getOrElse("field4", "").asInstanceOf[String]
//      , myDatas.getOrElse("field5", "").asInstanceOf[String]
//      , myDatas.getOrElse("step", "").asInstanceOf[String]
//      , myDatas.getOrElse("carrier", "").asInstanceOf[String]
//      , myDatas.getOrElse("layer", "").asInstanceOf[String]
//      , myDatas.getOrElse("operation", "").asInstanceOf[String]
//      , myDatas.getOrElse("product", "").asInstanceOf[String]
//      , myDatas.getOrElse("route", "").asInstanceOf[String]
//      , myDatas.getOrElse("stage", "").asInstanceOf[String]
//      , myDatas.getOrElse("technology", "").asInstanceOf[String]
//      , myDatas.getOrElse("type", "").asInstanceOf[String]
//      , myDatas.getOrElse("waferName", "").asInstanceOf[String]
//      , myDatas.getOrElse("runDataNum", "").asInstanceOf[String]
//    )
//  }




//  def parseAlarmLevelRule(indicatorResult: IndicatorResultScala,
//                          alarmConfig: AlarmRuleConfig,
//                          LIMIT: String,
//                          RuleTrigger: String,
//                          ruleList: List[Rule]): AlarmLevelRule = {
//    AlarmLevelRule(
//      indicatorResult.controlPlanVersion.toInt,
//      indicatorResult.chamberName,
//      indicatorResult.indicatorCreateTime,
//      //alarm创建的时间应该是当前时间
//      System.currentTimeMillis(),
//      indicatorResult.indicatorId,
//      indicatorResult.runId,
//      indicatorResult.toolName,
//      LIMIT,
//      RuleTrigger,
//      indicatorResult.indicatorValue,
//      indicatorResult.indicatorName,
//      indicatorResult.controlPlanId.toLong,
//      indicatorResult.controlPlanName,
//      indicatorResult.missingRatio,
//      indicatorResult.configMissingRatio,
//      indicatorResult.runStartTime,
//      indicatorResult.runEndTime,
//      indicatorResult.windowStartTime,
//      indicatorResult.windowEndTime,
//      indicatorResult.windowDataCreateTime,
//      indicatorResult.locationId.toLong,
//      indicatorResult.locationName,
//      indicatorResult.moduleId.toLong,
//      indicatorResult.moduleName,
//      indicatorResult.toolGroupId.toLong,
//      indicatorResult.toolGroupName,
//      indicatorResult.chamberGroupId.toLong,
//      indicatorResult.recipe,
//      indicatorResult.product,
//      indicatorResult.stage,
//      ruleList
//    )
//  }

  /**
   * 解析字符串成数字,分隔符是|
   */
  def strToList(str: String): List[String] = {
    var array = List("", "")
    try {
      if (str != null || str != "") {
        array = str.split("\\|").toList
      }
      if (array.size == 1) {
        array = array :+ ""
      }
    } catch {
      case ex: Exception => println(s"strToList: " + ex + "\tstr:" + str)
    }
    array
  }
}
