//package com.hzw.test
//
//import com.hzw.fdc.engine.calculatedindicator.{CalculatedIndicatorBuilder, CalculatedIndicatorBuilderFactory, ICalculatedIndicator}
//import com.hzw.fdc.json.JsonUtil.fromJson
//import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorResult}
//
//import scala.collection.concurrent
//
///**
// * @author ：gdj
// * @date ：Created in 2021/10/27 14:23
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//object logisticstest {
//
//  val indicatorDataByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[String, List[OfflineIndicatorResult]]]()
//
//  def main(args: Array[String]): Unit = {
//
//
//    val str = "{\"dataType\":\"offlineIndicator\",\"datas\":{\"isCalculationSuccessful\":true,\"calculationInfo\":\"\",\"taskId\":997,\"runId\":\"CMPTool_88--YMTC_chamber--1635312894843\",\"toolName\":\"CMPTool_88\",\"chamberName\":\"YMTC_chamber\",\"indicatorValue\":\"901.6637899368351\",\"indicatorId\":3971,\"indicatorName\":\"All Time%CMPSEN001%Max\",\"algoClass\":\"max\",\"runStartTime\":1635312894843,\"runEndTime\":1635312965004,\"windowStartTime\":1635312894893,\"windowEndTime\":1635312964003,\"windowindDataCreateTime\":1635313222227,\"unit\":\"unit\",\"offlineIndicatorConfigList\":[{\"indicatorId\":3971,\"indicatorName\":\"All Time%CMPSEN001%Max\",\"controlWindowId\":1184,\"algoClass\":\"max\",\"algoParam\":\"1\",\"algoType\":\"1\",\"driftStatus\":true,\"calculatedStatus\":true,\"logisticStatus\":true},{\"indicatorId\":3985,\"indicatorName\":\"111\",\"controlWindowId\":1184,\"algoClass\":\"calculated\",\"algoParam\":\"3971,3973,Number(12)|+,+|All Time%CMPSEN001%Max  +  drift1%MA2%All Time%CMPSEN001%Max  +  12\",\"algoType\":\"3\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":false},{\"indicatorId\":3971,\"indicatorName\":\"All Time%CMPSEN001%Max\",\"controlWindowId\":1184,\"algoClass\":\"max\",\"algoParam\":\"1\",\"algoType\":\"1\",\"driftStatus\":true,\"calculatedStatus\":true,\"logisticStatus\":true},{\"indicatorId\":3972,\"indicatorName\":\"MA2%All Time%CMPSEN001%Max\",\"controlWindowId\":1184,\"algoClass\":\"MA\",\"algoParam\":\"3971|2\",\"algoType\":\"2\",\"driftStatus\":true,\"calculatedStatus\":false,\"logisticStatus\":false},{\"indicatorId\":3973,\"indicatorName\":\"drift1%MA2%All Time%CMPSEN001%Max\",\"controlWindowId\":1184,\"algoClass\":\"drift\",\"algoParam\":\"3972|1\",\"algoType\":\"2\",\"driftStatus\":false,\"calculatedStatus\":true,\"logisticStatus\":false},{\"indicatorId\":3985,\"indicatorName\":\"111\",\"controlWindowId\":1184,\"algoClass\":\"calculated\",\"algoParam\":\"3971,3973,Number(12)|+,+|All Time%CMPSEN001%Max  +  drift1%MA2%All Time%CMPSEN001%Max  +  12\",\"algoType\":\"3\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":false}]}}"
//
//
//    val input = fromJson[FdcData[OfflineIndicatorResult]](str)
//
//
//    val indicatorConfigFilter = input.datas.offlineIndicatorConfigList.filter(x => x.indicatorId == input.datas.indicatorId)
//
//    if (indicatorConfigFilter.size >= 1) {
//
//      val indicatorConfig = indicatorConfigFilter.head
//      val algoParamList: List[String] = "Double i1 = $(\\\"All Time%CMPSEN001%Max\\\");\\nDouble i2 = i1 * 2;\\nreturn i2;^_hzw_^3971".split("\\^_hzw_\\^").toList
//
//
//      val indicatorResultList = LoadIndicatorResult(input.datas, algoParamList)
//
//      if (indicatorResultList.nonEmpty) {
//
//
//        val builder: CalculatedIndicatorBuilder = CalculatedIndicatorBuilderFactory.makeBuilder
//
//        builder.setCalculatedIndicatorClassName(input.datas.indicatorName)
//        builder.setAllowLoop(true)
//        builder.addStaticImport("java.lang.Math")
//        builder.setImplementation(algoParamList.head)
//        builder.setTimeLimit(500)
//        builder.setUserTimeLimit(300)
//
//
//        var calculatedIndicator: ICalculatedIndicator = null
//
//        try {
//          calculatedIndicator = builder.build
//          for (elem <- indicatorResultList) {
//
//            calculatedIndicator.setIndicatorValue(elem.indicatorId.toString, elem.indicatorName, elem.indicatorValue.toDouble)
//
//          }
//
//          val calculatedIndicatorValue: Double = calculatedIndicator.calculate
//
//          val logisticIndicatorResult = OfflineIndicatorResult(
//            isCalculationSuccessful = true,
//            calculationInfo = "",
//            taskId = input.datas.taskId,
//            runId = input.datas.runId,
//            toolName = input.datas.toolName,
//            chamberName = input.datas.chamberName,
//            indicatorValue = calculatedIndicatorValue.toString,
//            indicatorId = input.datas.indicatorId,
//            indicatorName = input.datas.indicatorName,
//            algoClass = input.datas.algoClass,
//            runStartTime = input.datas.runStartTime,
//            runEndTime = input.datas.runEndTime,
//            windowStartTime = input.datas.windowStartTime,
//            windowEndTime = input.datas.windowEndTime,
//            windowindDataCreateTime = input.datas.windowindDataCreateTime,
//            unit = input.datas.unit,
//            offlineIndicatorConfigList = input.datas.offlineIndicatorConfigList)
//
//
//         val out= (logisticIndicatorResult,
//            indicatorConfig.driftStatus,
//            indicatorConfig.calculatedStatus,
//            indicatorConfig.logisticStatus)
//
//          println(out)
//
//
//        } catch {
//          case e: Exception => println(s"calculatedIndicator.calculate error")
//        }
//
//
//      }
//    }
//  }
//
//
//  /**
//   * 加载IndicatorResult
//   *
//   * @param input
//   * @return 是否都所有计算原料都到齐了
//   */
//  def LoadIndicatorResult(input: OfflineIndicatorResult, algoParamList: List[String]): List[OfflineIndicatorResult] = {
//    val logisticIndicatorId = input.indicatorId
//    val runId = input.runId
//
//    if (algoParamList.size >= 2) {
//      val indicatorList = algoParamList.drop(1)
//
//      if (indicatorDataByAll.contains(logisticIndicatorId)) {
//        val runMap = indicatorDataByAll.get(logisticIndicatorId).get
//
//        if (runMap.contains(runId)) {
//          val IndicatorResultList: List[OfflineIndicatorResult] = runMap.get(runId).get
//          val newList = IndicatorResultList :+ input
//
//
//          if (newList.size >= indicatorList.size) {
//
//            runMap.remove(runId)
//            indicatorDataByAll.put(logisticIndicatorId, runMap)
//
//            newList
//          } else {
//
//            runMap.put(runId, newList)
//            indicatorDataByAll.put(logisticIndicatorId, runMap)
//
//            Nil
//          }
//
//
//        } else {
//
//
//          if (1 >= indicatorList.size) {
//
//            List(input)
//          } else {
//
//            runMap.put(runId, List(input))
//            indicatorDataByAll.put(logisticIndicatorId, runMap)
//
//            Nil
//          }
//
//        }
//
//
//      } else {
//
//
//        if (1 >= indicatorList.size) {
//
//          List(input)
//        } else {
//
//          val runMap = new concurrent.TrieMap[String, List[OfflineIndicatorResult]]()
//          runMap.put(runId, List(input))
//          indicatorDataByAll.put(logisticIndicatorId, runMap)
//
//          Nil
//        }
//
//      }
//
//
//    } else {
//      //配置错误 无依赖的indicator
//      Nil
//    }
//
//
//  }
//}
