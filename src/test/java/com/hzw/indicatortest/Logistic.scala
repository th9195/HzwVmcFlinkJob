//package com.hzw.indicatortest
//
//import com.hzw.fdc.engine.calculatedindicator.{CalculatedIndicatorBuilder, CalculatedIndicatorBuilderFactory, ICalculatedIndicator}
//import com.hzw.fdc.json.JsonUtil.fromJson
//import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorResult}
//
//import scala.collection.concurrent
//
///**
// * @author ：gdj
// * @date ：Created in 2021/10/31 16:19
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//class Logistic {
//
//  //taskid-->LogisticIndicatorId-->runId-->参与计算的indicator id --》IndicatorResult
//
//  val offlineIndicatorResultByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]]]()
//
//  val indicatorDataByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[String, List[OfflineIndicatorResult]]]()
//
//  def main(): Unit = {
//
//
//
//
//    val str="{\"dataType\":\"offlineIndicator\",\"datas\":{\"isCalculationSuccessful\":true,\"calculationInfo\":\"\",\"taskId\":9152,\"runId\":\"CMPTool_90--YMTC_chamber--1635743223216\",\"toolName\":\"CMPTool_90\",\"chamberName\":\"YMTC_chamber\",\"indicatorValue\":\"120.03262619594928\",\"indicatorId\":20468,\"indicatorName\":\"All Time%CMPSEN027%STD\",\"algoClass\":\"std\",\"runStartTime\":1635743223216,\"runEndTime\":1635743293308,\"windowStartTime\":1635743223266,\"windowEndTime\":1635743292307,\"windowindDataCreateTime\":1635744115001,\"unit\":\"unit\",\"offlineIndicatorConfigList\":[{\"indicatorId\":20468,\"indicatorName\":\"All Time%CMPSEN027%STD\",\"controlWindowId\":2722,\"algoClass\":\"std\",\"algoParam\":null,\"algoType\":\"1\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":true},{\"indicatorId\":20741,\"indicatorName\":\"gdjTest\",\"controlWindowId\":2722,\"algoClass\":\"logisticIndicator\",\"algoParam\":\"Double i1 = $(\\\"All Time%CMPSEN001%Sum\\\");\\nDouble i2 = $(\\\"All Time%CMPSEN027%STD\\\");\\nDouble i3 = i1 + i2; \\nreturn i3;^_hzw_^20437^_hzw_^20468\",\"algoType\":\"3\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":false},{\"indicatorId\":20437,\"indicatorName\":\"All Time%CMPSEN001%Sum\",\"controlWindowId\":2722,\"algoClass\":\"sum\",\"algoParam\":null,\"algoType\":\"1\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":true},{\"indicatorId\":20741,\"indicatorName\":\"gdjTest\",\"controlWindowId\":2722,\"algoClass\":\"logisticIndicator\",\"algoParam\":\"Double i1 = $(\\\"All Time%CMPSEN001%Sum\\\");\\nDouble i2 = $(\\\"All Time%CMPSEN027%STD\\\");\\nDouble i3 = i1 + i2; \\nreturn i3;^_hzw_^20437^_hzw_^20468\",\"algoType\":\"3\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":false}]}}"
//
//    val input=fromJson[FdcData[OfflineIndicatorResult]](str)
//
//
//
//    val str2="{\"dataType\":\"offlineIndicator\",\"datas\":{\"isCalculationSuccessful\":true,\"calculationInfo\":\"\",\"taskId\":9152,\"runId\":\"CMPTool_90--YMTC_chamber--1635743223216\",\"toolName\":\"CMPTool_90\",\"chamberName\":\"YMTC_chamber\",\"indicatorValue\":\"6930.075378413183831\",\"indicatorId\":20437,\"indicatorName\":\"All Time%CMPSEN001%Sum\",\"algoClass\":\"sum\",\"runStartTime\":1635743223216,\"runEndTime\":1635743293308,\"windowStartTime\":1635743223266,\"windowEndTime\":1635743292307,\"windowindDataCreateTime\":1635744115001,\"unit\":\"unit\",\"offlineIndicatorConfigList\":[{\"indicatorId\":20468,\"indicatorName\":\"All Time%CMPSEN027%STD\",\"controlWindowId\":2722,\"algoClass\":\"std\",\"algoParam\":null,\"algoType\":\"1\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":true},{\"indicatorId\":20741,\"indicatorName\":\"gdjTest\",\"controlWindowId\":2722,\"algoClass\":\"logisticIndicator\",\"algoParam\":\"Double i1 = $(\\\"All Time%CMPSEN001%Sum\\\");\\nDouble i2 = $(\\\"All Time%CMPSEN027%STD\\\");\\nDouble i3 = i1 + i2; \\nreturn i3;^_hzw_^20437^_hzw_^20468\",\"algoType\":\"3\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":false},{\"indicatorId\":20437,\"indicatorName\":\"All Time%CMPSEN001%Sum\",\"controlWindowId\":2722,\"algoClass\":\"sum\",\"algoParam\":null,\"algoType\":\"1\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":true},{\"indicatorId\":20741,\"indicatorName\":\"gdjTest\",\"controlWindowId\":2722,\"algoClass\":\"logisticIndicator\",\"algoParam\":\"Double i1 = $(\\\"All Time%CMPSEN001%Sum\\\");\\nDouble i2 = $(\\\"All Time%CMPSEN027%STD\\\");\\nDouble i3 = i1 + i2; \\nreturn i3;^_hzw_^20437^_hzw_^20468\",\"algoType\":\"3\",\"driftStatus\":false,\"calculatedStatus\":false,\"logisticStatus\":false}]}}"
////
//    val input2=fromJson[FdcData[OfflineIndicatorResult]](str2)
////
//    prome(input2)
//    prome(input)
//
//
//
//println("")
//
//  }
//
//  def prome(input:FdcData[OfflineIndicatorResult])={
//
//    val indicatorConfigFilter = input.datas.offlineIndicatorConfigList.filter(x => {
//      if(x.algoClass.equals("logisticIndicator"))  {
//        val algoParamList= x.algoParam.split("\\^_hzw_\\^").toList
//        if(algoParamList.size>=2){
//          val indicatorList = algoParamList.drop(1).map(x=>x.toLong)
//
//
//          indicatorList.contains(input.datas.indicatorId)
//
//        }else false
//      } else false })
//
//    for (elem <- indicatorConfigFilter) {
//
//
//
//      val algoParamList: List[String] = elem.algoParam.split("\\^_hzw_\\^").toList
//
//
//      val indicatorResultList = LoadIndicatorResult(input.datas, algoParamList,elem.indicatorId)
//
//
//      val sdf=offlineIndicatorResultByAll
//      if (indicatorResultList.nonEmpty) {
//
//
//        val builder: CalculatedIndicatorBuilder = CalculatedIndicatorBuilderFactory.makeBuilder
//        println("----")
//        println(elem.indicatorName)
//        builder.setCalculatedIndicatorClassName(elem.indicatorName)
//        builder.setAllowLoop(true)
//        builder.addStaticImport("java.lang.Math")
//
//        builder.setImplementation(algoParamList.head)
//        builder.setTimeLimit(500)
//        builder.setUserTimeLimit(300)
//        println("----")
//        println(algoParamList.head)
//
//        var calculatedIndicator: ICalculatedIndicator = null
//
//
//        calculatedIndicator = builder.build
//        for (elem <- indicatorResultList) {
//          println("----")
//          println(elem.indicatorName)
//          calculatedIndicator.setIndicatorValue(elem.indicatorId.toString, elem.indicatorName, elem.indicatorValue.toDouble)
//
//        }
//
//        val calculatedIndicatorValue: Double = calculatedIndicator.calculate
//
//        val logisticIndicatorResult = OfflineIndicatorResult(
//          isCalculationSuccessful = true,
//          calculationInfo = "",
//          taskId = input.datas.taskId,
//          runId = input.datas.runId,
//          toolName = input.datas.toolName,
//          chamberName = input.datas.chamberName,
//          indicatorValue = calculatedIndicatorValue.toString,
//          indicatorId = elem.indicatorId,
//          indicatorName = elem.indicatorName,
//          algoClass = elem.algoClass,
//          runStartTime = input.datas.runStartTime,
//          runEndTime = input.datas.runEndTime,
//          windowStartTime = input.datas.windowStartTime,
//          windowEndTime = input.datas.windowEndTime,
//          windowindDataCreateTime = input.datas.windowindDataCreateTime,
//          unit = input.datas.unit,
//          offlineIndicatorConfigList = input.datas.offlineIndicatorConfigList)
//
//
//        println("")
//
//
//
//      }
//    }
//  }
//
//
//
//  /**
//   * 加载IndicatorResult
//   * @param input
//   * @return 是否都所有计算原料都到齐了
//   */
//  def LoadIndicatorResult(input: OfflineIndicatorResult,algoParamList: List[String],logisticIndicatorId:Long): List[OfflineIndicatorResult] ={
//
//    val runId=input.runId
//
//
//    if (algoParamList.size >= 2) {
//      val indicatorList = algoParamList.drop(1)
//
//
//
//
//
//      if (offlineIndicatorResultByAll.contains(input.taskId)) {
//
//        val logisticIndicatorMap= offlineIndicatorResultByAll(input.taskId)
//
//        if (logisticIndicatorMap.contains(logisticIndicatorId)) {
//
//          val runMap = logisticIndicatorMap(logisticIndicatorId)
//
//          if (runMap.contains(runId)) {
//
//            val IndicatorResultMap = runMap(runId)
//
//            IndicatorResultMap.put(input.indicatorId,input)
//            val subIndicatorIdList = IndicatorResultMap.map(m => m._2.indicatorId).toList.sortBy(x=>x)
//            val mainIndicatorIdList = indicatorList.map(_.toLong).sortBy(x=>x)
//
//
//            println(s"subIndicatorIdList :$subIndicatorIdList mainIndicatorIdList:$mainIndicatorIdList")
//            val f=subIndicatorIdList.containsSlice(mainIndicatorIdList)
//            if(subIndicatorIdList.containsSlice(mainIndicatorIdList)){
//
//              runMap.remove(runId)
//              if(runMap.isEmpty){
//                logisticIndicatorMap.remove(logisticIndicatorId)
//              }else{
//                logisticIndicatorMap.put(logisticIndicatorId, runMap)
//              }
//              offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)
//
//              IndicatorResultMap.map(_._2).toList
//
//            }else {
//
//              runMap.put(runId, IndicatorResultMap)
//              logisticIndicatorMap.put(logisticIndicatorId, runMap)
//              offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)
//
//              Nil
//            }
//
//
//          } else {
//
//            //IndicatorResultMap is null
//            if (1 == indicatorList.size) {
//
//              List(input)
//            } else {
//
//              val IndicatorResultMap = new concurrent.TrieMap[Long, OfflineIndicatorResult]()
//              IndicatorResultMap.put(input.indicatorId,input)
//              runMap.put(runId, IndicatorResultMap)
//              logisticIndicatorMap.put(logisticIndicatorId, runMap)
//              offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)
//
//              Nil
//            }
//
//          }
//
//
//        } else {
//
//          //runMap is null
//          if (1 == indicatorList.size) {
//
//            List(input)
//          } else {
//
//            val IndicatorResultMap = new concurrent.TrieMap[Long, OfflineIndicatorResult]()
//            IndicatorResultMap.put(input.indicatorId,input)
//
//            val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]()
//            runMap.put(runId, IndicatorResultMap)
//            logisticIndicatorMap.put(logisticIndicatorId, runMap)
//            offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)
//
//            Nil
//          }
//
//        }
//      } else {
//
//        if (1 == indicatorList.size) {
//
//          List(input)
//        } else {
//
//
//          // logisticIndicatorMap is null
//          println("add logisticIndicatorMap")
//          val IndicatorResultMap = new concurrent.TrieMap[Long, OfflineIndicatorResult]()
//          IndicatorResultMap.put(input.indicatorId, input)
//
//          val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]()
//          runMap.put(runId, IndicatorResultMap)
//
//
//          val logisticIndicatorMap = new concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]]()
//          logisticIndicatorMap.put(logisticIndicatorId, runMap)
//
//          offlineIndicatorResultByAll.put(input.taskId, logisticIndicatorMap)
//          Nil
//        }
//      }
//
//
//    }else{
//      //配置错误 无依赖的indicator
//      Nil
//    }
//
//
//
//
//
//
//
//  }
//
//}
