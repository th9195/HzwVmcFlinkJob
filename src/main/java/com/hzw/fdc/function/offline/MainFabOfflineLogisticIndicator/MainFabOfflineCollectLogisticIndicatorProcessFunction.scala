package com.hzw.fdc.function.offline.MainFabOfflineLogisticIndicator

import com.hzw.fdc.engine.calculatedindicator.{CalculatedIndicatorBuilder, CalculatedIndicatorBuilderFactory, ICalculatedIndicator}
import com.hzw.fdc.scalabean.{FdcData, OfflineIndicatorResult}
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent

/**
 * @author ：gdj
 * @date ：Created in 2021/8/30 15:58
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineCollectLogisticIndicatorProcessFunction extends KeyedProcessFunction[Long,
  FdcData[OfflineIndicatorResult],(OfflineIndicatorResult, Boolean, Boolean, Boolean)]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineCollectLogisticIndicatorProcessFunction])


  //taskid-->LogisticIndicatorId-->runId-->参与计算的indicator id --》IndicatorResult
  val offlineIndicatorResultByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[String,
    concurrent.TrieMap[Long, OfflineIndicatorResult]]]]()


  override def processElement(input: FdcData[OfflineIndicatorResult],
                              context: KeyedProcessFunction[Long, FdcData[OfflineIndicatorResult], (OfflineIndicatorResult,
                                Boolean, Boolean, Boolean)]#Context,
                              collector: Collector[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]): Unit = {
    try {

      val indicatorConfigFilter = input.datas.offlineIndicatorConfigList.filter(x => {
        if(x.algoClass.equals("logisticIndicator"))  {
          val algoParamList= x.algoParam.split("\\^_hzw_\\^").toList
          if(algoParamList.size>=2){
            val indicatorList = algoParamList.drop(1).map(x=>x.toLong)

            indicatorList.contains(input.datas.indicatorId)

          }else false
        } else false })


      for (elem <- indicatorConfigFilter) {

        val algoParamList: List[String] = elem.algoParam.split("\\^_hzw_\\^").toList

        val indicatorResultList = LoadIndicatorResult(input.datas, algoParamList,elem.indicatorId)

        if (indicatorResultList.nonEmpty) {


          val builder: CalculatedIndicatorBuilder = CalculatedIndicatorBuilderFactory.makeBuilder

          builder.setCalculatedIndicatorClassName(elem.indicatorName)
          builder.setAllowLoop(true)
          builder.addStaticImport("java.lang.Math")
          builder.setImplementation(algoParamList.head)
          builder.setTimeLimit(500)
          builder.setUserTimeLimit(300)


          var calculatedIndicator: ICalculatedIndicator = null

          try {
            calculatedIndicator = builder.build
            for (elem <- indicatorResultList) {
              calculatedIndicator.setIndicatorValue(elem.indicatorId.toString, elem.indicatorName, elem.indicatorValue.toDouble)
            }

            val calculatedIndicatorValue: Double = calculatedIndicator.calculate

            if((!calculatedIndicatorValue.isNaN) &&
                (!calculatedIndicatorValue.isPosInfinity) &&
                (!calculatedIndicatorValue.isNegInfinity)) {
              val logisticIndicatorResult = OfflineIndicatorResult(
                isCalculationSuccessful = true,
                calculationInfo = "",
                batchId = input.datas.batchId,
                taskId = input.datas.taskId,
                runId = input.datas.runId,
                toolName = input.datas.toolName,
                chamberName = input.datas.chamberName,
                indicatorValue = calculatedIndicatorValue.toString,
                indicatorId = elem.indicatorId,
                indicatorName = elem.indicatorName,
                algoClass = elem.algoClass,
                runStartTime = input.datas.runStartTime,
                runEndTime = input.datas.runEndTime,
                windowStartTime = input.datas.windowStartTime,
                windowEndTime = input.datas.windowStartTime,
                windowindDataCreateTime = input.datas.windowindDataCreateTime,
                unit = input.datas.unit,
                offlineIndicatorConfigList = input.datas.offlineIndicatorConfigList)
              collector.collect(logisticIndicatorResult,
                elem.driftStatus,
                elem.calculatedStatus,
                elem.logisticStatus)
            }else{
              logger.warn(s"input: ${input} \t IndicatorValue == Double.NaN  Double.PositiveInfinity  Double.NegativeInfinity")
            }

          } catch {
            case e: Exception => logger.warn(s"calculated Offline LogisticIndicator error Exception: ${ExceptionInfo.getExceptionInfo(e)}")

              val logisticIndicatorResult = OfflineIndicatorResult(
                isCalculationSuccessful = false,
                calculationInfo = s"${e.toString}",
                batchId = input.datas.batchId,
                taskId = input.datas.taskId,
                runId = input.datas.runId,
                toolName = input.datas.toolName,
                chamberName = input.datas.chamberName,
                indicatorValue = "0",
                indicatorId = input.datas.indicatorId,
                indicatorName = input.datas.indicatorName,
                algoClass = input.datas.algoClass,
                runStartTime = input.datas.runStartTime,
                runEndTime = input.datas.runEndTime,
                windowStartTime = input.datas.windowStartTime,
                windowEndTime = input.datas.windowEndTime,
                windowindDataCreateTime = input.datas.windowindDataCreateTime,
                unit = input.datas.unit,
                offlineIndicatorConfigList = input.datas.offlineIndicatorConfigList)
              collector.collect(logisticIndicatorResult,
                elem.driftStatus,
                elem.calculatedStatus,
                elem.logisticStatus)
          }
        }
      }
    } catch {
      case e: Exception => logger.warn(s"builder Offline LogisticIndicator error Exception: ${ExceptionInfo.getExceptionInfo(e)}")

        val logisticIndicatorResult = OfflineIndicatorResult(
          isCalculationSuccessful = false,
          calculationInfo = s"${e.toString}",
          batchId = input.datas.batchId,
          taskId = input.datas.taskId,
          runId = input.datas.runId,
          toolName = input.datas.toolName,
          chamberName = input.datas.chamberName,
          indicatorValue =  "0",
          indicatorId = input.datas.indicatorId,
          indicatorName = input.datas.indicatorName,
          algoClass = input.datas.algoClass,
          runStartTime = input.datas.runStartTime,
          runEndTime = input.datas.runEndTime,
          windowStartTime = input.datas.windowStartTime,
          windowEndTime = input.datas.windowEndTime,
          windowindDataCreateTime = input.datas.windowindDataCreateTime,
          unit = input.datas.unit,
          offlineIndicatorConfigList = input.datas.offlineIndicatorConfigList)
        collector.collect(logisticIndicatorResult,
          false,
          false,
          false)
    }








  }

  /**
   * 加载IndicatorResult
   * @param input
   * @return 是否都所有计算原料都到齐了
   */
  def LoadIndicatorResult(input: OfflineIndicatorResult,algoParamList: List[String],logisticIndicatorId:Long):
  List[OfflineIndicatorResult] ={

    val runId=input.runId

    if (algoParamList.size >= 2) {
      val indicatorList = algoParamList.drop(1)

      if (offlineIndicatorResultByAll.contains(input.taskId)) {

        val logisticIndicatorMap= offlineIndicatorResultByAll(input.taskId)

        if (logisticIndicatorMap.contains(logisticIndicatorId)) {

          val runMap = logisticIndicatorMap(logisticIndicatorId)

          if (runMap.contains(runId)) {

            val IndicatorResultMap = runMap(runId)

            IndicatorResultMap.put(input.indicatorId,input)
            val subIndicatorIdList = IndicatorResultMap.map(m => m._2.indicatorId).toList.sortBy(x=>x)
            val mainIndicatorIdList = indicatorList.map(_.toLong).sortBy(x=>x)

            if(subIndicatorIdList.containsSlice(mainIndicatorIdList)){

              runMap.remove(runId)
              if(runMap.isEmpty){
                logisticIndicatorMap.remove(logisticIndicatorId)
              }else{
                logisticIndicatorMap.put(logisticIndicatorId, runMap)
              }
              offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)

              IndicatorResultMap.map(_._2).toList

            }else {

              runMap.put(runId, IndicatorResultMap)
              logisticIndicatorMap.put(logisticIndicatorId, runMap)
              offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)

              Nil
            }


          } else {

            //IndicatorResultMap is null
            if (1 == indicatorList.size) {

              List(input)
            } else {

              val IndicatorResultMap = new concurrent.TrieMap[Long, OfflineIndicatorResult]()
              IndicatorResultMap.put(input.indicatorId,input)
              runMap.put(runId, IndicatorResultMap)
              logisticIndicatorMap.put(logisticIndicatorId, runMap)
              offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)

              Nil
            }

          }


        } else {

          //runMap is null
          if (1 == indicatorList.size) {

            List(input)
          } else {

            val IndicatorResultMap = new concurrent.TrieMap[Long, OfflineIndicatorResult]()
            IndicatorResultMap.put(input.indicatorId,input)

            val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]()
            runMap.put(runId, IndicatorResultMap)
            logisticIndicatorMap.put(logisticIndicatorId, runMap)
            offlineIndicatorResultByAll.put(input.taskId,logisticIndicatorMap)

            Nil
          }

        }
      } else {
        // logisticIndicatorMap is null
        if (1 == indicatorList.size) {

          List(input)
        } else {

          val IndicatorResultMap = new concurrent.TrieMap[Long, OfflineIndicatorResult]()
          IndicatorResultMap.put(input.indicatorId, input)

          val runMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]()
          runMap.put(runId, IndicatorResultMap)


          val logisticIndicatorMap = new concurrent.TrieMap[Long, concurrent.TrieMap[String, concurrent.TrieMap[Long, OfflineIndicatorResult]]]()
          logisticIndicatorMap.put(logisticIndicatorId, runMap)

          offlineIndicatorResultByAll.put(input.taskId, logisticIndicatorMap)
          Nil
        }
      }
    }else{
      //配置错误 无依赖的indicator
      Nil
    }
  }
}
