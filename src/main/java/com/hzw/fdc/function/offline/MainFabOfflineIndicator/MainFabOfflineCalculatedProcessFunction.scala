package com.hzw.fdc.function.offline.MainFabOfflineIndicator

import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

/**
 *  Calculated计算， 维度是run_id, 对同一run_id 的多个indicator计算
 */
class MainFabOfflineCalculatedProcessFunction extends KeyedProcessFunction[Long, FdcData[OfflineIndicatorResult],
  ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineCalculatedProcessFunction])

  // {"taskId" : {"INDICATOR_ID": "IndicatorConfigScala"}}}
  var indicatorConfigByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, OfflineIndicatorConfig]]()

  // 缓存RUN_ID { "taskId": {"RUN_ID": {"INDICATOR_ID": "INDICATOR_VALUE"} }}
  var calculatedDataByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, String]]]()


  // 用于保存哪些indicator完成了计算  {"RUN_ID": ["INDICATOR_ID1"]}
  val finishIndicatorAll = new concurrent.TrieMap[String, scala.collection.mutable.Set[String]]()


  override def processElement(record: FdcData[OfflineIndicatorResult], ctx: KeyedProcessFunction[Long,
    FdcData[OfflineIndicatorResult], ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                              out: Collector[ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "offlineIndicator" =>

          val indicatorResult = record.datas

          val taskId = indicatorResult.taskId.toString


          // RUN_ID: RNITool_31--YMTC_chamber--1604980730000
          val RUN_ID: String = indicatorResult.runId
          val INDICATOR_ID = indicatorResult.indicatorId.toString
          val INDICATOR_VALUE = indicatorResult.indicatorValue

          // 保留配置信息
          if(!indicatorConfigByAll.contains(taskId)){
            val offlineIndicatorConfigList = indicatorResult.offlineIndicatorConfigList

            val tmpList = concurrent.TrieMap[String, OfflineIndicatorConfig]()
            for(offlineIndicatorConfig <- offlineIndicatorConfigList){
              if(offlineIndicatorConfig.algoClass == "calculated"){
                tmpList.put(offlineIndicatorConfig.indicatorId.toString, offlineIndicatorConfig)
              }
            }
            indicatorConfigByAll.put(taskId, tmpList)
          }


          if(!this.calculatedDataByAll.contains(taskId)){
            val tmp = concurrent.TrieMap(RUN_ID -> concurrent.TrieMap[String, String](INDICATOR_ID -> INDICATOR_VALUE))
            this.calculatedDataByAll += (taskId -> tmp)
            //logger.info(s"CalculatedIndicator_step2")
          }else {
            var calculatedDataMap = this.calculatedDataByAll(taskId)

            // 添加INDICATOR_ID和INDICATOR_VALUE
            if (!calculatedDataMap.contains(RUN_ID)) {
              val INDICATOR_ID_data = concurrent.TrieMap[String, String](INDICATOR_ID -> INDICATOR_VALUE)
              calculatedDataMap += (RUN_ID -> INDICATOR_ID_data)
              this.calculatedDataByAll.update(taskId, calculatedDataMap)
            } else {
              var indicatorDataMap = calculatedDataMap(RUN_ID)
              indicatorDataMap += (INDICATOR_ID -> INDICATOR_VALUE)
              calculatedDataMap.update(RUN_ID, indicatorDataMap)
              this.calculatedDataByAll.update(taskId, calculatedDataMap)
            }

          }

           // 开始计算
           val calculatedDataMap = this.calculatedDataByAll(taskId)
           val indicatorDataMap = calculatedDataMap(RUN_ID)

           //            if(indicatorConfigSet == indicatorDataMap.keySet){
           val IndicatorResultScalaList = new ListBuffer[(OfflineIndicatorResult, Boolean, Boolean, Boolean)]()

           val indicatorConfigMap = indicatorConfigByAll(taskId)
           for(indicatorConfig <- indicatorConfigMap.values){
             var parameter = ""
             var operator = ""
             val algoParam = indicatorConfig.algoParam
             try{
               val tmp = algoParam.split("\\|")
               parameter = tmp(0)
               operator = tmp(1)
             }catch {
               case ex: Exception => logger.warn(s"CalculatedIndicator algoParam: $ex")
             }

             // 判断是否开始计算
             if(isCalculated(parameter, indicatorDataMap.keys.toList)) {

               // 基础的计算
               val indicator_value: String = baseMath(parameter, operator, indicatorDataMap)

               // 封装返回结果的对象
               val IndicatorResultScala = resIndicatorResultScala(indicatorResult,
                 indicator_value, indicatorConfig)
               IndicatorResultScalaList.append((IndicatorResultScala, indicatorConfig.driftStatus,
                 indicatorConfig.calculatedStatus, indicatorConfig.logisticStatus))

               // 判断RUN_ID 的所有indicator是否都计算完成
               if(isRunIDFinishCalculated(indicatorConfigMap, RUN_ID, indicatorConfig)){
                 calculatedDataMap.remove(RUN_ID)
                 this.calculatedDataByAll.update(taskId, calculatedDataMap)
               }
             }
           }

           if(IndicatorResultScalaList.nonEmpty){
             out.collect(IndicatorResultScalaList)
           }

           // 超100个开始清理过期的taskId
           if(calculatedDataByAll.size > 100) {
             for (elem <- calculatedDataByAll.keys.toList.sorted.take(50)){
               calculatedDataByAll.remove(elem)
               indicatorConfigByAll.remove(elem)
             }
           }

        case _ => logger.warn(s"CalculatedIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("004008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "calculated"), s"indicator job error $ex").toJson)
    }
  }


  /**
   *  判断是否满足计算的条件
   */
  def isCalculated(parameter: String, indicatorDataList: List[String]):Boolean = {
    try{
      val indicatorList = parameter.split(",")
      // 过滤不是indicator的数字
      indicatorList.foreach(indicator =>
      {
        if(!indicator.contains("Number(")) {
          if(!indicatorDataList.contains(indicator)){
            return false
          }
        }
      })
      true
    }catch {
      case ex: Exception => logger.warn(s"CalculatedIndicator isCalculated_ERROR: $ex")
        false
    }
  }


  /**
   *  判断RUN_ID 的所有indicator是否都计算完成
   */
  def isRunIDFinishCalculated(indicatorConfigMap: concurrent.TrieMap[String, OfflineIndicatorConfig], RUN_ID: String,
                              indicatorConfig: OfflineIndicatorConfig):Boolean = {
    var Res = false
    try{
      val indicatorId = indicatorConfig.indicatorId.toString

      if(finishIndicatorAll.contains(RUN_ID)){
        var indicatorDataSet: mutable.Set[String] = finishIndicatorAll(RUN_ID)

        indicatorDataSet.add(indicatorId)
        finishIndicatorAll.update(RUN_ID, indicatorDataSet)

        // 判断是否全部计算完
        if(indicatorDataSet == indicatorConfigMap.keySet) {
          Res = true
          finishIndicatorAll.remove(RUN_ID)
        }
      }else{
        finishIndicatorAll += (RUN_ID -> mutable.Set(indicatorId))
      }
    }catch {
      case ex: Exception => logger.warn(s"CalculatedIndicator_hasCalculated_ERROR: $ex")
    }
    Res
  }

  /**
   *  返回IndicatorResultScala对象
   */
  def resIndicatorResultScala(indicatorResult: OfflineIndicatorResult, indicator_value: String,
                              dstIndicatorConfig: OfflineIndicatorConfig):OfflineIndicatorResult={
    OfflineIndicatorResult(
      isCalculationSuccessful = true,
      "",
      indicatorResult.batchId,
      indicatorResult.taskId,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.chamberName,
      indicator_value,
      dstIndicatorConfig.indicatorId.toLong,
      dstIndicatorConfig.indicatorName,
      dstIndicatorConfig.algoClass,
      indicatorResult.runStartTime,
      indicatorResult.runEndTime,
      indicatorResult.windowStartTime,
      indicatorResult.windowEndTime,
      indicatorResult.windowindDataCreateTime,
      indicatorResult.unit,
      indicatorResult.offlineIndicatorConfigList
    )
  }

  /**
   *  基础的参数计算
   */
  def baseMath(parameter: String, operator: String, indicatorDataMap: concurrent.TrieMap[String, String]): String = {
    var res_indicator_value = ""
    try {
      var indicator_value: scala.math.BigDecimal = 0.0
      var parameter1: scala.math.BigDecimal = 0.00
      var parameter2: scala.math.BigDecimal= 0.00
      val operator_list = operator.split(",")
      val parameter_list = parameter.split(",")

      def parameter_value(value: String): String = {
        if(value.contains("Number(")){
          value.replace("Number(", "").replace(")", "")
        }else{
          indicatorDataMap(value)
        }
      }

      for (i <- 1 until parameter_list.length) {

        if (i == 1) {
          parameter1 = BigDecimal(parameter_value(parameter_list(0)))
          parameter2 = BigDecimal(parameter_value(parameter_list(1)))
        } else {
          parameter1 = indicator_value
          parameter2 = BigDecimal(parameter_value(parameter_list(i)))
        }
        // 匹配计算 +-*/
        indicator_value = operator_list(i-1) match {
          case "+" => parameter1 + parameter2
          case "-" => parameter1 - parameter2
          case "*" => parameter1 * parameter2
          case "/" => if (parameter1 != 0) parameter1 / parameter2 else 0
        }
      }
      res_indicator_value = indicator_value.toString
    }catch {
      case ex: Exception => logger.error(s"CalculatedIndicatorJob_baseMath ERROR:  $ex")
        res_indicator_value = "error"
    }
    res_indicator_value
  }

  def generateKey(toolGroup: String, chamberGroupId: String, subRecipelistId: String): String = {
    s"$toolGroup#$chamberGroupId#$subRecipelistId"
  }

  def indicatorKey(toolid: String, chamberid: String, sensor: String): String = {
    s"$toolid#$chamberid#$sensor"
  }
}

