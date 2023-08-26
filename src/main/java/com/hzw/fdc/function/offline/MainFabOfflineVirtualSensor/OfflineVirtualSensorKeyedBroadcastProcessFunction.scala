package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor

import com.greenpineyu.fel.{FelEngine, FelEngineImpl}
import org.apache.flink.util.Collector
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{ErrorCode, OfflineVirtualSensorOpentsdbResult, OfflineVirtualSensorOpentsdbResultValue, OfflineVirtualSensorParam, PTSensorData, Point, VirtualSensorAlgoParam, VirtualSensorConfig}
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

class OfflineVirtualSensorKeyedBroadcastProcessFunction extends KeyedProcessFunction[String,
  OfflineVirtualSensorOpentsdbResult, OfflineVirtualSensorOpentsdbResult] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineVirtualSensorKeyedBroadcastProcessFunction])

  // {"toolName + chamberName + taskId + sensorAlias + svid": {"runId": List[Point}}
  val cacheVirtualSensorData = new concurrent.TrieMap[String, concurrent.TrieMap[String, List[Point]]]()

  //arithmetic缓存 {"toolName + chamberName + taskId + timestamp":{"svid": "value"}}}
  private val CacheArithmeticSensorData = new concurrent.TrieMap[String, concurrent.TrieMap[String, Double]]()


  override def processElement(value: OfflineVirtualSensorOpentsdbResult, ctx: KeyedProcessFunction[String,
    OfflineVirtualSensorOpentsdbResult, OfflineVirtualSensorOpentsdbResult]#Context,
                              out: Collector[OfflineVirtualSensorOpentsdbResult]): Unit = {

    var offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue("", 0.00, "", "", "", "")

    try {
      val offlineSensorData: OfflineVirtualSensorOpentsdbResultValue = value.data

      val unit = value.data.unit
      val toolName = value.toolName
      val chamberName = value.chamberName
      val key = s"${value.runId}|${value.taskId}|${value.timestamp.toString}"

      //缓存方便计算
      if(CacheArithmeticSensorData.contains(key)){
        val timestampMap = CacheArithmeticSensorData(key)
        timestampMap.put(offlineSensorData.svid, offlineSensorData.sensorValue)
        CacheArithmeticSensorData.put(key, timestampMap)
      }else{
        val sensorMap = concurrent.TrieMap[String, Double](offlineSensorData.svid -> offlineSensorData.sensorValue)
        CacheArithmeticSensorData.put(key, sensorMap)
      }

      val sensorMap = CacheArithmeticSensorData(key)
      val sensorSvidDataSet = sensorMap.keySet


      val virtualConfigSortList = value.virtualConfigList.sortBy(_.virtualSensorLevel)

      for(virtualConfig <- virtualConfigSortList) {

        val param = virtualConfig.param
        val algoClass = virtualConfig.algoClass

        try {
          val virtualSensorAliasName = virtualConfig.virtualSensorAliasName
          val virtualSvid = virtualConfig.svid
          val svid = virtualConfig.param.head.svidMap.getOrElse(toolName + "|" + chamberName, "")
          // 排序
          val paramListSort = param.sortWith(_.paramIndex < _.paramIndex)

          // calculated计算
          if (algoClass == "arithmetic") {
            val sensorSvidConfigSet = paramListSort
              .filter(x => {x.paramDataType == "SENSOR_ALIAS_ID"})
              .map(x => x.svidMap.get(toolName + "|" + chamberName))
              .filter(x => x.nonEmpty)
              .map(x => x.get).toSet

            if (sensorSvidDataSet == sensorSvidConfigSet) {
              // 解析配置
              val parseTmp = parseParam(paramListSort, toolName, chamberName)
              val parameter = parseTmp._1
              val operator = parseTmp._2

              // 基础的计算
              val calculatedResult: String = baseMath(parameter, operator, sensorMap)
              if (calculatedResult != "") {
                offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, calculatedResult.toDouble,
                  virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
                CacheArithmeticSensorData.remove(key)
              }
            }
          }

          if(algoClass == "customer"){

            var expression = ""
            val sensorSvidConfigList = new ListBuffer[(Int, String, String)]()
            for(param <- paramListSort){
              if(param.paramName == "expression"){
                expression = param.paramValue.replace("Math.", "$('Math').")
              }
              if(param.paramDataType == "SENSOR_ALIAS_ID"){
                if(param.svidMap.get(toolName + "|" + chamberName).nonEmpty){
                  sensorSvidConfigList.append((param.paramName.length, param.svidMap(toolName + "|" + chamberName), param.paramName))
                }
              }
            }

              var isContainAllSensor = true
              for(elem <- sensorSvidConfigList.sortWith(_._1 > _._1)){
                val paramSvid = elem._2
                val sensorAlias = elem._3
                if(!sensorMap.contains(paramSvid)){
                  isContainAllSensor = false
                }else{
                  expression  = expression.replace(s"${sensorAlias}", sensorMap(paramSvid).toString)
                }
              }

              // 是否包含所有的paramSvid
              if(isContainAllSensor) {
                try {
                  val sensorValue = FelEngine.instance.eval(expression)

                  offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, sensorValue.toString.toDouble,
                    virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
                  CacheArithmeticSensorData.remove(key)
                }catch {
                  case ex: Exception => logger.warn(s"${ex.toString} \t ${paramListSort} $expression")
                }
              }
          }

          // abs 求绝对值计算
          if (algoClass == "abs") {
            val paramSvid = paramListSort.head.paramValue
            val sensorValue = offlineSensorData.sensorValue.asInstanceOf[Number].doubleValue
            val absValue = sensorValue.abs

            offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, absValue,
              virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
          }

          // repout 计算
          if (algoClass == "repOut") {
            val sensorAlias = paramListSort.head.paramValue
            val sensorValue = offlineSensorData.sensorValue.asInstanceOf[Number].doubleValue

            // NA 不触发计算
            var upperLimitStatus = true
            val upperLimit = paramListSort(1).paramValue
            if (upperLimit.toString != "NA") {
              if (sensorValue > upperLimit.toDouble) {
                upperLimitStatus = false
              }
            }
            var lowerLimitStatus = true
            val lowerLimit = paramListSort(2).paramValue
            if (lowerLimit.toString != "NA") {
              if (sensorValue < lowerLimit.toDouble) {
                lowerLimitStatus = false
              }
            }

            if (upperLimitStatus && lowerLimitStatus) {
              offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, sensorValue,
                virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
            }else{
              if(value.lastSensorStatus){
                offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, Double.NaN,
                  virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
              }
            }
          }

          // movAvg 计算
          if (algoClass == "movAvg") {
            val paramSvid = svid
            val avgX = paramListSort(1).paramValue.toInt
            val cacheKey = s"${value.runId}|${value.taskId}|" + paramSvid + "|" + virtualSvid


            // 缓存sensorAlias
            val sensorValue = offlineSensorData.sensorValue.asInstanceOf[Number].doubleValue
            val runId = value.runId
            cacheFunction(paramSvid, avgX, sensorValue, cacheKey, value.timestamp, runId)

            // 开始计算
            val dataMap = cacheVirtualSensorData(cacheKey)
            val nowAvgList = dataMap(runId)
            val max = avgX.toInt
            if (max > nowAvgList.size) {
              logger.warn(s"AvgFunction AvgXInt > nowAvgList.size sensorMap:$value")
            } else {
              //提取列表的后n个元素
              val mathList: List[Point] = nowAvgList.takeRight(max)
              val Result: Double = mathList.map(_.value).sum / max.toDouble

              offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, Result,
                virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
            }
          }

          // slope 斜率计算
          if (algoClass == "slope") {
            val paramSvid = svid
            val avgX = paramListSort(1).paramValue.toInt
            val cacheKey = s"${value.runId}|${value.taskId}|" + paramSvid + "|" + virtualSvid

            // 缓存sensorAlias
            val sensorValue = offlineSensorData.sensorValue.asInstanceOf[Number].doubleValue
            val runId = value.runId
            cacheFunction(paramSvid, avgX, sensorValue, cacheKey, value.timestamp, runId)

            // 开始计算
            val dataMap = cacheVirtualSensorData(cacheKey)
            val nowAvgList = dataMap(runId)
            val max = avgX.toInt + 1
            if (max > nowAvgList.size) {
              logger.warn(s"AvgFunction AvgXInt > nowAvgList.size sensorMap:$value")
            } else {
              //提取列表的后n个元素
              val mathList: List[Point] = nowAvgList.takeRight(max)

              // 公式: Yn=(Xn-Xn-i)/(Tn-Tn-i)   Tn:秒数时间戳
              val Result: Double = (mathList.head.value - mathList.last.value) /
                ((mathList.head.timestamp - mathList.last.timestamp) / 1000.000)

              offlineVirtualSensorResult = OfflineVirtualSensorOpentsdbResultValue(virtualSvid, Result,
                virtualSensorAliasName, offlineSensorData.stepName, offlineSensorData.stepId, unit)
            }
          }
        } catch {
          case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
            Map("algoClass" -> algoClass, "离线计算报错" -> "virtual sensor", "value" -> value), ExceptionInfo.getExceptionInfo(exception)).toJson)
        }

        if(offlineVirtualSensorResult.svid != "") {
          val res = OfflineVirtualSensorOpentsdbResult(
            taskId=value.taskId,
            toolName=toolName,
            chamberName=chamberName,
            batchId=value.batchId,
            timestamp=value.timestamp,
            runId=value.runId,
            lastSensorStatus = value.lastSensorStatus,
            virtualConfigList=value.virtualConfigList,
            data=offlineVirtualSensorResult,
            errorMsg=""
          )

//          logger.warn("virtual task res: " + res)
          out.collect(res)
        }
      }
    } catch {
      case exception: Exception =>logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("离线计算报错" -> "virtual sensor"), exception.toString).toJson)
    }
  }


  /**
   * 缓存sensorAlias
   */
  def cacheFunction(sensorAlias: String, avgX: Int, sensorValue: Double, cacheKey: String, timestamp:Long, runId: String): Unit = {
    if (cacheVirtualSensorData.contains(cacheKey)) {
      val dataMap: concurrent.TrieMap[String, List[Point]] = cacheVirtualSensorData(cacheKey)

      if(dataMap.contains(runId)){
        val dataList: List[Point] = dataMap(runId)
        val newList = dataList :+ Point(timestamp, sensorValue)
        val dropList = if (newList.size > avgX + 1) {
          newList.drop(1)
        } else {
          newList
        }
        dataMap.put(runId,dropList)
      }else{
        dataMap.put(runId, List(Point(timestamp, sensorValue)))
      }

      cacheVirtualSensorData.put(cacheKey, dataMap)
    } else {
      val dataMap: concurrent.TrieMap[String, List[Point]] = concurrent.TrieMap[String, List[Point]](runId -> List(Point(timestamp, sensorValue)))
      cacheVirtualSensorData.put(cacheKey, dataMap)
    }
  }


  def parseParam(paramList: List[OfflineVirtualSensorParam],toolName:String, chamberName:String): (String, String) = {
    //根据paramIndex排序
    val paramListSort = paramList.sortWith(_.paramIndex < _.paramIndex)
    var parameter = ""
    var operator = ""
    for (param <- paramListSort){
      if(param.paramDataType == "OPERATOR"){
        if(operator.isEmpty)
          operator = param.paramValue
        else
          operator = operator + "," + param.paramValue
      }

      if(param.paramDataType == "SENSOR_ALIAS_ID"){
        if(parameter.isEmpty)
          parameter = param.svidMap(toolName + "|" + chamberName)
        else
          parameter = parameter + "," + param.svidMap(toolName + "|" + chamberName)
      }

      if(param.paramDataType == "NUMERIC" || param.paramDataType == "FLOAT"){
        if(parameter.isEmpty)
          parameter = "Number(" + param.paramValue + ")"
        else
          parameter = parameter + ",Number(" + param.paramValue + ")"
      }
    }
    (parameter, operator)
  }


  /**
   *  arithmetic的参数计算
   */
  def baseMath(parameter: String, operator: String, sensorAliasDataMap: concurrent.TrieMap[String, Double]): String = {
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
          sensorAliasDataMap(value).toString
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
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("function"->"baseMath", "sensorAliasDataMap" -> sensorAliasDataMap,
          "parameter" -> parameter, "operator"-> operator), ex.toString).toJson)
    }
    res_indicator_value
  }
}
