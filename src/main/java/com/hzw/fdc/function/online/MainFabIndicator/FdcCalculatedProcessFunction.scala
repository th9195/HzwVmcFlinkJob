package com.hzw.fdc.function.online.MainFabIndicator


import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

/**
 *  Calculated计算， 维度是run_id, 对同一run_id 的多个indicator计算
 */
class FdcCalculatedProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
  ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcCalculatedProcessFunction])


  // {"controlPlanId": {"version" : {"INDICATOR_ID": "IndicatorConfigScala"}}}
  var indicatorConfigByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String,
    IndicatorConfig]]]()

  // 缓存RUN_ID { "controlPlanId": {"RUN_ID": {"INDICATOR_ID": "INDICATOR_VALUE"} }}
  private var calculatedDataByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, String]]]()


  // 用于保存哪些indicator完成了计算  {"RUN_ID": ["INDICATOR_ID1"]}
  val finishIndicatorAll = new concurrent.TrieMap[String, scala.collection.mutable.Set[String]]()

  /** The state that is maintained by this process function */
  private var runIdState: ValueState[taskIdTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

//    val initIndicatorHbase = new InitIndicatorOracle()
    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

//    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList

    logger.warn(s"FdcCalculatedProcessFunction initConfig SIZE: " + initConfig.size)

    initConfig.foreach(kafkaDatas=> {
      kafkaDatas.`dataType` match {
        case "indicatorconfig" => addIndicatorConfigToTCSP(kafkaDatas)

        case _ => logger.warn(s"FdcCalculatedProcessFunction job open no mach type: " + kafkaDatas.`dataType`)
      }
    })

    val runIdStateDescription = new ValueStateDescriptor[taskIdTimestamp]("alarmIndicatorOutValueState",
      TypeInformation.of(classOf[taskIdTimestamp]))
    runIdState = getRuntimeContext.getState(runIdStateDescription)
  }


  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#OnTimerContext,
                       out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      runIdState.value() match {
        case taskIdTimestamp(cacheKey, lastModified)
          if (System.currentTimeMillis() >= lastModified + 1000 * 60 * 60 * ProjectConfig.RUN_MAX_LENGTH) => {
             val tmpKeyList = cacheKey.split("\\|")
             val controlPlanId = tmpKeyList.head
             val runId = tmpKeyList.last
             if(calculatedDataByAll.contains(controlPlanId)){
               val controlPlanMap = calculatedDataByAll(controlPlanId)
               if(controlPlanMap.contains(runId)){
                 controlPlanMap.remove(runId)
                 calculatedDataByAll.put(controlPlanId, controlPlanMap)
               }

               if(finishIndicatorAll.contains(runId)){
                 finishIndicatorAll.remove(runId)
               }
             }else{
               logger.warn(s"no contains controlPlanId: ${controlPlanId}")
             }
             close()
          }

        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("onTimer" -> "!!"), exception.toString).toString)
    }
  }

  override def processElement(record: FdcData[IndicatorResult], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
    ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#ReadOnlyContext, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "indicator" =>

          val myDatas = record.datas
          val indicatorResult = myDatas.toJson.fromJson[IndicatorResult]

          val controlPlanId = indicatorResult.controlPlanId.toString

          val controlPlanVersion = indicatorResult.controlPlanVersion.toString


          // RUN_ID: RNITool_31--YMTC_chamber--1604980730000
          val RUN_ID: String = indicatorResult.runId
          val INDICATOR_ID = indicatorResult.indicatorId.toString
          val INDICATOR_VALUE = indicatorResult.indicatorValue


          // 配置不存在
          if (!this.indicatorConfigByAll.contains(controlPlanId)) {
            return
          }
          val versionMap = indicatorConfigByAll(controlPlanId)
          if(!versionMap.contains(controlPlanVersion)){
            logger.warn(ErrorCode("004008d001C", System.currentTimeMillis(), Map("record" -> record, "exit controlPlanVersion"
              -> versionMap.keys, "match controlPlanVersion" -> controlPlanVersion, "type" -> "calculated"), "indicator_contains controlPlanVersion no exist").toJson)
            return
          }

          if(versionMap(controlPlanVersion).isEmpty){
            logger.warn(s"indicatorMap isEmpty: " + controlPlanId)
            return
          }

          if(!this.calculatedDataByAll.contains(controlPlanId)){
            val tmp = concurrent.TrieMap(RUN_ID -> concurrent.TrieMap[String, String](INDICATOR_ID -> INDICATOR_VALUE))
            this.calculatedDataByAll += (controlPlanId -> tmp)
            //logger.info(s"CalculatedIndicator_step2")
          }else{
            var calculatedDataMap = this.calculatedDataByAll(controlPlanId)

            // 添加INDICATOR_ID和INDICATOR_VALUE
            if (!calculatedDataMap.contains(RUN_ID)) {
              val INDICATOR_ID_data = concurrent.TrieMap[String, String](INDICATOR_ID -> INDICATOR_VALUE)
              calculatedDataMap += (RUN_ID -> INDICATOR_ID_data)
              this.calculatedDataByAll.update(controlPlanId, calculatedDataMap)
            } else {
              var indicatorDataMap = calculatedDataMap(RUN_ID)
              indicatorDataMap += (INDICATOR_ID -> INDICATOR_VALUE)
              calculatedDataMap.update(RUN_ID, indicatorDataMap)
              this.calculatedDataByAll.update(controlPlanId, calculatedDataMap)
            }

            // 开始计算
            val indicatorDataMap = calculatedDataMap(RUN_ID)

            //            if(indicatorConfigSet == indicatorDataMap.keySet){
            val IndicatorResultScalaList = new ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()

            val indicatorConfigMap = indicatorConfigByAll(controlPlanId)
            val versionMap = indicatorConfigMap(controlPlanVersion)
            for(indicatorConfig <- versionMap.values){
              var parameter = ""
              var operator = ""
              val algoParam = indicatorConfig.algoParam

              val indicatorId = indicatorConfig.indicatorId.toString
              // 判断是否已经计算过 如果计算过就不有需要再计算了
              if(!judgeIsCalced(indicatorId,RUN_ID)){
                try{
                  val tmp = algoParam.split("\\|")
                  parameter = tmp(0)
                  operator = tmp(1)
                }catch {
                  case ex: Exception => logger.warn(s"CalculatedIndicator algoParam: $ex")
                }

                try {
                  val key = s"${controlPlanId}|${RUN_ID}"
                  // write the state back
                  runIdState.update(taskIdTimestamp(key, System.currentTimeMillis()))

                  // schedule the next timer 30 seconds from the current event time
                  ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 1000 * 60 * 60 * ProjectConfig.RUN_MAX_LENGTH)
                }catch {
                  case ex: Exception => logger.warn("processEnd registerProcessingTimeTimer error: " + ex.toString)
                }

                // 判断是否开始计算
                if(isCalculated(parameter, indicatorDataMap.keys.toList)) {

                  //                logger.info(s"CalculatedIndicator_step5:\t" +parameter + "\toperator\t: "+ operator)

                  // 基础的计算
                  val indicator_value: String = baseMath(parameter, operator, indicatorDataMap)

                  // 封装返回结果的对象
                  val IndicatorResultScala = resIndicatorResultScala(indicatorResult, indicator_value, indicatorConfig)
                  IndicatorResultScalaList.append((IndicatorResultScala, indicatorConfig.driftStatus, indicatorConfig.calculatedStatus, indicatorConfig.logisticStatus))

//                  // 判断RUN_ID 的所有indicator是否都计算完成
//                  if(isRunIDFinishCalculated(versionMap, RUN_ID, indicatorConfig)){
//                    calculatedDataMap.remove(RUN_ID)
//                    this.calculatedDataByAll.put(controlPlanId, calculatedDataMap)
//                  }

                  // 记录计算过的Indicator
                  addFinishIndicatorAll(RUN_ID,indicatorId)
                }
              }
            }
            // 清理缓存
            clearCatch(versionMap,controlPlanId, RUN_ID)
            out.collect(IndicatorResultScalaList)
          }
        case _ => logger.warn(s"CalculatedIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("004008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "calculated"), s"indicator job error $ex").toJson)
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" => addIndicatorConfigToTCSP(value)

      case _ => logger.warn(s"CalculatedIndicator job processBroadcastElement no mach type")
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
   * 判断是否已经计算过了
   * @param indicatorId
   * @param controlPlanId
   * @param RUN_ID
   * @return
   */
  def judgeIsCalced(indicatorId: String, RUN_ID: String): Boolean = {
    var res = false
    if(finishIndicatorAll.contains(RUN_ID)){
      val indicatorDataSet: mutable.Set[String] = finishIndicatorAll(RUN_ID)
      if(indicatorDataSet.contains(indicatorId)){
        res = true
      }
    }
    res
  }

  /**
   * 每计算一个 Indicator 都需要记录
   * @param RUN_ID
   * @param indicatorId
   * @return
   */
  def addFinishIndicatorAll(RUN_ID: String, indicatorId: String) = {
    if(finishIndicatorAll.contains(RUN_ID)){
      val indicatorIdSet: mutable.Set[String] = finishIndicatorAll(RUN_ID)
      indicatorIdSet += indicatorId
      finishIndicatorAll.put(RUN_ID,indicatorIdSet)
    }else{
      finishIndicatorAll.put(RUN_ID,mutable.Set(indicatorId))
    }
  }

  /**
   * 如果全部Indicator计算完，清理缓存
   * @param indicatorConfigMap
   * @param controlPlanId
   * @param RUN_ID
   * @return
   */
  def clearCatch(indicatorConfigMap: TrieMap[String, IndicatorConfig], controlPlanId: String, RUN_ID: String) = {
    if(finishIndicatorAll.contains(RUN_ID)){
      val indicatorIdSet: mutable.Set[String] = finishIndicatorAll(RUN_ID)
      // 判断是否全部计算完
      if(indicatorConfigMap.keySet.subsetOf(indicatorIdSet)) {

        finishIndicatorAll.remove(RUN_ID)
        logger.warn(s"isRunIDFinishCalculated run == ${RUN_ID} ; indicatorIdSet == ${indicatorIdSet}")

        if(calculatedDataByAll.contains(controlPlanId)){
          val calculatedDataMap = calculatedDataByAll(controlPlanId)
          if(calculatedDataMap.contains(RUN_ID)){
            calculatedDataMap.remove(RUN_ID)
            calculatedDataByAll.put(controlPlanId,calculatedDataMap)
          }
        }
      }
    }
  }


  /**
   *  判断RUN_ID 的所有indicator是否都计算完成
   */
  def isRunIDFinishCalculated(indicatorConfigMap: concurrent.TrieMap[String, IndicatorConfig], RUN_ID: String,
                              indicatorConfig: IndicatorConfig):Boolean = {
    var Res = false
    try{
      val indicatorId = indicatorConfig.indicatorId.toString

      if(finishIndicatorAll.contains(RUN_ID)){
        var indicatorDataSet: mutable.Set[String] = finishIndicatorAll(RUN_ID)

        indicatorDataSet.add(indicatorId)
        finishIndicatorAll.update(RUN_ID, indicatorDataSet)

        // 判断是否全部计算完
        if(indicatorConfigMap.keySet.subsetOf(indicatorDataSet)) {
          Res = true
          finishIndicatorAll.remove(RUN_ID)
          logger.warn(s"isRunIDFinishCalculated run: ${RUN_ID}")
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
   *   过滤过期的RUN_ID, 找到TOO_ID数量最多的一个,删除其对应的部分RUN_ID
   */
  def filterRunIdTime(calculatedDataMap: concurrent.TrieMap[String, concurrent.TrieMap[String, String]]):
  concurrent.TrieMap[String, concurrent.TrieMap[String, String]] = {
    try {
      // {"TOOLD_ID": NUM}
      var Tmp:mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

      val RUN_ID_LIST = calculatedDataMap.keys.toList
      for (elem <- RUN_ID_LIST){
        val toolid = elem.split("--").head
        if (!Tmp.contains(toolid)){
          Tmp += (toolid -> 1)
        }else{
          Tmp.update(toolid, Tmp(toolid) + 1)
        }
      }
      var max = ("", 0)
      for (ele <- Tmp){
        if(ele._2 > max._2){
          max = ele
        }
      }
      for (elem <- RUN_ID_LIST.sorted.reverse.take(400)){
        if(elem.contains(max._1)){
          calculatedDataMap.remove(elem)
          logger.warn(s"filterRunIdTime_delete RUN_ID: " + elem)
        }
      }
    }catch {
      case ex: Exception => logger.error(s"ERROR filterRunIdTime:  $ex")
    }
    calculatedDataMap
  }

  /**
   *  返回IndicatorResultScala对象
   */
  def resIndicatorResultScala(indicatorResult: IndicatorResult, indicator_value: String,
                              dstIndicatorConfig: IndicatorConfig):IndicatorResult={
    IndicatorResult(
      controlPlanId = indicatorResult.controlPlanId,
      controlPlanName = indicatorResult.controlPlanName,
      controlPlanVersion = indicatorResult.controlPlanVersion,
      locationId = indicatorResult.locationId,
      locationName = indicatorResult.locationName,
      moduleId = indicatorResult.moduleId,
      moduleName = indicatorResult.moduleName,
      toolGroupId = indicatorResult.toolGroupId,
      toolGroupName = indicatorResult.toolGroupName,
      chamberGroupId = indicatorResult.chamberGroupId,
      chamberGroupName = indicatorResult.chamberGroupName,
      recipeGroupName =   indicatorResult.recipeGroupName,
      runId = indicatorResult.runId,
      toolName = indicatorResult.toolName,
      toolId = indicatorResult.toolId,
      chamberName = indicatorResult.chamberName,
      chamberId = indicatorResult.chamberId,
      indicatorValue=indicator_value,
      indicatorId = dstIndicatorConfig.indicatorId,
      indicatorName = dstIndicatorConfig.indicatorName,
      algoClass = dstIndicatorConfig.algoClass,
      indicatorCreateTime = indicatorResult.indicatorCreateTime,
      missingRatio = indicatorResult.missingRatio,
      configMissingRatio = indicatorResult.configMissingRatio,
      runStartTime = indicatorResult.runStartTime,
      runEndTime = indicatorResult.runEndTime,
      windowStartTime = indicatorResult.windowStartTime,
      windowEndTime = indicatorResult.windowEndTime,
      windowDataCreateTime = indicatorResult.windowDataCreateTime,
      limitStatus = indicatorResult.limitStatus,
      materialName = indicatorResult.materialName,
      recipeName = indicatorResult.recipeName,
      recipeId = indicatorResult.recipeId,
      product = indicatorResult.product,
      stage = indicatorResult.stage,
      bypassCondition = dstIndicatorConfig.bypassCondition,
      pmStatus = indicatorResult.pmStatus,
      pmTimestamp = indicatorResult.pmTimestamp,
      area = indicatorResult.area,
      section = indicatorResult.section,
      mesChamberName = indicatorResult.mesChamberName,
      lotMESInfo = indicatorResult.lotMESInfo,
      dataVersion =indicatorResult.dataVersion,
      cycleIndex = indicatorResult.cycleIndex,
      unit =  ""
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

  def addIndicatorConfigToTCSP(kafkaConfigData: ConfigData[IndicatorConfig]): Unit = {
    try {

      val indicatorConfig = kafkaConfigData.datas
      val key = indicatorConfig.controlPlanId.toString

      val version = indicatorConfig.controlPlanVersion.toString

      if(!indicatorConfig.algoClass.equals("calculated")){
//        logger.error(s"addIndicatorConfigToTCSP is no Calculated class: " + indicatorConfig.ALGO_CLASS)
        return
      }

      val INDICATOR_ID = indicatorConfig.indicatorId.toString

      if (!kafkaConfigData.status) {

        // 删除indicatorConfig逻辑
        if (this.indicatorConfigByAll.contains(key)) {

          //有一样的key
          val versionAndIndicator = this.indicatorConfigByAll(key)

          if (versionAndIndicator.contains(version)) {
            val indicatorKey = versionAndIndicator(version)

            if (indicatorKey.contains(INDICATOR_ID)) {

              indicatorKey.remove(INDICATOR_ID)

              versionAndIndicator.put(version, indicatorKey)
              this.indicatorConfigByAll += (key -> versionAndIndicator)
            }

            if (indicatorKey.isEmpty) {
              this.indicatorConfigByAll.remove(key)
            }
            //logger.info(s"CalculatedIndicatorTOTCSP_step2" + indicatorKey)
          } else {
            logger.warn(s"CalculatedIndicator Config version no exist: " + indicatorConfig)
          }
        }
      } else {
        // 新增逻辑
        if (this.indicatorConfigByAll.contains(key)) {
          val versionAndIndicatorMap = this.indicatorConfigByAll(key)

          if (versionAndIndicatorMap.contains(version)) {
            val algoMap = versionAndIndicatorMap(version)
            algoMap += (INDICATOR_ID -> indicatorConfig)
            versionAndIndicatorMap += (version -> algoMap)
          } else {
            val algoToScala = concurrent.TrieMap[String, IndicatorConfig](INDICATOR_ID -> indicatorConfig)
            versionAndIndicatorMap += (version -> algoToScala)
          }

          // 更新版本
          val k = versionAndIndicatorMap.keys.map(_.toLong)
          if (k.size > 2) {
            val minVersion = k.toList.min
            versionAndIndicatorMap.remove(minVersion.toString)
            logger.warn(s"addIndicatorConfigToTCSP: " + minVersion + "\tindicatorConfig: " + indicatorConfig)
          }
          this.indicatorConfigByAll += (key -> versionAndIndicatorMap)

        } else {
          // 没有一样的key
          val indicatorToScala = concurrent.TrieMap[String, IndicatorConfig](INDICATOR_ID -> indicatorConfig)
          val versionScala = concurrent.TrieMap[String, concurrent.TrieMap[String, IndicatorConfig]](version -> indicatorToScala)
          this.indicatorConfigByAll += (key -> versionScala)
        }
      }
    }catch {
      case ex: Exception => logger.error(s"CalculatedIndicatorJob_addIndicatorConfigToTCSP ERROR:  $ex")
    }
  }
}
