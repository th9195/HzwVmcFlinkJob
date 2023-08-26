package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.engine.alg.AlgorithmUtils
import com.hzw.fdc.engine.alg.AlgorithmUtils.DataPoint
import com.hzw.fdc.function.online.MainFabIndicator.{IndicatorCommFunction, InitIndicatorOracle}
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}


/**
 * 需开发一种算法来对某个Indicator的trend down/up的信号进行抓取，
 * 整个trend过程中可能会有异常点波动，但是这些波动不需影响trenddown up的信号获取。
 */
class FdcLinearFitIndicatorProcessFunction extends KeyedBroadcastProcessFunction[String, JsonNode,
  JsonNode, ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcLinearFitIndicatorProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": { "version": { "LinearFitX" : "IndicatorConfig"}}}}
  var LinearFitIndicatorByAll: TrieMap[String, TrieMap[String, TrieMap[String,
    TrieMap[String, IndicatorConfig]]]] = IndicatorByAll

  //数据缓存: indicatorid ->((tool + chamber) or (tool+chamber+recipe) ->LinearFitList)
  var LinearFitValue = new concurrent.TrieMap[String, concurrent.TrieMap[String, List[DataPoint]]]()

  val calcType = "linearFit"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    //val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList
    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

    initConfig.foreach(kafkaDatas=> {
      kafkaDatas.`dataType` match {
        case "indicatorconfig" =>
          addIndicatorConfigByAll(kafkaDatas, "linearFit")

        case _ => logger.warn(s"LinearFitIndicator job open no mach type: " + kafkaDatas.`dataType`)
      }
    })

//    logger.warn("LinearFitIndicatorByAll: " + LinearFitIndicatorByAll)

    // 通过Redis初始化上次结果数据
    if(ProjectConfig.INIT_ADVANCED_INDICATOR_FROM_REDIS){
      initLinearFitAdvancedIndicator(calcType,this.LinearFitValue)
//      logger.warn(s"LinearFitValue == ${LinearFitValue.toJson}")
    }
  }

  override def processElement(record: JsonNode, ctx: KeyedBroadcastProcessFunction[String,
    JsonNode, JsonNode, ListBuffer[(IndicatorResult, Boolean, Boolean,
    Boolean)]]#ReadOnlyContext, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {

      record.findPath(MainFabConstants.dataType).asText() match {
        case "indicator" =>

          val value = toBean[FdcData[IndicatorResult]](record)
          val myDatas = value.datas

          val indicatorResult = myDatas.toJson.fromJson[IndicatorResult]
          val version = indicatorResult.controlPlanVersion.toString
          val indicatorKey = indicatorResult.indicatorId.toString

          if (!this.LinearFitIndicatorByAll.contains(indicatorKey)) {
            return
          }
          val controlPlanIdMap = LinearFitIndicatorByAll(indicatorKey)
          for(versionMap <- controlPlanIdMap.values) {

            breakable {
              if (!versionMap.contains(version)) {
                logger.warn(ErrorCode("006008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                  -> versionMap.keys, "match version" -> version, "type" -> "linearFit"), "indicator_contains version no exist").toJson)
                break
              }
            }
            val LinearFitIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version)


            val LinearFitX = LinearFitIndicator.keys.map(_.toInt).max
            val indicatorValueDouble = indicatorResult.indicatorValue.toDouble
            val w2wType = LinearFitIndicator(LinearFitX.toString).w2wType


            val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()
            for ((movAvgX, indicatorConfigScala) <- LinearFitIndicator) {

              val confIndicatorId = indicatorConfigScala.indicatorId.toString

//              LinearFitCacheHistoryData(w2wType, indicatorResult, confIndicatorId, indicatorValueDouble,
//                LinearFitX, this.LinearFitValue)

              val isCalc = linearFitCacheHistoryDataAndRedis(w2wType,
                indicatorResult,
                confIndicatorId,
                indicatorValueDouble,
                LinearFitX,
                this.LinearFitValue,
                calcType,
                ctx)

              if(isCalc){
                try {
                  val res = MovAvgFunction(movAvgX, indicatorResult, indicatorConfigScala)
                  if (res._1 != null) {
                    IndicatorList.append(res)
                  }
                } catch {
                  case ex: Exception => logger.warn(s"MovAvgFunction error $ex  config:$indicatorConfigScala  indicatorResult:$indicatorResult ")
                }
              }
            }
            out.collect(IndicatorList)
          }
        case "ClearLinearFit" =>

          val value = toBean[FdcData[clearLinearFit]](record)
          val indicatorId = value.datas.indicatorId
          if(LinearFitValue.contains(indicatorId.toString)){
            LinearFitValue.remove(indicatorId.toString)
          }
        case _ => logger.warn(s"LinearFitIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("006008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "linearFit"), s"alarm job error $ex").toJson)
    }
  }

  override def processBroadcastElement(record: JsonNode, ctx: KeyedBroadcastProcessFunction[String,
    JsonNode, JsonNode, ListBuffer[(IndicatorResult, Boolean,
    Boolean, Boolean)]]#Context, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    record.findPath(MainFabConstants.dataType).asText() match {
      case "indicatorconfig" =>
        val value = toBean[ConfigData[IndicatorConfig]](record)
        addIndicatorConfigByAll(value, "linearFit")

      case _ => None
    }
  }



  /**
   *  计算MovAvg值
   */
  def MovAvgFunction(movAvgX: String, indicatorResult: IndicatorResult, indicatorConfigScala: IndicatorConfig
                    ): (IndicatorResult, Boolean, Boolean, Boolean) = {

    val nowMovAvgList: List[DataPoint] = getLinearFitCacheValue(indicatorResult, indicatorConfigScala, this.LinearFitValue)

    val max = movAvgX.toInt + 1
    if(max > nowMovAvgList.size){
      logger.warn(s"LinearFitFunction linearFit > linearFit.size config:$indicatorConfigScala  indicatorResult:$indicatorResult")
      return (null,indicatorConfigScala.driftStatus, indicatorConfigScala.calculatedStatus, indicatorConfigScala.logisticStatus)
    }

    //提取列表的后n个元素
    val mathList: List[DataPoint] = nowMovAvgList.takeRight(max)
    val Result: Double = AlgorithmUtils.linearFit(mathList.toArray: _*)


    val movAvgIndicator = IndicatorResult(
      indicatorResult.controlPlanId,
      indicatorResult.controlPlanName,
      indicatorResult.controlPlanVersion,
      indicatorResult.locationId,
      indicatorResult.locationName,
      indicatorResult.moduleId,
      indicatorResult.moduleName,
      indicatorResult.toolGroupId,
      indicatorResult.toolGroupName,
      indicatorResult.chamberGroupId,
      indicatorResult.chamberGroupName,
      indicatorResult.recipeGroupName,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.toolId,
      indicatorResult.chamberName,
      indicatorResult.chamberId,
      Result.toString,
      indicatorConfigScala.indicatorId,
      indicatorConfigScala.indicatorName,
      indicatorConfigScala.algoClass,
      indicatorResult.indicatorCreateTime,
      indicatorResult.missingRatio,
      indicatorResult.configMissingRatio,
      indicatorResult.runStartTime,
      indicatorResult.runEndTime,
      indicatorResult.windowStartTime,
      indicatorResult.windowEndTime,
      indicatorResult.windowDataCreateTime,
      indicatorResult.limitStatus,
      indicatorResult.materialName,
      indicatorResult.recipeName,
      indicatorResult.recipeId,
      indicatorResult.product,
      indicatorResult.stage,
      bypassCondition=indicatorResult.bypassCondition,
      pmStatus=indicatorResult.pmStatus,
      pmTimestamp=indicatorResult.pmTimestamp,
      area = indicatorResult.area,
      section = indicatorResult.section,
      mesChamberName = indicatorResult.mesChamberName,
      indicatorResult.lotMESInfo,
      dataVersion = indicatorResult.dataVersion,
      cycleIndex = indicatorResult.cycleIndex,
      unit =""
    )

    (movAvgIndicator,indicatorConfigScala.driftStatus, indicatorConfigScala.calculatedStatus, indicatorConfigScala.logisticStatus)
  }


  /**
   *
   * @param w2wType
   * @param indicatorResult
   * @param confIndicatorId
   * @param indicatorValueDouble
   * @param LinearFitX
   * @param LinearFitValue
   * @param calcType
   * @param ctx
   */
  def linearFitCacheHistoryDataAndRedis(w2wType: String,
                                        indicatorResult: IndicatorResult,
                                        confIndicatorId: String,
                                        indicatorValueDouble: Double,
                                        linearFitX: Int,
                                        cacheHistoryValue: TrieMap[String, TrieMap[String, List[DataPoint]]],
                                        calcType: String,
                                        ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#ReadOnlyContext):Boolean = {

    var res = true // 解决重启后会重复消费数据的问题

    var toolMap = new concurrent.TrieMap[String, List[DataPoint]]()

    val toolName = indicatorResult.toolName
    val chamberName = indicatorResult.chamberName
    val recipeName = indicatorResult.recipeName

    // 解析 By Tool-Chamber-Recipe-Product-Stage
    var key = toolName + "|" + chamberName
    val productKeyList: ListBuffer[String] = ListBuffer()
    val stageKeyList: ListBuffer[String] = ListBuffer()


    if(w2wType.contains("Recipe")){
      key = key + "|" + recipeName
    }

    if(w2wType.contains("Product")){
      for(product <- indicatorResult.product){
        productKeyList.append(key + "|" + product)
      }
    }

    if(w2wType.contains("Stage")){
      // 如果包含了Product
      if(w2wType.contains("Product")) {
        for(stage <- indicatorResult.stage) {
          for (productKey <- productKeyList) {
            stageKeyList.append(productKey + "|" + stage)
          }
        }
      }else{
        for(stage <- indicatorResult.stage) {
          stageKeyList.append(key + "|" + stage)
        }
      }
    }


    def addData(w2wKey: String): Unit = {
      // 判断indicatorId 之前是否存在
      val dataPointList = ListBuffer[DataPoint]()
      val newDataPoint = new DataPoint(indicatorResult.runStartTime, indicatorValueDouble)

      if (!cacheHistoryValue.contains(confIndicatorId)) {
        toolMap += w2wKey -> List(newDataPoint)
        dataPointList.append(newDataPoint)
        cacheHistoryValue += confIndicatorId -> toolMap
      }else{
        toolMap = cacheHistoryValue(confIndicatorId)

        // 判断toolName, chamberName, recipe, product, stage之前是否存在
        if (!toolMap.contains(w2wKey)) {
          toolMap += w2wKey -> List(newDataPoint)
          dataPointList.append(newDataPoint)
          cacheHistoryValue += confIndicatorId -> toolMap
        }else{
          val oldDataPointList: List[DataPoint] = toolMap(w2wKey)

          // 处理重启情况下重复消费数据的问题
          oldDataPointList.foreach(dataPoint => {
            val time = dataPoint.time
            // 如果 新进入的数据runStartTime 有早于缓存里面的时间，说明重复消费历史数据了
            if(time >= indicatorResult.runStartTime){
              logger.error(s"当前indicator数据已经被处理过! indicatorResult = ${indicatorResult.toJson}")
              res = false
            }
          })

          if(res){
            // 更新缓存
            val newDataPointList = oldDataPointList :+ newDataPoint
            val dropKeyList: List[DataPoint] = if (newDataPointList.size > linearFitX + 1) { newDataPointList.drop(1) } else { newDataPointList }
            dataPointList ++= dropKeyList
            toolMap += w2wKey -> dropKeyList

            cacheHistoryValue += confIndicatorId -> toolMap
          }
        }
      }

      // todo 组装Redis缓存数据 并 侧道输出
      if(res){
        try{
          val advancedLinearFitIndicatorCacheData = AdvancedLinearFitIndicatorCacheData(baseIndicatorId = confIndicatorId,
            w2wKey = w2wKey,
            dataPointList = dataPointList.toList,
            dataVersion = indicatorResult.dataVersion,
            configVersion = ProjectConfig.JOB_VERSION)

          val advancedIndicatorCacheDataJsonNode: JsonNode =
            beanToJsonNode[RedisCache[AdvancedLinearFitIndicatorCacheData]](RedisCache(dataType = "advancedIndicator_" + calcType,
              datas = advancedLinearFitIndicatorCacheData))

          ctx.output(advancedIndicatorCacheDataOutput,advancedIndicatorCacheDataJsonNode)
        }catch {
          case e:Exception => {
            logger.error(s"output linearFitCacheHistoryDataAndRedis error ! \n " +
              s"calcType == ${calcType}; \n" +
              s"baseIndicatorId == ${confIndicatorId} ; \n " +
              s"w2wKey == ${w2wKey}; \n" +
              s"dataPointList == ${dataPointList.toJson}; \n " +
              s"${e.printStackTrace()}")
          }
        }
      }

    }


    if(w2wType.contains("Stage")){
      for(stageKey <- stageKeyList) {
        addData(stageKey)
      }
    }else if(w2wType.contains("Product")){
      for (productKey <- productKeyList) {
        addData(productKey)
      }
    }else{
      addData(key)
    }


    res
  }

  def LinearFitCacheHistoryData(w2wType: String, indicatorResult: IndicatorResult, indicatorKey: String,
                                indicatorValueDouble: Double, X: Int, cacheHistoryValue: concurrent.TrieMap[String, concurrent.TrieMap[String, List[DataPoint]]]):  Unit = {
    var toolMap = new concurrent.TrieMap[String, List[DataPoint]]()

    val toolName = indicatorResult.toolName
    val chamberName = indicatorResult.chamberName
    val recipeName = indicatorResult.recipeName



    // 解析 By Tool-Chamber-Recipe-Product-Stage
    var key = toolName + "|" + chamberName
    val productKeyList: ListBuffer[String] = ListBuffer()
    val stageKeyList: ListBuffer[String] = ListBuffer()


    if(w2wType.contains("Recipe")){
      key = key + "|" + recipeName
    }

    if(w2wType.contains("Product")){
      for(product <- indicatorResult.product){
        productKeyList.append(key + "|" + product)
      }
    }

    if(w2wType.contains("Stage")){
      // 如果包含了Product
      if(w2wType.contains("Product")) {
        for(stage <- indicatorResult.stage) {
          for (productKey <- productKeyList) {
            stageKeyList.append(productKey + "|" + stage)
          }
        }
      }else{
        for(stage <- indicatorResult.stage) {
          stageKeyList.append(key + "|" + stage)
        }
      }
    }


    def addData(Key: String): Unit = {
      // 判断indicatorId 之前是否存在
      if (!cacheHistoryValue.contains(indicatorKey)) {
        toolMap += Key -> List(new DataPoint(indicatorResult.runStartTime, indicatorValueDouble))
        cacheHistoryValue += indicatorKey -> toolMap
      }else{
        toolMap = cacheHistoryValue(indicatorKey)

        // 判断toolName, chamberName, recipe, product, stage之前是否存在
        if (!toolMap.contains(Key)) {
          toolMap += Key -> List(new DataPoint(indicatorResult.runStartTime, indicatorValueDouble))
          cacheHistoryValue += indicatorKey -> toolMap
        }else{
          val KeyList: List[DataPoint] = toolMap(Key)
          val newKeyList = KeyList :+ new DataPoint(indicatorResult.runStartTime, indicatorValueDouble)
          val dropKeyList = if (newKeyList.size > X + 1) { newKeyList.drop(1) } else { newKeyList }
          toolMap += Key -> dropKeyList

          cacheHistoryValue += indicatorKey -> toolMap
        }
      }
    }


    if(w2wType.contains("Stage")){
      for(stageKey <- stageKeyList) {
        addData(stageKey)
      }
    }else if(w2wType.contains("Product")){
      for (productKey <- productKeyList) {
        addData(productKey)
      }
    }else{
      addData(key)
    }

  }



  /**
   * 获取缓存的历史数据，支持ByTool-Chamber-Recipe和By Tool-Chamber
   */
  def getLinearFitCacheValue(indicatorResult: IndicatorResult, config: IndicatorConfig, cacheHistoryValue:
  concurrent.TrieMap[String, concurrent.TrieMap[String, List[DataPoint]]]): List[DataPoint] = {

    val toolMap = cacheHistoryValue(config.indicatorId.toString)

    val w2wType = config.w2wType
    val chamberName = indicatorResult.chamberName
    val recipeName = indicatorResult.recipeName

    var key = indicatorResult.toolName + "|" + chamberName
    if(w2wType.contains("Recipe")){
      key = key + "|" + recipeName
    }

    if(w2wType.contains("Product")){
      key = key + "|" + indicatorResult.product.head
    }

    if(w2wType.contains("Stage")){
      key = key + "|" + indicatorResult.stage.head
    }

    val ValueList: List[DataPoint] = toolMap(key)
    ValueList
  }

}

