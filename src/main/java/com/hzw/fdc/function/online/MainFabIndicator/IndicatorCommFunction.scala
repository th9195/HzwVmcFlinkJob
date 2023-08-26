package com.hzw.fdc.function.online.MainFabIndicator


import akka.actor.FSM.->
import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.TDao
import com.hzw.fdc.engine.alg.AlgorithmUtils.DataPoint
import com.hzw.fdc.function.PublicFunction.FdcKafkaSchema
import com.hzw.fdc.json.JsonUtil.beanToJsonNode
import com.hzw.fdc.scalabean.{AdvancedIndicatorCacheData, AdvancedLinearFitIndicatorCacheData, AlarmEwmaCacheData, ConfigData, ErrorCode, FdcData, IndicatorConfig, IndicatorResult, KafkaIndicator, RedisCache}
import com.hzw.fdc.util.ProjectConfig
import com.hzw.fdc.util.redisUtils.RedisUtil
import com.hzw.fdc.util.redisUtils.RedisUtil.getStringDataByPrefixKey
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

trait IndicatorCommFunction extends TDao {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcDriftConfigBroadcastProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": {"version": { "driftX" : "IndicatorConfig"}}}}
  var IndicatorByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String,
    concurrent.TrieMap[String, IndicatorConfig]]]]()

  // ewma 缓存数据侧道输出
  lazy val advancedIndicatorCacheDataOutput = new OutputTag[JsonNode]("advancedIndicatorCacheData")

  def parseOption[A](a: Option[A]): A = {
    a.get
  }

  def Try[A](a: => A): Option[A] = {
    try Some(a)
    catch { case e: Exception => None}
  }

  /**
   *  获取全量的和增量indicator配置信息
   */
  def addIndicatorConfigByAll(kafkaConfigData: ConfigData[IndicatorConfig], Type:String): Unit = {
    try {


      val indicatorConfig = kafkaConfigData.datas

      val version = indicatorConfig.controlPlanVersion.toString
      val controlPlanId = indicatorConfig.controlPlanId.toString

      if(!indicatorConfig.algoClass.equals(Type)){
        return
      }

      logger.warn(s"addIndicatorConfigByAll_step1: " + kafkaConfigData )

      //key为原始indicator
      val algoParam = indicatorConfig.algoParam
      var key = ""
      var driftX = ""
      try {
        val algoParamList = algoParam.split("\\|")
        key = algoParamList(0)
        if (algoParamList.size >= 2) {
          driftX = algoParamList(1)
        }
      }catch {
        case ex: Exception => logger.info(s"flatMapOutputStream algoParamList")
      }


      if (!kafkaConfigData.status) {

        //删除
        if (IndicatorByAll.contains(key)) {
          val controlPlanIdMap = IndicatorByAll(key)
          if(controlPlanIdMap.contains(controlPlanId)) {

            val versionAndIndicatorMap = controlPlanIdMap(controlPlanId)
            if (versionAndIndicatorMap.contains(version)) {

              val driftXAndIndicator = versionAndIndicatorMap(version)

              if (driftXAndIndicator.contains(driftX)) {
                driftXAndIndicator.remove(driftX)
                versionAndIndicatorMap += (version -> driftXAndIndicator)
                controlPlanIdMap.put(controlPlanId, versionAndIndicatorMap)
                IndicatorByAll += (key -> controlPlanIdMap)
              }

              if (driftXAndIndicator.isEmpty) {
                IndicatorByAll.remove(key)
              }
            } else {
              logger.warn(s"addIndicatorConfigByAll version no exist: " + kafkaConfigData)
            }
          }
        }else{
          logger.warn(s"addIndicatorConfigByAll key no exist: " + kafkaConfigData )
        }
      } else {
        //新增逻辑
        if (IndicatorByAll.contains(key)) {

          val controlPlanIdMap = IndicatorByAll(key)
          if(controlPlanIdMap.contains(controlPlanId)) {

            val versionAndIndicatorMap = controlPlanIdMap(controlPlanId)

            if (versionAndIndicatorMap.contains(version)) {
              val algoMap = versionAndIndicatorMap(version)
              algoMap += (driftX -> indicatorConfig)
              versionAndIndicatorMap += (version -> algoMap)
            } else {
              val algoToScala = concurrent.TrieMap[String, IndicatorConfig](driftX -> indicatorConfig)
              versionAndIndicatorMap += (version -> algoToScala)
            }

            // 更新版本
            val k = versionAndIndicatorMap.keys.map(_.toLong)
            if(k.size > 2){
              val minVersion = k.toList.min
              versionAndIndicatorMap.remove(minVersion.toString)
            }
            controlPlanIdMap.put(controlPlanId, versionAndIndicatorMap)
            IndicatorByAll += (key -> controlPlanIdMap)

          }else{
            val driftXAndIndicator = concurrent.TrieMap[String, IndicatorConfig](driftX -> indicatorConfig)
            val versionScala = concurrent.TrieMap[String, concurrent.TrieMap[String,
              IndicatorConfig]](version -> driftXAndIndicator)
            controlPlanIdMap += (controlPlanId -> versionScala)
            IndicatorByAll.put(key, controlPlanIdMap)
          }
        } else {
          //没有一样的key
          val driftXAndIndicator = concurrent.TrieMap[String, IndicatorConfig](driftX -> indicatorConfig)
          val versionScala = concurrent.TrieMap[String, concurrent.TrieMap[String, IndicatorConfig]](version -> driftXAndIndicator)
          val controlPlanIdScala = concurrent.TrieMap[String, concurrent.TrieMap[String,
            concurrent.TrieMap[String, IndicatorConfig]]](controlPlanId -> versionScala)
          IndicatorByAll.put(key, controlPlanIdScala)
        }
      }

    }catch {
      case ex: Exception=> logger.warn(s"addIndicatorConfigToDriftIndicatorByAll_error: $ex" )
    }
  }

  /**
   * 缓存历史数据，用于计算
   */
  def CacheHistoryData(w2wType: String, indicatorResult: IndicatorResult, indicatorKey: String, indicatorValueDouble: Double, X: Int, cacheHistoryValue: mutable.HashMap[String, mutable.HashMap[String, List[Double]]]):  Unit = {
    var toolMap = new mutable.HashMap[String, List[Double]]()

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
        toolMap += Key -> List(indicatorValueDouble)
        cacheHistoryValue += indicatorKey -> toolMap
      }else{
        toolMap = cacheHistoryValue(indicatorKey)

        // 判断toolName, chamberName, recipe, product, stage之前是否存在
        if (!toolMap.contains(Key)) {
          toolMap += Key -> List(indicatorValueDouble)
          cacheHistoryValue += indicatorKey -> toolMap
        }else{
          val KeyList: List[Double] = toolMap(Key)
          val newKeyList = KeyList :+ indicatorValueDouble
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
   * 初始化 指定 高阶indicator的状态
   * @param calcType
   * @param cacheHistoryValue
   */
  def initAdvancedIndicator(calcType:String,
                            cacheHistoryValue: mutable.HashMap[String, mutable.HashMap[String, List[Double]]]) = {
    try {

      val advancedIndicatorCacheDataList: ListBuffer[RedisCache[AdvancedIndicatorCacheData]] =  calcType match {
        case "drift" =>
          getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_drift|")
        case "driftPercent" =>
          getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_driftPercent|")
        case "movAvg" =>
          getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_movAvg|")
        case "avg" =>
          getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_avg|")
        case _ => null

      }

      if(null != advancedIndicatorCacheDataList){
        advancedIndicatorCacheDataList.foreach(redisCache => {
          val advancedIndicatorCacheData  = redisCache.datas
          val baseIndicatorId = advancedIndicatorCacheData.baseIndicatorId
          val w2wKey = advancedIndicatorCacheData.w2wKey
          val indicatorValueList = advancedIndicatorCacheData.indicatorValueList

          val w2wMap = if (cacheHistoryValue.contains(baseIndicatorId)) {
            cacheHistoryValue(baseIndicatorId)
          } else {
            mutable.HashMap[String, List[Double]]()
          }

          w2wMap += w2wKey -> indicatorValueList
          cacheHistoryValue += baseIndicatorId  -> w2wMap
        })
        logger.warn(s"initAdvancedIndicator ${calcType} Finish: size = ${advancedIndicatorCacheDataList.size}")
      }else{
        logger.warn(s"advancedIndicatorCacheDataList is null")
      }

    }catch {
      case exception: Exception => logger.warn(ErrorCode("002001d001C", System.currentTimeMillis(),
        Map("function" -> "initAdvancedIndicator","msg" -> "redis读取失败!!"), exception.toString).toString)
    }
  }


  /**
   * 初始化 指定 高阶indicator的状态
   * @param calcType
   * @param cacheHistoryValue
   */
  def initLinearFitAdvancedIndicator(calcType:String,
                                     cacheHistoryValue: TrieMap[String, TrieMap[String, List[DataPoint]]]) = {
    try {

      val advancedLinearFitIndicatorCacheDataList =  calcType match {
        case "linearFit" =>
          getStringDataByPrefixKey[AdvancedLinearFitIndicatorCacheData]("advancedIndicator_linearFit|")
        case _ => null

      }

      if(null != advancedLinearFitIndicatorCacheDataList){
        advancedLinearFitIndicatorCacheDataList.foreach(redisCache => {
          val advancedIndicatorCacheData  = redisCache.datas
          val baseIndicatorId = advancedIndicatorCacheData.baseIndicatorId
          val w2wKey = advancedIndicatorCacheData.w2wKey
          val dataPointList = advancedIndicatorCacheData.dataPointList

          val w2wMap = if (cacheHistoryValue.contains(baseIndicatorId)) {
            cacheHistoryValue(baseIndicatorId)
          } else {
            TrieMap[String, List[DataPoint]]()
          }

          w2wMap += w2wKey -> dataPointList
          cacheHistoryValue += baseIndicatorId  -> w2wMap
        })
        logger.warn(s"initLinearFitAdvancedIndicator ${calcType} Finish: size = ${advancedLinearFitIndicatorCacheDataList.size}")
      }else{
        logger.warn(s"linearFitAdvancedIndicatorCacheDataList is null")
      }

    }catch {
      case exception: Exception => logger.warn(ErrorCode("002001d001C", System.currentTimeMillis(),
        Map("function" -> "initLinearFitAdvancedIndicator","msg" -> "redis读取失败!!"), exception.toString).toString)
    }
  }

  /**
   * 缓存历史数据，用于计算
   */
  def cacheHistoryDataAndRedis(w2wType: String,
                       indicatorResult: IndicatorResult,
                       indicatorKey: String,
                       indicatorValueDouble: Double,
                       X: Int,
                       cacheHistoryValue: mutable.HashMap[String, mutable.HashMap[String, List[Double]]],
                       calcType:String,
                       ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#ReadOnlyContext):  Unit = {
    var toolMap = new mutable.HashMap[String, List[Double]]()

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
      val indicatorValueList = ListBuffer[Double]()
      if (!cacheHistoryValue.contains(indicatorKey)) {
        indicatorValueList.append(indicatorValueDouble)
        toolMap += w2wKey -> indicatorValueList.toList
        cacheHistoryValue += indicatorKey -> toolMap
      }else{
        toolMap = cacheHistoryValue(indicatorKey)

        // 判断toolName, chamberName, recipe, product, stage之前是否存在
        if (!toolMap.contains(w2wKey)) {
          indicatorValueList.append(indicatorValueDouble)
          toolMap += w2wKey -> indicatorValueList.toList
          cacheHistoryValue += indicatorKey -> toolMap
        }else{
          val KeyList: List[Double] = toolMap(w2wKey)
          val newKeyList = KeyList :+ indicatorValueDouble
          val dropKeyList = if (newKeyList.size > X + 1) { newKeyList.drop(1) } else { newKeyList }
          indicatorValueList ++= dropKeyList
          toolMap += w2wKey -> dropKeyList
          cacheHistoryValue += indicatorKey -> toolMap
        }
      }

      // todo 组装Redis缓存数据 并 侧道输出
      try{
        val advancedIndicatorCacheData = AdvancedIndicatorCacheData(baseIndicatorId = indicatorKey,
          w2wKey = w2wKey,
          indicatorValueList = indicatorValueList.toList,
          dataVersion = indicatorResult.dataVersion,
          configVersion = ProjectConfig.JOB_VERSION)

        val advancedIndicatorCacheDataJsonNode: JsonNode = beanToJsonNode[RedisCache[AdvancedIndicatorCacheData]](RedisCache(dataType = "advancedIndicator_" + calcType,datas = advancedIndicatorCacheData))
        ctx.output(advancedIndicatorCacheDataOutput,advancedIndicatorCacheDataJsonNode)
      }catch {
        case e:Exception => {
          logger.error(s"output advancedIndicatorCacheDataOutput error ! \n " +
            s"calcType == ${calcType}; \n" +
            s"baseIndicatorId == ${indicatorKey} ; \n " +
            s"w2wKey == ${w2wKey}; \n" +
            s"indicatorValueList == ${indicatorValueList}; \n " +
            s"${e.printStackTrace()}")
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
  def getCacheValue(indicatorResult: IndicatorResult, config: IndicatorConfig, cacheHistoryValue: mutable.HashMap[String, mutable.HashMap[String, List[Double]]]): List[Double] = {

    val toolMap = cacheHistoryValue(indicatorResult.indicatorId.toString)

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

    val ValueList: List[Double] = toolMap(key)
    ValueList
  }


  /**
   *  写入kafka
   */
  def writeKafka(indicatorOutputStream: DataStream[(IndicatorResult, Boolean, Boolean, Boolean)]): Unit ={
    val indicatorJson: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream.map(i => {
      FdcData[IndicatorResult]("indicator", i._1)
    })

    val driftIndicatorStream: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream
      .filter(_._2 ==true)
      .map(i => {
        FdcData[IndicatorResult]("indicator", i._1)
      })

    val calculatedIndicatorStream: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream
      .filter(_._3 ==true)
      .map(i => {
        FdcData[IndicatorResult]("indicator", i._1)
      })

    val logisticIndicatorStream: DataStream[FdcData[IndicatorResult]] = indicatorOutputStream
      .filter(_._4 ==true)
      .map(i => {
        FdcData[IndicatorResult]("indicator", i._1)
      })


    indicatorJson.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC, (element: FdcData[IndicatorResult]) => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To Indicator").uid("IndicatorJob_sink")

    driftIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_DRIFT_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_DRIFT_INDICATOR_TOPIC, (element: FdcData[IndicatorResult]) => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To driftIndicatorStream").uid("IndicatorJob_drift_sink")

    calculatedIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_CALCULATED_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_CALCULATED_INDICATOR_TOPIC, (element: FdcData[IndicatorResult]) => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To calculatedIndicatorStream").uid("IndicatorJob_calculated_sink")

    logisticIndicatorStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_LOGISTIC_INDICATOR_TOPIC, (element: FdcData[IndicatorResult]) => element.datas.toolName + element.datas.indicatorId.toString)
      , ProjectConfig.getKafkaProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("Kafka Sink To  logisticIndicatorStream").uid("IndicatorJob_logistic_sink")
  }


}

