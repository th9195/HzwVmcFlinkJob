//package com.hzw.fdc.function.online.MainFabAlarm
//
//import com.fasterxml.jackson.databind.JsonNode
//import com.hzw.fdc.function.PublicFunction.limitCondition.MatchingLimitConditionFunctions.{produce, satisfied}
//import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
//import com.hzw.fdc.scalabean._
//import com.hzw.fdc.json.MarshallableImplicits._
//import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
//import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
//import org.apache.flink.util.Collector
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.concurrent
//import scala.collection.mutable.ListBuffer
//
//class EWMAConfigProcessFunction extends KeyedBroadcastProcessFunction[String,JsonNode,JsonNode,(JsonNode,Option[AlarmRuleLimit])]{
//
//  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
//
//  private lazy val outputTag = new OutputTag[JsonNode]("alarm_config_output") {}
//
//  //  private var map: concurrent.TrieMap[String,(AlarmRuleConfig,Option[Double])] = _
//
//  //业务逻辑从上到下逐级匹配
//  // key为indicatorid|Type --> tool|chamber|recipe|product|stage --> controlPlanVersion --> config配置
//  // Type 包括 tool， chamber，recipe，product，stage
//  var ewmaRuleConfigMap = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]]
//
//  // ewma 缓存数据 key 为 (toolid + chamberid + indicatorid + specDefId) or (toolid + chamberid + recipe + indicatorid + specDefId)
//  private var ewmaCacheData = new concurrent.TrieMap[String, Option[Double]]
//
//  /**
//   *  初始化
//   */
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//
//    // 获取全局变量
//    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
//    ProjectConfig.getConfig(p)
//
////    map=concurrent.TrieMap()
////    InitFlinkFullConfigHbase.AlarmConfigList.foreach(addEWMAConfig(_))
//
//    // 初始化加载 alarm 配置信息
//    InitFlinkFullConfigHbase.AlarmConfigList.foreach(updateRuleConfig)
//  }
//
//  /**
//   *
//   *  数据流
//   */
//  override def processElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
//    (JsonNode, Option[AlarmRuleLimit])]#ReadOnlyContext,
//                              out: Collector[(JsonNode, Option[AlarmRuleLimit])]): Unit = {
//    try {
//      val fdcData = toBean[FdcData[IndicatorResult]](value)
//
//      val indicatorResult = fdcData.datas
//      val indicatorId = indicatorResult.indicatorId.toString
//      val version = indicatorResult.controlPlanVersion.toString
//
//      var KeyList = indicatorResult.stage.map(x => {
//        s"${indicatorId}|${x}"
//      })
//      val productList = indicatorResult.product.map(x => {s"${indicatorId}|${x}"})
//      val indicatorList = List(
//        s"${indicatorId}|${indicatorResult.recipeName}",
//        s"${indicatorId}|${indicatorResult.chamberName}",
//        s"${indicatorId}|${indicatorResult.toolName}",
//        s"${indicatorId}|"
//      )
//
//      KeyList ++= productList
//      KeyList ++= indicatorList
//
//      KeyList.foreach(indicatorElem => {
//
//        if(ewmaRuleConfigMap.contains(indicatorElem)){
//          val ewmaToolMap = ewmaRuleConfigMap(indicatorElem)
//
//          for(ewmaRuleVersion <- ewmaToolMap.values) {
//            //匹配版本
//            if (ewmaRuleVersion.contains(version)) {
//              val indicatorRule = ewmaRuleVersion(version)
//              // 匹配 是否和limit condition中的 tool chamber recipe product stage 一样
//              if (isMatch(indicatorRule, indicatorResult)) {
//                // ewma主要处理逻辑
//                if(indicatorRule.EwmaArgs != null){
//                  logger.warn(s"ewma触发：$indicatorId")
//                  val indicatorValue = indicatorResult.indicatorValue.toDouble
//
//                  // 缓存的key值
//                  val ruleKeyTuple = getRuleKey(indicatorRule.w2wType, indicatorResult)
//                  val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" + indicatorRule.specDefId
//
//                  val mapTarget = if(ewmaCacheData.contains(ruleKey)) ewmaCacheData(ruleKey).get else indicatorValue
//
//                  val args = indicatorRule.EwmaArgs
//                  val limit = indicatorRule.limit
//                  val λ = args.lambdaValue
//                  val lastTarget = if (args.pmRetargetTrigger && indicatorResult.pmStatus == MainFabConstants.pmEnd) {
//                    Double.NaN
//                  } else {
//                    mapTarget
//                  }
//
//                  val nowTarget = if (lastTarget.equals(Double.NaN)) indicatorValue else λ * lastTarget + (1 - λ) * indicatorValue
//
//                  val newLimit = getAlarmRuleLimit(args, limit, nowTarget)
//
//                  logger.warn(s"ewma计算结果，indicator=$indicatorId,limit=${newLimit},target=$nowTarget")
//
//                  if (newLimit != null) {
//                    indicatorRule.limit = newLimit
//                  }
//
//                  // 更新ewma 计算的值
//                  ewmaCacheData.put(ruleKey, Option.apply(nowTarget))
//                  out.collect((value, Option.apply(indicatorRule.limit)))
//                  return
//                }
//              }
//            }
//          }
//        }
//
//      })
//    } catch {
//      case e:Exception =>logger.error(s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
//    }
//    out.collect((value, None))
//  }
//
//  /**
//   * 匹配indicator 和alarm 配置中的 tool chamber recipe product stage是否一样
//   */
//  def isMatch(indicatorRule: AlarmRuleConfig, indicatorResult: IndicatorResult): Boolean = {
//    var status = true
//    try{
//
//      if(indicatorRule.recipeName.nonEmpty){
//        if(indicatorRule.recipeName.get != indicatorResult.recipeName){
//          status = false
//        }
//      }
//
//      if(indicatorRule.chamberName.nonEmpty){
//        if(indicatorRule.chamberName.get != indicatorResult.chamberName){
//          status = false
//        }
//      }
//
//      if(indicatorRule.toolName.nonEmpty){
//        if(indicatorRule.toolName.get != indicatorResult.toolName){
//          status = false
//        }
//      }
//
//      if(indicatorRule.productName.nonEmpty){
//        val productName = indicatorRule.productName.get
//        if(!indicatorResult.product.contains(productName)){
//          status = false
//        }
//      }
//
//      if(indicatorRule.stage.nonEmpty){
//        val stage = indicatorRule.stage.get
//        if(!indicatorResult.stage.contains(stage)){
//          status = false
//        }
//      }
//
//    }catch {
//      case ex: Exception => logger.warn(s"alarm job isMatch Exception${ExceptionInfo.getExceptionInfo(ex)}")
//    }
//
//    status
//  }
//
////  /**
////   *
////   * @param value
////   * @param ctx
////   * @param out
////   */
////  override def processElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
////    (FdcData[IndicatorResult], Option[AlarmRuleLimit])]#ReadOnlyContext, out: Collector[(FdcData[IndicatorResult], Option[AlarmRuleLimit])]): Unit = {
////    try {
////      val fdcData = toBean[FdcData[IndicatorResult]](value)
////      val ir=fdcData.datas
////      val indicatorId = ir.indicatorId.toString
////      val allKeys = map.keySet.filter(_.split(",").head==indicatorId)
////      if(allKeys.size>0){
////        val entities = produce(ir)
////
////        val (alarmConfig,mapTarget) = if(allKeys.size==1) {
////            map.get(allKeys.head).get
////          }else if (entities.size==1) {
////            val condition = entities(0)
////            val suitedTuples = allKeys.map(map.get(_).get)
////              .map(t2 => (t2, satisfied(produce(t2._1), condition)))
////              .filter(_._2 >= 0)
////            if (suitedTuples.isEmpty) (null,None) else suitedTuples.maxBy(_._2)._1
////          }else{
////            logger.error(s"indicator:${ir.indicatorId}的product或stage不唯一${ir.toString}")
////            val suitedTuples = allKeys.map(map.get(_).get)
////              .flatMap(t2=>
////                entities.map(entity=>
////                  (t2,satisfied(produce(t2._1) ,entity))
////                )
////              )
////              .filter(_._2>=0)
////            if (suitedTuples.isEmpty) (null,None) else suitedTuples.maxBy(_._2)._1
////          }
////
////        if (alarmConfig==null || alarmConfig.EwmaArgs==null){
////          out.collect((fdcData, None))
////        }else {
////          logger.warn(s"ewma触发：$indicatorId")
////          val indicatorValue = ir.indicatorValue.toDouble
////
////          val args = alarmConfig.EwmaArgs
////          val limit = alarmConfig.limit
////          val λ = args.lambdaValue
////          val lastTarget = if (args.pmRetargetTrigger && ir.pmStatus == MainFabConstants.pmEnd) {
////            None
////          } else {
////            mapTarget
////          }
////
////          val nowTarget = if (lastTarget == None) indicatorValue else λ * lastTarget.get + (1 - λ) * indicatorValue
////
////          val newLimit = getAlarmRuleLimit(args, limit, nowTarget)
////
////          logger.warn(s"ewma计算结果，indicator=$indicatorId,limit=${newLimit},target=$nowTarget")
////
////          if (newLimit != null) {
////            alarmConfig.limit = newLimit
////          }
////
////          val key = alarmConfig.indicatorId + "," + alarmConfig.specDefId
////          map.put(key, (alarmConfig, Option.apply(nowTarget)))
////          out.collect((fdcData, Option.apply(newLimit)))
////        }
////      }else{
////        out.collect((fdcData, None))
////      }
////    } catch {
////      case e:Exception =>logger.error(s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
////    }
////  }
//
//  override def processBroadcastElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
//    (JsonNode,Option[AlarmRuleLimit])]#Context, out: Collector[(JsonNode, Option[AlarmRuleLimit])]): Unit = {
//    try {
//      value.get("dataType").asText()  match {
//        case "alarmconfig" =>
//          val config = toBean[ConfigData[AlarmRuleConfig]](value)
//          updateRuleConfig(config)
//        case _ =>
//      }
//      ctx.output(outputTag,value)
//    } catch {
//      case e:Exception =>logger.error(s"EWMA function add config error: ${value.toString} Exception: ${e.toString}")
//    }
//  }
//
//
////  /**
////   * EWMA 配置初始化
////   * @param config
////   */
////  def addEWMAConfig(config:ConfigData[AlarmRuleConfig]): Unit ={
////    try {
////      val alarmRuleConfig = config.datas
////      val key = alarmRuleConfig.indicatorId + "," + alarmRuleConfig.specDefId
////      if (alarmRuleConfig.EwmaArgs == null) {
////        map.put(key, (alarmRuleConfig, None))
////      } else if (config.status) {
////        if (map.contains(key)) {
////          val (oldConfig, target) = map.get(key).get
////          if (alarmRuleConfig.EwmaArgs.manualRetargetOnceTrigger && alarmRuleConfig.controlPlanVersion > oldConfig.controlPlanVersion) {
////            map.put(key, (alarmRuleConfig, None))
////          } else {
////            map.put(key, (alarmRuleConfig, target))
////          }
////        } else {
////          map.put(key, (alarmRuleConfig, None))
////        }
////      } else {
////        map.remove(key)
////      }
////    }catch {
////      case ex: Exception => logger.warn(s"EWMA配置初始化异常:${ExceptionInfo.getExceptionInfo(ex)}")
////    }
////  }
//
//
//  /**
//   * 处理从kafka topic新增的Rule配置数据
//   */
//  def updateRuleConfig(record: ConfigData[AlarmRuleConfig]): Unit = {
//    try {
//      //RULE配置
//      val alarmRuleConfig = record.datas
//      //只有tool、chamber、recipe、product、indicatorId能决定全局唯一配置
//      if (record.status) {
//        addAlarmRuleConfig(alarmRuleConfig)
//      } else if (!record.status) {
//        // 获取最小粒度
//        val Type = getTypeFunction(alarmRuleConfig)
//        val key = s"${alarmRuleConfig.indicatorId}|$Type"
//        ewmaRuleConfigMap.remove(key)
//      } else {
//        logger.warn(s"ewma alarm job indicatorRule match fail data: $record ")
//      }
//    } catch {
//      case ex: Exception => logger.warn(s"ewma alarm job indicatorRule Exception${ExceptionInfo.getExceptionInfo(ex)} data: $record ")
//    }
//  }
//
//  /**
//   * 新增rule配置
//   * @param alarmRuleConfig
//   */
//  def addAlarmRuleConfig(alarmRuleConfig: AlarmRuleConfig): Unit = {
//    val version = alarmRuleConfig.controlPlanVersion.toString
//
//    val recodeKey = s"${alarmRuleConfig.toolName}|${alarmRuleConfig.chamberName}|${alarmRuleConfig.recipeName}|" +
//      s"${alarmRuleConfig.productName}|${alarmRuleConfig.stage}"
//
//      // 获取最小粒度
//      val Type = getTypeFunction(alarmRuleConfig)
//      val key = s"${alarmRuleConfig.indicatorId}|$Type"
//
//      val ewmaRuleByIndicatorIdTmp = if(this.ewmaRuleConfigMap.contains(key))  this.ewmaRuleConfigMap(key)
//      else new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]()
//
//      val indicatorRuleByStageTmpRes = updateConfigData(alarmRuleConfig, version, recodeKey, ewmaRuleByIndicatorIdTmp)
//      ewmaRuleConfigMap.put(key, indicatorRuleByStageTmpRes)
//
//  }
//
//  /**
//   * 升级配置版本
//   * @param alarmRuleConfig
//   * @param version
//   * @param key
//   * @param indicatorRule
//   * @return
//   */
//  def updateConfigData(alarmRuleConfig: AlarmRuleConfig, version: String, key: String, indicatorRule: concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]):
//  concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]] = {
//    try {
//      // 新增数据
//      if (indicatorRule.contains(key)) {
//        val alarmRuleMap = indicatorRule(key)
//        alarmRuleMap += (version -> alarmRuleConfig)
//        indicatorRule.put(key, alarmRuleMap)
//        logger.warn(s"add upData ewma alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//      } else {
//        val versionMap = concurrent.TrieMap[String, AlarmRuleConfig](version -> alarmRuleConfig)
//
//        indicatorRule.put(key, versionMap)
//        logger.warn(s"add new ewma alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//      }
//
//      // 更新版本, 同一个indicator只保留两个版本
//      val versionAndAlarmRuleConfig = indicatorRule(key)
//      val keys = versionAndAlarmRuleConfig.keys.map(_.toLong)
//      if (keys.size > 2) {
//        val s = keys.min
//        versionAndAlarmRuleConfig.remove(s.toString)
//        indicatorRule.put(key, versionAndAlarmRuleConfig)
//        logger.warn(s"add upData version alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//
//
//        val newKeys=versionAndAlarmRuleConfig.keys.map(_.toLong)
//        val oldKey= newKeys.min.toString
//        val newKey= newKeys.max.toString
//
//        val oldConfig = versionAndAlarmRuleConfig(oldKey)
//        val newConfig = versionAndAlarmRuleConfig(newKey)
//
//        if(alarmRuleConfig.EwmaArgs != null) {
//          /**
//           * 增加Limit Reset Manual Trigger Once，如果选择On，那么手动触发reset一次，如果是off不触发
//           */
//          if (alarmRuleConfig.EwmaArgs.manualRetargetOnceTrigger && newConfig.controlPlanVersion > oldConfig.controlPlanVersion) {
//            //如果配置变了，要更新计数状态
//            val Type = getTypeFunction(alarmRuleConfig)
//            val key = s"${alarmRuleConfig.indicatorId}|${Type}"
//            if(ewmaCacheData.contains(key)) ewmaCacheData.remove(key)
//            logger.warn(s"delete ewmaCacheData key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//          }
//        }
//      }
//    } catch {
//      case ex: Exception => logger.warn(s"alarm job updateData Exception: ${ExceptionInfo.getExceptionInfo(ex)}")
//    }
//    indicatorRule
//  }
//
//
//
//  /**
//   * 获取key
//   *
//   * @return
//   */
//  def geneAlarmKey(ags: String*): String = {
//    ags.reduceLeft((a, b) => s"$a|$b")
//  }
//
//  /**
//   *  获取最小粒度
//   */
//  def getTypeFunction(alarmRuleConfig: AlarmRuleConfig): String ={
//    val Type = if(alarmRuleConfig.stage.nonEmpty){
//      alarmRuleConfig.stage.get
//    }else if(alarmRuleConfig.productName.nonEmpty){
//      alarmRuleConfig.productName.get
//    }else if(alarmRuleConfig.recipeName.nonEmpty){
//      alarmRuleConfig.recipeName.get
//    }else if(alarmRuleConfig.chamberName.nonEmpty){
//      alarmRuleConfig.chamberName.get
//    }else if(alarmRuleConfig.toolName.nonEmpty){
//      alarmRuleConfig.toolName.get
//    }else{
//      ""
//    }
//    Type
//  }
//
//
//  /**
//   * 判断计算出的cl有没有超限，超限则丢弃
//   * @param ucl
//   * @param lcl
//   * @param upLimit
//   * @param lowLimit
//   * @return
//   */
//  def judgeValue(ucl:Double, lcl:Double, upLimit:Double, lowLimit:Double):(Option[Double],Option[Double])={
//    if(upLimit>ucl && ucl>lowLimit && upLimit>lcl && lcl>lowLimit ){
//      (Option.apply(ucl),Option.apply(lcl))
//    }else{
//      null
//    }
//  }
//
//  def getAlarmRuleLimit(args:EwmaArgs, limit: AlarmRuleLimit, target:Double):AlarmRuleLimit={
//    try{
//      val usl=BigDecimal(limit.USL.get)
//      val lsl=BigDecimal(limit.LSL.get)
//      val bdTarget=BigDecimal(target)
//      val bdDeltaUCL=BigDecimal(args.deltaUCL)
//      val bdDeltaLCL=BigDecimal(args.deltaLCL)
//
//      val cls = args.deltaType match {
//        case "Num" =>
//          val ucl = (bdTarget + bdDeltaUCL).toDouble
//          val uclLimit = (usl - bdDeltaUCL).toDouble
//          val lcl = (bdTarget + bdDeltaLCL).toDouble
//          val lclLimit = (lsl - bdDeltaLCL).toDouble
//          judgeValue(ucl,lcl,uclLimit,lclLimit)
//        case "Percent" =>
//          val ucl = (bdTarget * (1+bdDeltaUCL)).toDouble
//          val uclLimit = (usl - target * bdDeltaUCL).toDouble
//          val lcl = (bdTarget * (1+bdDeltaLCL)).toDouble
//          val lclLimit = (lsl - target * bdDeltaLCL).toDouble
//          judgeValue(ucl,lcl,uclLimit,lclLimit)
//        case _ =>
//          logger.warn(s"${this.getClass.getSimpleName}#getAlarmRuleLimit deltaType not match: ${args.deltaType}")
//          null
//      }
//      if (cls!=null){
//        return AlarmRuleLimit(
//          USL=limit.USL,
//          UBL=None,
//          UCL=cls._1,
//          LCL=cls._2,
//          LBL=None,
//          LSL=limit.LSL
//        )
//      }
//    }catch {
//      case e:Exception=>
//        logger.error(s"${ExceptionInfo.getExceptionInfo(e)}")
//    }
//    null
//  }
//
//  /**
//   *   alarm job 计算W2W时增加recipe、product、stage配置
//   */
//  def getRuleKey(w2wType: String, indicatorResult: IndicatorResult): (String, ListBuffer[String]) = {
//
//    val w2wKeyList: ListBuffer[String] = ListBuffer()
//
//    val toolName = indicatorResult.toolName
//    val chamberName = indicatorResult.chamberName
//    val recipeName = indicatorResult.recipeName
//
//
//    // 解析 By Tool-Chamber-Recipe-Product-Stage
//    var key = toolName + "|" + chamberName
//    val productKeyList: ListBuffer[String] = ListBuffer()
//    val stageKeyList: ListBuffer[String] = ListBuffer()
//
//
//    if(w2wType.contains("Recipe")){
//      key = key + "|" + recipeName
//    }
//
//    if(w2wType.contains("Product")){
//      for(product <- indicatorResult.product){
//        productKeyList.append(key + "|" + product)
//      }
//    }
//
//    if(w2wType.contains("Stage")){
//      // 如果包含了Product
//      if(w2wType.contains("Product")) {
//        for(stage <- indicatorResult.stage) {
//          for (productKey <- productKeyList) {
//            stageKeyList.append(productKey + "|" + stage)
//          }
//        }
//      }else{
//        for(stage <- indicatorResult.stage) {
//          stageKeyList.append(key + "|" + stage)
//        }
//      }
//    }
//
//    if(w2wType.contains("Stage")){
//      for(stageKey <- stageKeyList) {
//        w2wKeyList.append(stageKey)
//      }
//    }else if(w2wType.contains("Product")){
//      for (productKey <- productKeyList) {
//        w2wKeyList.append(productKey)
//      }
//    }else{
//      w2wKeyList.append(key)
//    }
//
//    var ruleKey = indicatorResult.toolName + "|" + chamberName
//    if(w2wType.contains("Recipe")){
//      ruleKey = ruleKey + "|" + recipeName
//    }
//
//    if(w2wType.contains("Product")){
//      ruleKey = ruleKey + "|" + indicatorResult.product.head
//    }
//
//    if(w2wType.contains("Stage")){
//      ruleKey = ruleKey + "|" + indicatorResult.stage.head
//    }
//
//    (ruleKey, w2wKeyList)
//  }
//
//}
