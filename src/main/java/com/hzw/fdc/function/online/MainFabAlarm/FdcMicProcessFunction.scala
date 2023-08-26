package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{MicDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

/**
 *  增加MultipeOCAP，该功能是选择多个已建立Spec的Indicator（Basic或者Advanced Indicator），
 *  并勾选对应的Limit线（USL，UBL，UCL，LCL，LBL及LCL），最后再选择OCAP，当这些Indicator同时Trigger out of limit时才触发OCAP。
 */

class FdcMicProcessFunction extends KeyedBroadcastProcessFunction[String, (AlarmRuleResult, IndicatorLimitResult), JsonNode, AlarmMicRule]{
  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcMicProcessFunction])

  // {"controlPlanId": {"version" : {"micId": "MicConfig"}}}
  var micConfigByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[Long, MicConfig]]]()

  // 用于触发mic计算的条件: {"controlPlanId": ["INDICATOR_ID1": "INDICATOR_ID2"]}
  val defIndicatorConfigByAll = new mutable.HashMap[String, scala.collection.mutable.Set[String]]()

  // 缓存RUN_ID { "controlPlanId": {"RUN_ID": {"INDICATOR_ID": CacheMicDataScala} }}
  var micDataByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, CacheMicDataScala]]]()

  // 用于保存哪些micId完成了计算  {"RUN_ID": ["micId"]}
  val finishIndicatorAll = new concurrent.TrieMap[String, scala.collection.mutable.Set[Long]]()



  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    //val initConfig:ListBuffer[ConfigData[MicConfig]] = InitFlinkFullConfigHbase.MicConfigList
    val initConfig:ListBuffer[ConfigData[MicConfig]] = readHbaseAllConfig[MicConfig](ProjectConfig.HBASE_SYNC_MIC_ALARM_TABLE, MicDataType)

    initConfig.foreach(k=> {
      k.`dataType` match {
        case "micalarmconfig" => addMicConfigToTCSP(k)
        case _ => logger.warn(s"FdcMicProcessFunction open no mach type: " + k.`dataType`)
      }
    })
    logger.warn(s"open micConfigByAll: " + micConfigByAll)
    logger.warn(s"open defIndicatorConfigByAll: " + defIndicatorConfigByAll)
  }

  override def processElement(value: (AlarmRuleResult, IndicatorLimitResult), ctx: KeyedBroadcastProcessFunction[String, (AlarmRuleResult, IndicatorLimitResult), JsonNode, AlarmMicRule]#ReadOnlyContext, out: Collector[AlarmMicRule]): Unit = {
    try {

      val alarmLevelRule = value._1
      val controlPlnId = alarmLevelRule.controlPlnId.toString
      val version = alarmLevelRule.controlPlanVersion.toString
      val triggerLevel = value._2.oocLevel

      //      logger.warn(s"FdcMicProcessFunction_step1")

      // RUN_ID: RNITool_31--YMTC_chamber--1604980730000
      val RUN_ID: String = alarmLevelRule.runId
      val INDICATOR_ID = alarmLevelRule.indicatorId.toString

      // 配置不存在
      if (!this.micConfigByAll.contains(controlPlnId)) {
        return
      }

      val versionMap = micConfigByAll(controlPlnId)
      if(!versionMap.contains(version)){
        logger.warn(s"indicator_contains version no exist: " + controlPlnId + "\t: exit version: " + versionMap.keys + "\t match version: " + version)
        return
      }

      if(versionMap(version).isEmpty){
        logger.warn(s"indicatorMap isEmpty: " + controlPlnId)
        return
      }

      // 过滤不参与计算的INDICATOR_ID
      val indicatorConfigSet = this.defIndicatorConfigByAll(controlPlnId)
      if (!indicatorConfigSet.contains(INDICATOR_ID)) {
        return
      }

      //      logger.warn(s"FdcMicProcessFunction_step3: ")

      val oocValue = CacheMicDataScala(
        alarmLevelRule.indicatorId,
        alarmLevelRule.indicatorType,
        alarmLevelRule.indicatorName,
        List(triggerLevel),
        value._2.limit,
        alarmLevelRule.indicatorValue,
        alarmLevelRule.recipeName,
        alarmLevelRule.toolGroupName,
        alarmLevelRule.toolGroupId,
        alarmLevelRule.configMissingRatio,
        alarmLevelRule.dataMissingRatio,
        alarmLevelRule.materialName
      )


      // 将INDICATOR_ID和oocLevel 缓存起来
      if(!this.micDataByAll.contains(controlPlnId)){
        val tmp = concurrent.TrieMap(RUN_ID -> concurrent.TrieMap[String, CacheMicDataScala](INDICATOR_ID -> oocValue))
        this.micDataByAll += (controlPlnId -> tmp)
//        logger.info(s"CalculatedIndicator_step2")
      }else {
        var micDataMap = this.micDataByAll(controlPlnId)

        // 添加INDICATOR_ID和oocLevel
        if (!micDataMap.contains(RUN_ID)) {
          val INDICATOR_ID_data = concurrent.TrieMap[String, CacheMicDataScala](INDICATOR_ID -> oocValue)
          micDataMap += (RUN_ID -> INDICATOR_ID_data)
          this.micDataByAll.update(controlPlnId, micDataMap)
        } else {
          var indicatorDataMap = micDataMap(RUN_ID)
          indicatorDataMap += (INDICATOR_ID -> oocValue)
          micDataMap.update(RUN_ID, indicatorDataMap)
          this.micDataByAll.update(controlPlnId, micDataMap)
        }
        //        logger.warn(s"FdcMicProcessFunction_step4: " + RUN_ID + "\t" + INDICATOR_ID + "\t" + alarmLevelRule.indicatorName)

        // 开始计算
        val indicatorDataMap = micDataMap(RUN_ID)
        val indicatorConfigMap = micConfigByAll(controlPlnId)
        val versionMap = indicatorConfigMap(version)

        for (micConfig: MicConfig <- versionMap.values) {

          if(!(finishIndicatorAll.contains(RUN_ID) && finishIndicatorAll(RUN_ID).contains(micConfig.micId))) {
            // 判断是否开始计算
            if (isStartMath(micConfig, indicatorDataMap.keys.toSet)) {
//              logger.warn(s"FdcMicProcessFunction_step5:\t" + micConfig + "\toperator\t: " + indicatorDataMap)

              // 满足所有的indicator同时Trigger out of limit, 触发OCAP
              if (baseMath(micConfig, indicatorDataMap)) {
//                logger.warn(s"FdcMicProcessFunction_step6")
                val res = resAlarmMicRule(micConfig, indicatorDataMap, alarmLevelRule)
                try {
                  out.collect(res)
                }catch {
                  case ex: Exception => logger.warn(s"resAlarmMicRule error: $ex \t $res")
                }
              }else{
//                logger.warn(s"FdcMicProcessFunction_step7")
                val res = getEmptyAlarmMicRule(micConfig, indicatorDataMap, alarmLevelRule)
                out.collect(res)
              }

              // 判断RUN_ID的所有mic是否都计算完成
              if (isRunIDFinishMic(versionMap, RUN_ID, micConfig)) {
//                logger.warn(s"FdcMicProcessFunction_step7: " + RUN_ID)
                micDataMap.remove(RUN_ID)
                this.micDataByAll.update(controlPlnId, micDataMap)
              }
            }
          }
        }

        // 超700个开始清理过期的run_id
        if(micDataMap.size > 700) {
//          logger.warn(s"FdcMicProcessFunction_step8: ")
          micDataMap = filterRunIdTime(micDataMap)
          this.micDataByAll.update(controlPlnId, micDataMap)
        }
      }
    } catch {
      case ex: Exception => logger.warn(s"FdcMicProcessFunction error: $value\t$ex")
    }
  }



  override def processBroadcastElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, (AlarmRuleResult, IndicatorLimitResult), JsonNode, AlarmMicRule]#Context, out: Collector[AlarmMicRule]): Unit = {
    try {
      value.get("dataType").asText() match {
        case "micalarmconfig" =>
          val config = toBean[ConfigData[MicConfig]](value)

          addMicConfigToTCSP(config)

        //日志优化，以免由于alarmconfig打印日志
        case "alarmconfig" =>

        case _ => logger.warn(s"FdcMicProcessFunction processBroadcastElement no mach type: " + value.get("dataType").asText())
      }
    }catch {
      case ex: Exception => logger.warn(s"FdcMicProcessBroadcastFunction error: $value\t$ex")
    }
  }


  def resAlarmMicRule(micConfig: MicConfig, indicatorDataMap: concurrent.TrieMap[String, CacheMicDataScala],
                      alarmLevelRule: AlarmRuleResult): AlarmMicRule = {

    var recipeName = ""
    var toolGroupName = ""
    var toolGroupId = 0L
    var configMissingRatio = 0.00
    var dataMissingRatio = 0.00
    var materialName = ""

    val configDefMap = mutable.HashMap[Long, List[Long]]()
    micConfig.`def`.foreach(x => {
      configDefMap += (x.indicatorId -> x.oocLevel)
    })

    // 组装mic触发的indicator返回数据格式
    val  operator = new ListBuffer[operatorScala]()
    for(indicatorData <- indicatorDataMap){
      val indicatorId = indicatorData._1.toLong
      val cacheMicDataScala: CacheMicDataScala = indicatorData._2
      if(configDefMap.contains(indicatorId)){
        operator.+=(operatorScala(
          cacheMicDataScala.indicatorId,
          cacheMicDataScala.indicatorType,
          cacheMicDataScala.indicatorName,
          configDefMap(cacheMicDataScala.indicatorId),
          cacheMicDataScala.triggerLevel,
          cacheMicDataScala.limit,
          cacheMicDataScala.indicatorValue))

        recipeName = cacheMicDataScala.recipeName
        toolGroupName = cacheMicDataScala.toolGroupName
        toolGroupId = cacheMicDataScala.toolGroupId
        configMissingRatio = cacheMicDataScala.configMissingRatio
        dataMissingRatio = cacheMicDataScala.dataMissingRatio
        materialName = cacheMicDataScala.materialName
      }else{
        logger.warn(s"resAlarmMicRule  cacheMicDataScala: " + cacheMicDataScala + "\t indicatorId: " +indicatorId)
      }
    }

    val alarmMicRule = AlarmMicRule(controlPlanId = micConfig.controlPlanId,
      controlPlanName = micConfig.controlPlanName,
      controlPlanVersion = micConfig.controlPlanVersion,
      action = micConfig.actions,
      micId = micConfig.micId,
      micName = micConfig.micName,
      locationId = alarmLevelRule.locationId,
      locationName = alarmLevelRule.locationName,
      moduleId = alarmLevelRule.moduleId,
      moduleName = alarmLevelRule.moduleName,
      operator = operator.toList,
      runId = alarmLevelRule.runId,
      toolName = alarmLevelRule.toolName,
      chamberName = alarmLevelRule.chamberName,
      recipeName = recipeName,
      toolGroupName = toolGroupName,
      toolGroupId = toolGroupId,
      chamberGroupName = alarmLevelRule.chamberGroupName,
      chamberGroupId = alarmLevelRule.chamberGroupId,
      configMissingRatio = configMissingRatio,
      dataMissingRatio = dataMissingRatio,
      materialName = alarmLevelRule.materialName,
      runStartTime = alarmLevelRule.runStartTime,
      runEndTime = alarmLevelRule.runEndTime,
      windowStartTime = alarmLevelRule.windowStartTime,
      windowEndTime = alarmLevelRule.windowEndTime,
      windowDataCreateTime = alarmLevelRule.windowDataCreateTime,
      x_of_total = micConfig.xofTotal,
      alarmCreateTime = System.currentTimeMillis(),
      area = alarmLevelRule.area,
      section = alarmLevelRule.section,
      mesChamberName = alarmLevelRule.mesChamberName,
      lotMESInfo = alarmLevelRule.lotMESInfo,
      dataVersion = alarmLevelRule.dataVersion)
    alarmMicRule
  }

  /**
   *  返回空的mic结果
   */
  def getEmptyAlarmMicRule(micConfig: MicConfig, indicatorDataMap: concurrent.TrieMap[String, CacheMicDataScala],
                           alarmLevelRule: AlarmRuleResult): AlarmMicRule = {

    var recipeName = ""
    var toolGroupName = ""
    var toolGroupId = 0L
    var configMissingRatio = 0.00
    var dataMissingRatio = 0.00

    val alarmMicRule = AlarmMicRule(controlPlanId = micConfig.controlPlanId,
      controlPlanName = micConfig.controlPlanName,
      controlPlanVersion = micConfig.controlPlanVersion,
      action = List(),
      micId = micConfig.micId,
      micName = micConfig.micName,
      locationId = alarmLevelRule.locationId,
      locationName = alarmLevelRule.locationName,
      moduleId = alarmLevelRule.moduleId,
      moduleName = alarmLevelRule.moduleName,
      operator = List(),
      runId = alarmLevelRule.runId,
      toolName = alarmLevelRule.toolName,
      chamberName = alarmLevelRule.chamberName,
      recipeName = recipeName,
      toolGroupName = toolGroupName,
      toolGroupId = toolGroupId,
      chamberGroupName = alarmLevelRule.chamberGroupName,
      chamberGroupId = alarmLevelRule.chamberGroupId,
      configMissingRatio = configMissingRatio,
      dataMissingRatio = dataMissingRatio,
      materialName = alarmLevelRule.materialName,
      runStartTime = alarmLevelRule.runStartTime,
      runEndTime = alarmLevelRule.runEndTime,
      windowStartTime = alarmLevelRule.windowStartTime,
      windowEndTime = alarmLevelRule.windowEndTime,
      windowDataCreateTime = alarmLevelRule.windowDataCreateTime,
      x_of_total = micConfig.xofTotal,
      alarmCreateTime = System.currentTimeMillis(),
      area = alarmLevelRule.area,
      section = alarmLevelRule.section,
      mesChamberName = alarmLevelRule.mesChamberName,
      lotMESInfo = List(),
      dataVersion = alarmLevelRule.dataVersion)
    alarmMicRule
  }


  /**
   *   过滤过期的RUN_ID, 找到TOO_ID数量最多的一个,删除其对应的部分RUN_ID
   */
  def filterRunIdTime(micDataMap: concurrent.TrieMap[String, concurrent.TrieMap[String, CacheMicDataScala]]):
  concurrent.TrieMap[String, concurrent.TrieMap[String, CacheMicDataScala]] = {
    try {
      // {"TOOLD_ID": NUM}
      var Tmp:mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

      val RUN_ID_LIST = micDataMap.keys.toList
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
      for (elem <- RUN_ID_LIST.sorted.take(400)){
        if(elem.contains(max._1)){
          micDataMap.remove(elem)
          logger.info(s"filterRunIdTime_delete RUN_ID: " + elem)
        }
        finishIndicatorAll.remove(elem)
      }
    }catch {
      case ex: Exception => logger.error(s"ERROR filterRunIdTime:  $ex")
    }
    micDataMap
  }

  /**
   *  判断RUN_ID 的所有indicator是否都计算完成
   */
  def isRunIDFinishMic(indicatorConfigMap: concurrent.TrieMap[Long, MicConfig], RUN_ID: String,
                       micConfig: MicConfig):Boolean = {
    var Res = false
    try{
      if(finishIndicatorAll.contains(RUN_ID)){
        var indicatorDataSet: mutable.Set[Long] = finishIndicatorAll(RUN_ID)
        indicatorDataSet.add(micConfig.micId)
        finishIndicatorAll.update(RUN_ID, indicatorDataSet)
      }else{
        finishIndicatorAll += (RUN_ID -> mutable.Set(micConfig.micId))
      }

      val DataSet = finishIndicatorAll(RUN_ID)
//      logger.warn(s"isRunIDFinishMic DataSet: " + DataSet + "\t indicatorConfigMap.keySet: " + indicatorConfigMap.keySet)
      // 判断是否全部计算完
      if(DataSet == indicatorConfigMap.keySet) {
        Res = true
        finishIndicatorAll.remove(RUN_ID)
      }

    }catch {
      case ex: Exception => logger.warn(s"CalculatedIndicator_hasCalculated_ERROR: $ex")
    }
    Res
  }

  /**
   *  判断所有的indicator 是否同时Trigger out of limit
   */
  def baseMath(micConfig: MicConfig, indicatorDataMap: concurrent.TrieMap[String, CacheMicDataScala]): Boolean = {
    var Res = false
    try {
      Res = true
      val x_of_total = micConfig.xofTotal
      var count = 0
      for (p <- micConfig.`def`) {
        val defIndicatorId = p.indicatorId.toString
        val defOocLevelList = p.oocLevel
        if (indicatorDataMap.contains(defIndicatorId)) {

          val dataOocLevel = indicatorDataMap(defIndicatorId).triggerLevel.head
          // 考虑dataOocLevel 会有-1，-2，-3的情况
          if(dataOocLevel < 0){
            val ooc = defOocLevelList.filter(x => {x < 0}).filter(x => {dataOocLevel <= x})
            if(ooc.isEmpty){
              Res = false
            }else{
              count += 1
            }
          }else{
            val ooc = defOocLevelList.filter(x => {x> 0}).filter(x => {dataOocLevel >= x})
            if(ooc.isEmpty){
              Res = false
            }else{
              count += 1
            }
          }
        }else{
          Res = false
        }
      }

      //MIC增加total X触发alarm的配置
      if(x_of_total > 0){
        if(count >= x_of_total){
          Res = true
        }
      }
    }catch {
      case ex: Exception => logger.warn(s"baseMath_ERROR: $ex")
        Res = false
    }
    Res
  }

  /**
   *  判断是否满足计算的条件
   */
  def isStartMath(micConfig: MicConfig, indicatorDataSet: Set[String]):Boolean = {
    var status = false
    try{
      val indicatorConfigSet = micConfig.`def`.map(_.indicatorId.toString).toSet

      if(indicatorConfigSet.subsetOf(indicatorDataSet)){
        status = true
      }
    }catch {
      case ex: Exception => logger.warn(s"isStartMath_ERROR: $ex")
    }
    status
  }


  /**
   *  增加mic配置
   */
  def addMicConfigToTCSP(config: ConfigData[MicConfig]): Unit = {
    try {

      val micConfig = config.datas

      val key = micConfig.controlPlanId.toString

      val version = micConfig.controlPlanVersion.toString


      val micId = micConfig.micId

      if (!config.status) {

        // 删除micConfig逻辑
        if (this.micConfigByAll.contains(key)) {

          //有一样的key
          val versionAndIndicator = this.micConfigByAll(key)

          if (versionAndIndicator.contains(version)) {
            val indicatorKey = versionAndIndicator(version)

            if (indicatorKey.contains(micId)) {

              indicatorKey.remove(micId)

              versionAndIndicator.put(version, indicatorKey)
              this.micConfigByAll += (key -> versionAndIndicator)

              val versionMap = this.micConfigByAll(key)
              val indicatorMap = versionMap(version)
              this.defIndicatorConfigByAll += (key -> IndicatorListByControlPlan(indicatorMap))
            }

            if (indicatorKey.isEmpty) {
              this.micConfigByAll.remove(key)
            }
            //logger.info(s"CalculatedIndicatorTOTCSP_step2" + indicatorKey)
          } else {
            logger.warn(s"CalculatedIndicator Config version no exist: " + micConfig)
          }
        }



      } else {
        logger.warn(s"addMicConfigToTCSP_step2: " + config)
        // 新增逻辑
        if (this.micConfigByAll.contains(key)) {
          val versionAndIndicatorMap = this.micConfigByAll(key)

          if (versionAndIndicatorMap.contains(version)) {
            val algoMap = versionAndIndicatorMap(version)
            algoMap += (micId -> micConfig)
            versionAndIndicatorMap += (version -> algoMap)
          } else {
            val algoToScala = concurrent.TrieMap[Long, MicConfig](micId -> micConfig)
            versionAndIndicatorMap += (version -> algoToScala)
          }

          // 更新版本
          val k = versionAndIndicatorMap.keys.map(_.toLong)
          if (k.size > 2) {
            val minVersion = k.toList.min
            versionAndIndicatorMap.remove(minVersion.toString)
            logger.warn(s"addMicConfigToTCSP: " + minVersion + "\tmicConfig: " + micConfig)
          }
          this.micConfigByAll += (key -> versionAndIndicatorMap)

        } else {
          // 没有一样的key
          val indicatorToScala = concurrent.TrieMap[Long, MicConfig](micId -> micConfig)
          val versionScala = concurrent.TrieMap[String, concurrent.TrieMap[Long, MicConfig]](version -> indicatorToScala)
          this.micConfigByAll += (key -> versionScala)
        }

        val versionMap = this.micConfigByAll(key)
        val indicatorMap = versionMap(version)
        this.defIndicatorConfigByAll += (key -> IndicatorListByControlPlan(indicatorMap))
      }
    }catch {
      case ex: Exception => logger.error(s"addMicConfigToTCSP ERROR:  $ex")
    }
  }

  /**
   *  参与计算逻辑的 Indicator
   */
  def IndicatorListByControlPlan(micMap: concurrent.TrieMap[Long, MicConfig]): scala.collection.mutable.Set[String] ={
    val indicatorSet = scala.collection.mutable.Set[String]()
    for(micConfig <- micMap.values){
      micConfig.`def`.map(x => {
        indicatorSet.add(x.indicatorId.toString)
      })
    }
    indicatorSet
  }


}
