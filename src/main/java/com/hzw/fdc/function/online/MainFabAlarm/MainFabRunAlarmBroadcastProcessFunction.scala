package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, fromMap, toBean}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{AlarmLevelRule, AlarmMicRule, AlarmRuleResult, ConfigData, ErrorCode, FdcData, IndicatorConfig, MicAlarmData, MicConfig, WindowConfigData, indicatorAlarm, micAlarm, runAlarmTimestamp}
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, MicDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

class MainFabRunAlarmBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, AlarmLevelRule] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRunAlarmBroadcastProcessFunction])

  // =============== 配置 ==========================================================================

  //micId和indicatorId配置 {"controlPlnId":  ["micId|1", "indicatorId|10"]}
  val runEndConfig = new concurrent.TrieMap[Long, mutable.Set[String]]()


  // ============== 缓存数据 ========================================================================

  //{"runId": {"controlPlnId": [AlarmRuleResult]}}
  private val indicatorCacheMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, mutable.Set[AlarmRuleResult]]]()

  // {"runId": {"controlPlnId": [AlarmMicRule]}}
  private val micCacheMap= new concurrent.TrieMap[String, concurrent.TrieMap[Long, mutable.Set[AlarmMicRule]]]()

  // 用于保存哪些micId和indicatorId完成了计算  {"runId": {"controlPlnId": ["micId|1", "indicatorId|10"]}
  private val finishIndicatorAll = new concurrent.TrieMap[String, concurrent.TrieMap[Long, mutable.Set[String]]]()


  /** The state that is maintained by this process function */
  private var state: ValueState[runAlarmTimestamp] = _


  /**
   *   初始化
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val eventStartStateDescription = new
        ValueStateDescriptor[runAlarmTimestamp]("runAlarmTimeOutValueState", TypeInformation.of(classOf[runAlarmTimestamp]))
    state = getRuntimeContext.getState(eventStartStateDescription)

    // 从hbase获取indicator配置
    //val initConfig: ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList
    val initConfig: ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)
    logger.warn("indicatorConfig SIZE: " + initConfig.size)
    initConfig.foreach(addIndicatorConfigToTCSP)

    //val initMicConfig:ListBuffer[ConfigData[MicConfig]] = InitFlinkFullConfigHbase.MicConfigList
    val initMicConfig:ListBuffer[ConfigData[MicConfig]] = readHbaseAllConfig[MicConfig](ProjectConfig.HBASE_SYNC_MIC_ALARM_TABLE, MicDataType)
    initMicConfig.foreach(addMicConfigToTCSP)

  }

  /**
   *  数据流超时
   */
  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
    AlarmLevelRule]#OnTimerContext, out: Collector[AlarmLevelRule]): Unit = {
    try {
      state.value match {
        case runAlarmTimestamp(runId, runEndTime, lastModified)
          if (System.currentTimeMillis() >= lastModified + ProjectConfig.RUN_ALARM_TIME_OUT.toLong) =>

          if(indicatorCacheMap.contains(runId)){
            val controlPlnIdSet = indicatorCacheMap(runId).keySet
            controlPlnIdSet.foreach(controlPlanId => {
              // 处理processEnd类型的indicator,聚合告警
//              logger.warn(s"onTimer_tep2: $runId \t $controlPlanId")
              val processEndRes = isProcessEndResult(runId, controlPlanId)
              if(processEndRes != null){
                out.collect(processEndRes)
              }
            })
          }

          if(runEndTime != 0L){
            finishIndicatorAll.remove(runId)
          }

          micCacheMap.remove(runId)
          indicatorCacheMap.remove(runId)
          state.clear()
          close()
        case _ =>
      }
    }catch {
      case ex: Exception => logger.error(s"onTimer error: ${ex.toString}")
    }
  }

  /**
   *  数据流
   */
  override def processElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
    AlarmLevelRule]#ReadOnlyContext, out: Collector[AlarmLevelRule]): Unit = {
    var runId = "runId"
    try {

      // logger.warn("processElement_step0: " + value)
      val option = value.path(MainFabConstants.dataType).asText()
      var controlPlanId = 0L
      var runEndTime = 0L

      // indicator处理
      if (option == "AlarmLevelRule") {

        val a = toBean[FdcData[AlarmRuleResult]](value)
        val alarmRuleResult = a.datas

        runId = alarmRuleResult.runId
        controlPlanId = alarmRuleResult.controlPlnId
        runEndTime = alarmRuleResult.runEndTime

        // 缓存有告警的indicator
        if(alarmRuleResult.RULE != Nil || alarmRuleResult.RULE.nonEmpty) {

          var flag = false
          alarmRuleResult.RULE.foreach(elem => {
            for (info <- elem.alarmInfo) {
              if(info.action.nonEmpty){
                flag = true
              }
            }
          })

          // 存在action不为空的情况, 才会聚合告警发往后台
          if(flag) {

            if (indicatorCacheMap.contains(runId)) {
              val controlPlnIdMap = indicatorCacheMap(runId)

              if (controlPlnIdMap.contains(controlPlanId)) {
                val indicatorSet = controlPlnIdMap(controlPlanId)
                indicatorSet.add(alarmRuleResult)
                controlPlnIdMap.put(controlPlanId, indicatorSet)
              } else {
                controlPlnIdMap.put(controlPlanId, mutable.Set(alarmRuleResult))
              }
              indicatorCacheMap.put(runId, controlPlnIdMap)
            } else {
              val controlPlnIdMapNew = concurrent.TrieMap[Long, mutable.Set[AlarmRuleResult]](controlPlanId ->
                mutable.Set(alarmRuleResult))
              indicatorCacheMap.put(runId, controlPlnIdMapNew)
            }
          }
        }

//        // 加入完成计算的set
        val indicator = s"indicatorId|${alarmRuleResult.indicatorId}"
        addFinishIndicatorAll(runId, controlPlanId, indicator)


      }else if(option == MainFabConstants.micAlarm){

        val mic = toBean[MicAlarmData](value)
        val micAlarmData = mic.datas
        runId = micAlarmData.runId
        controlPlanId = micAlarmData.controlPlanId
        runEndTime = micAlarmData.runEndTime

        // 缓存有告警的mic
        if(micAlarmData.action.nonEmpty) {
          if(micCacheMap.contains(runId)){
            val controlPlnIdMap = micCacheMap(runId)

            if(controlPlnIdMap.contains(controlPlanId)){
              val indicatorSet = controlPlnIdMap(controlPlanId)
              indicatorSet.add(micAlarmData)
              controlPlnIdMap.put(controlPlanId, indicatorSet)
            }else{
              controlPlnIdMap.put(controlPlanId, mutable.Set(micAlarmData))
            }
            micCacheMap.put(runId, controlPlnIdMap)
          }else{
            val controlPlnIdMapNew =  concurrent.TrieMap[Long, mutable.Set[AlarmMicRule]](controlPlanId ->
              mutable.Set(micAlarmData))
            micCacheMap.put(runId, controlPlnIdMapNew)
          }
        }

        // 加入完成计算的set
        val micIdValue = s"micId|${micAlarmData.micId}"
        addFinishIndicatorAll(runId, controlPlanId, micIdValue)
      }

      //      logger.warn(s"step1: $runId \t  $controlPlanId")


      if(finishIndicatorAll.contains(runId)) {
        val controlPlnIdMap = finishIndicatorAll(runId)
        if (controlPlnIdMap.contains(controlPlanId)) {
          val indicatorDataSet = controlPlnIdMap(controlPlanId)
          val indicatorConfigSet = runEndConfig(controlPlanId)
          if (indicatorConfigSet.nonEmpty && indicatorConfigSet.subsetOf(indicatorDataSet)) {

//            logger.warn(s"step2: $runId \t  $controlPlanId")
            // 处理processEnd类型的indicator,聚合告警
            val processEndRes = isProcessEndResult(runId, controlPlanId)
            if(processEndRes != null){
              out.collect(processEndRes)
            }

            // 删除mic记录
            if(micCacheMap.contains(runId)){
              val micControlPlnIdMap = micCacheMap(runId)
              micControlPlnIdMap.remove(controlPlanId)
              micCacheMap.put(runId, micControlPlnIdMap)
            }

            // 删除indicator记录
            if(indicatorCacheMap.contains(runId)){
              val indicatorControlPlnIdMap = indicatorCacheMap(runId)
              indicatorControlPlnIdMap.remove(controlPlanId)
              indicatorCacheMap.put(runId, indicatorControlPlnIdMap)
            }

          }
        }
      }

      // write the state back
      state.update(runAlarmTimestamp(runId, runEndTime, System.currentTimeMillis()))

      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + ProjectConfig.RUN_ALARM_TIME_OUT.toLong)

    } catch {
      case exception: Exception => logger.error(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("range" -> "processElement失败"), ExceptionInfo.getExceptionInfo(exception)).toJson)
    }

  }

  /**
   *  广播变量
   */
  override def processBroadcastElement(in2: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
    AlarmLevelRule]#Context, out: Collector[AlarmLevelRule]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()

      if (dataType == "micalarmconfig") {
        try {
          val config = toBean[ConfigData[MicConfig]](in2)
          addMicConfigToTCSP(config)
        } catch {
          case e: Exception => logger.warn(s"micalarmconfig json error data:$in2  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      } else if(dataType == "indicatorconfig"){
        val indicatorConfig = toBean[ConfigData[IndicatorConfig]](in2)
        addIndicatorConfigToTCSP(indicatorConfig)
      }
    } catch {
      case e: Exception => logger.error(s"runAlarm processBroadcastElement error data:$in2  " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }


  /**
   * 添加finishIndicatorAll
   */
  def addFinishIndicatorAll(runId: String, controlPlnId: Long, indicator: String): Unit ={
    if(finishIndicatorAll.contains(runId)){
      val controlPlnIdMap = finishIndicatorAll(runId)

      if(controlPlnIdMap.contains(controlPlnId)){
        val indicatorSet = controlPlnIdMap(controlPlnId)
        indicatorSet.add(indicator)
        controlPlnIdMap.put(controlPlnId, indicatorSet)
      }else{
        controlPlnIdMap.put(controlPlnId, mutable.Set(indicator))
      }
      finishIndicatorAll.put(runId, controlPlnIdMap)
    }else{
      val controlPlnIdMapNew =  concurrent.TrieMap[Long, mutable.Set[String]](controlPlnId -> mutable.Set(indicator))
      finishIndicatorAll.put(runId, controlPlnIdMapNew)
    }
  }

  /**
   *  判断以processEnd的indicator是否计算完
   */
  def isProcessEndResult(runId: String, controlPlnId: Long): AlarmLevelRule = {
    try{

       val indicatorAlarmList = new ListBuffer[indicatorAlarm]()
       val indicatorIdSet = mutable.Set[Long]()
       var indicatorCacheSet = mutable.Set[AlarmRuleResult]()

       if(indicatorCacheMap.contains(runId)) {
         val indicatorControlPlanMap = indicatorCacheMap(runId)
         //            logger.warn("isProcessEndResult step4 indicatorControlPlanMap: " + indicatorControlPlanMap)
         if (indicatorControlPlanMap.contains(controlPlnId)) {
           indicatorCacheSet = indicatorControlPlanMap(controlPlnId)

           indicatorCacheSet.foreach((indicator) => {
             // 增加indicator去重机制
             if(!indicatorIdSet.contains(indicator.indicatorId)) {
               indicatorAlarmList.append(indicatorAlarm(
                 indicatorId = indicator.indicatorId,
                 indicatorType = indicator.indicatorType,
                 limit = indicator.limit,
                 ruleTrigger = indicator.ruleTrigger,
                 indicatorValue = indicator.indicatorValue,
                 indicatorName = indicator.indicatorName,
                 algoClass = indicator.algoClass,
                 switchStatus = indicator.switchStatus,
                 unit = indicator.unit,
                 alarmLevel = indicator.alarmLevel,
                 oocLevel = indicator.oocLevel,
                 rule = indicator.RULE
               ))
               indicatorIdSet.add(indicator.indicatorId)
             }
           })
         }
       }

       val micAlarmList = new ListBuffer[micAlarm]()
       var micCacheSet = mutable.Set[AlarmMicRule]()

       if (micCacheMap.contains(runId)) {
         val micControlPlanMap = micCacheMap(runId)
         //            logger.warn("isProcessEndResult step5 micControlPlanMap: " + micControlPlanMap)

         if(micControlPlanMap.contains(controlPlnId)){

           micCacheSet = micControlPlanMap(controlPlnId)
           micCacheSet.foreach(mic => {
             micAlarmList.append(micAlarm(
               micId = mic.micId,
               micName = mic.micName,
               x_of_total = mic.x_of_total,
               operator = mic.operator,
               action = mic.action
             ))
           })
         }
       }

       // indicator告警不为空
       if(indicatorAlarmList.nonEmpty){
         val indicator = indicatorCacheSet.head
         val alarmLevelRule = AlarmLevelRule(
           toolName = indicator.toolName,
           chamberName = indicator.chamberName,
           recipeName = indicator.recipeName,
           controlPlnId = indicator.controlPlnId,
           controlPlanName = indicator.controlPlanName,
           controlPlanVersion = indicator.controlPlanVersion,
           runId = indicator.runId,
           dataMissingRatio = indicator.dataMissingRatio,
           windowStartTime = indicator.windowStartTime,
           windowEndTime = indicator.windowEndTime,
           windowDataCreateTime = indicator.windowDataCreateTime,
           indicatorCreateTime = indicator.indicatorCreateTime,
           alarmCreateTime = indicator.alarmCreateTime,
           configMissingRatio = indicator.configMissingRatio,
           runStartTime = indicator.runStartTime,
           runEndTime = indicator.runEndTime,
           locationId = indicator.locationId,
           locationName = indicator.locationName,
           moduleId = indicator.moduleId,
           moduleName = indicator.moduleName,
           toolGroupId = indicator.toolGroupId,
           toolGroupName = indicator.toolGroupName,
           chamberGroupId = indicator.chamberGroupId,
           chamberGroupName = indicator.chamberGroupName,
           pmStatus = indicator.pmStatus,
           pmTimestamp = indicator.pmTimestamp,
           materialName = indicator.materialName,
           area = indicator.area,
           section = indicator.section,
           mesChamberName = indicator.mesChamberName,
           dataVersion = indicator.dataVersion,
           lotMESInfo = indicator.lotMESInfo,
           alarm = indicatorAlarmList.toList,
           micAlarm = micAlarmList.toList)

         return alarmLevelRule
       }else if(micAlarmList.nonEmpty){

         val indicator = micCacheSet.head
         val alarmLevelRule = AlarmLevelRule(
           toolName = indicator.toolName,
           chamberName = indicator.chamberName,
           recipeName = indicator.recipeName,
           controlPlnId = indicator.controlPlanId,
           controlPlanName = indicator.controlPlanName,
           controlPlanVersion = indicator.controlPlanVersion.toInt,
           runId = indicator.runId,
           dataMissingRatio = indicator.dataMissingRatio,
           windowStartTime = indicator.windowStartTime,
           windowEndTime = indicator.windowEndTime,
           windowDataCreateTime = indicator.windowDataCreateTime,
           indicatorCreateTime = System.currentTimeMillis(),
           alarmCreateTime = indicator.alarmCreateTime,
           configMissingRatio = indicator.configMissingRatio,
           runStartTime = indicator.runStartTime,
           runEndTime = indicator.runEndTime,
           locationId = indicator.locationId,
           locationName = indicator.locationName,
           moduleId = indicator.moduleId,
           moduleName = indicator.moduleName,
           toolGroupId = indicator.toolGroupId,
           toolGroupName = indicator.toolGroupName,
           chamberGroupId = indicator.chamberGroupId,
           chamberGroupName = indicator.chamberGroupName,
           pmStatus = "",
           pmTimestamp = 0L,
           materialName = indicator.materialName,
           area = indicator.area,
           section = indicator.section,
           mesChamberName = indicator.mesChamberName,
           dataVersion = indicator.dataVersion,
           lotMESInfo = indicator.lotMESInfo,
           alarm = indicatorAlarmList.toList,
           micAlarm = micAlarmList.toList)

         return alarmLevelRule
       }
    }catch {
      case ex: Exception => logger.error(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("range" -> "isProcessEndResult失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
    null
  }




  /**
   *  增加mic配置
   */
  def addMicConfigToTCSP(config: ConfigData[MicConfig]): Unit = {
    try {

      val micConfigData = config.datas
      val controlPlanId = micConfigData.controlPlanId
      val micId = s"micId|${micConfigData.micId}"

//      logger.warn(s"runAlarm addMicConfigToTCSP: " + config)

      if (!config.status) {
        //删除indicatorConfig逻辑
        if (this.runEndConfig.contains(controlPlanId)) {

          //有一样的key
          val IndicatorSet = this.runEndConfig(controlPlanId)
          IndicatorSet.remove(micId)
        }
      } else {
        //新增逻辑
        if (this.runEndConfig.contains(controlPlanId)) {
          val IndicatorSet = this.runEndConfig(controlPlanId)

          IndicatorSet.add(micId)
          this.runEndConfig.put(controlPlanId, IndicatorSet)

        } else {
          //没有一样的key
          this.runEndConfig.put(controlPlanId, mutable.Set(micId))
        }
      }
    }catch {
      case ex: Exception => logger.error(s"addMicConfigToTCSP ERROR:  $ex")
    }
  }

  /**
   *  添加indicator配置
   */
  def addIndicatorConfigToTCSP(kafkaConfigData: ConfigData[IndicatorConfig]): Unit = {
    try {
      val indicatorConfig = kafkaConfigData.datas
      val controlPlanId = indicatorConfig.controlPlanId

//      logger.warn(s"runAlarm addIndicatorConfigToTCSP_step1: " + kafkaConfigData)

      val indicatorId = s"indicatorId|${indicatorConfig.indicatorId}"

      if (!kafkaConfigData.status) {
        //删除indicatorConfig逻辑
        if (this.runEndConfig.contains(controlPlanId)) {

          //有一样的key
          val IndicatorSet = this.runEndConfig(controlPlanId)
          IndicatorSet.remove(indicatorId)
        }
      } else {
        //新增逻辑
        if (this.runEndConfig.contains(controlPlanId)) {
          val IndicatorSet = this.runEndConfig(controlPlanId)

          IndicatorSet.add(indicatorId)
          runEndConfig.put(controlPlanId, IndicatorSet)

        } else {
          //没有一样的key
          this.runEndConfig.put(controlPlanId, mutable.Set(indicatorId))
        }
      }
    } catch {
      case ex: Exception => logger.error(s"addIndicatorConfigToTCSP_error: $ex")
    }
  }

}


