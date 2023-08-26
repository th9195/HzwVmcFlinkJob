package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.controller.wafern.WaferNByPassController
import com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules.{AlarmProcessObject, AlarmProcessRuleParent, AlarmProcessRule_1_2_6, AlarmProcessRule_3_merge_by_level, AlarmProcessRule_4_5}
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{AlarmConfigDataType, AlarmSwitchDataType, EwmaRetargetDataType, readHbaseAllConfig, readHbaseAllConfigByTable}
import com.hzw.fdc.util._
import com.hzw.fdc.util.redisUtils.RedisUtil
import com.hzw.fdc.util.redisUtils.RedisUtil.getStringDataByPrefixKey
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation, _}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

/**
 * AlarmProcessObject
 *
 * @desc:
 * @author tobytang
 * @date 2022/6/16 13:44
 * @since 1.0.0
 * @update 2022/6/16 13:44
 * */
@SerialVersionUID(1L)
class IndicatorAlarmProcessFunctionNew extends
  KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[IndicatorAlarmProcessFunctionNew])

  //业务逻辑从上到下逐级匹配
  // key为indicatorid + stage --> tool|chamber|recipe|product|stage --> controlPlanVersion --> config配置
  var indicatorRuleByStage = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]]
  // key为indicatorid + product
  var indicatorRuleByProduct = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]]
  // key为indicatorid +recipe
  var indicatorRuleByRecipe = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]]
  // key为indicatorid + chamber
  var indicatorRuleByChamberId = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]]
  // key为indicatorid +tool
  var indicatorRuleByTool = new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]
  // key为indicatorid  1个
  var indicatorRuleByIndicatorId = new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]


  // ewma 缓存数据 {"indicatorid|specDefId": {"tool|chamber|recipe": "Value"}}
  // key1 : indicatorId|specId
  // key2 : 看w2wType 配置，默认都是 tool|chamber|recipe
  // value : 4元组 (target,ucl,lcl,runId)
  var ewmaCacheData = new concurrent.TrieMap[String, concurrent.TrieMap[String, (Option[Double], Option[Double], Option[Double], String,Option[Double])]]

  // key1 : indicatorId|specId
  // key2 : 看w2wType 配置，默认都是 tool|chamber|recipe
  // value : EwmaCacheData
  var ewmaCacheDataMap = new concurrent.TrieMap[String, concurrent.TrieMap[String,EwmaCacheData]]

  // 缓存 ewma 手动触发retarget配置信息
  var ewmaManualRetargetConfigMap = new TrieMap[String,EwmaManualRetargetConfig]()

  // 缓存 ewma 触发retarget配置信息(支持手动/自动 retarget)
  // key : indicatorId|specId|toolName|chamberName
  var ewmaRetargetConfigMap = new TrieMap[String,EwmaRetargetConfig]()

  // ewma 缓存数据侧道输出 为了支持0 down time
  lazy val ewmaCacheOutput = new OutputTag[JsonNode]("AlarmEwmaCache")

  // ewmaRetargetResult 数据侧道输出
  lazy val retargetResultOutput = new OutputTag[JsonNode]("EwmaRetargetResult")


  // 控制alarm开关的配置 key: toolid + chamberid
  val switchEventConfig = new concurrent.TrieMap[String, AlarmSwitchEventConfig]

  // wafer N 需求的 状态对象
  val waferNRequirementImpl = new WaferNByPassController()

  /**
   * 在往mainfab_alarm_config_topic推送alarm配置增加两条开始和结束消息,用以判断plan配置升级是否完成
    */
  val alarmRuleConfigAckSet: mutable.Set[Long] = mutable.Set()

  val alarmProcessRule_1_2_6 = new AlarmProcessRule_1_2_6()
  val alarmProcessRule_3 = new AlarmProcessRule_3_merge_by_level()
  val alarmProcessRule_4_5 = new AlarmProcessRule_4_5()
  var alarmProcessRuleParent = new AlarmProcessRuleParent()

  lazy val AlarmSwitchEventOutput = new OutputTag[List[AlarmSwitchEventConfig]]("AlarmSwitchEvent")

  /** The state that is maintained by this process function */
  private var indicatorIdState: ValueState[taskIdTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val runIdStateDescription = new ValueStateDescriptor[taskIdTimestamp]("alarmIndicatorOutValueState",
      TypeInformation.of(classOf[taskIdTimestamp]))
    indicatorIdState = getRuntimeContext.getState(runIdStateDescription)

    if(!MainFabConstants.IS_DEBUG){  // 开发环境Debug,获取Hbase中的配置信息
      //val initConfig: ListBuffer[ConfigData[AlarmRuleConfig]] = InitFlinkFullConfigHbase.AlarmConfigList
      val initConfig: ListBuffer[ConfigData[AlarmRuleConfig]] = readHbaseAllConfig[AlarmRuleConfig](ProjectConfig.HBASE_SYNC_RULE_TABLE, AlarmConfigDataType)

      logger.warn("hbase数据加载完毕,开始遍历结果的list到嵌套map中...")

      for (alarmConfig <- initConfig) {
        val alarmRuleConfig: AlarmRuleConfig = alarmConfig.datas
        try {
          addAlarmRuleConfig(alarmRuleConfig)
        }catch {
          case e:Exception => logger.warn("Hbase AlarmCofnig addAlarmRuleConfig error: " + e.toString)
        }
      }
      logger.warn("嵌套map中数据加载完毕...")

      //val switchConfig: ListBuffer[ConfigData[AlarmSwitchEventConfig]] = InitFlinkFullConfigHbase.AlarmSwitchConfigList
      val switchConfig: ListBuffer[ConfigData[AlarmSwitchEventConfig]] = readHbaseAllConfig[AlarmSwitchEventConfig](ProjectConfig.HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE, AlarmSwitchDataType)
      for (config <- switchConfig) {
        try {
          updateAlarmSwitchEventConfig(config.datas)
        }catch {
          case e:Exception => logger.warn("updateAlarmSwitchEventConfig error: " + e.toString)
        }
      }

      // 读取Hbase 中ewmaRetargetConfig 策略信息
      val ewmaRetargetCofnigList: ListBuffer[ConfigData[EwmaRetargetConfig]] = readHbaseAllConfigByTable[EwmaRetargetConfig](ProjectConfig.HBASE_SYNC_EWMA_RETARGET_CONFIG_TABLE,EwmaRetargetDataType)
      ewmaRetargetCofnigList.foreach(parseEwmaRetarget(_))

    }

    waferNRequirementImpl.open_new(parameters, this)

    // todo 查询 Redis 中 alarmEwmaCache数据
    if(ProjectConfig.INIT_EWMA_TARGET_FROM_REDIS){
//      initAlarmEwmaCache()
      initAlarmEwmaCacheMap()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
    (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
    try {
      indicatorIdState.value() match {
        case taskIdTimestamp(cacheKey, lastModified)
          if (System.currentTimeMillis() >= lastModified + 20000L) =>
          logger.warn(s"====indicatorId=== ${cacheKey} math is time out 30s")
          indicatorIdState.clear()
          close()
        case _ =>
      }
    }catch {
      case exception: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("onTimer" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }

  /**
   *  数据流
   */
  override def processElement(record: JsonNode,
                              ctx: KeyedBroadcastProcessFunction[String, JsonNode,
                                JsonNode, (String, String)]#ReadOnlyContext, out: Collector[(String, String)]): Unit = {


    try {
      val fdcData = toBean[FdcData[IndicatorResult]](record)

      try {
        val key = s"${fdcData.datas.toolName}|${fdcData.datas.chamberName}|${fdcData.datas.indicatorId}"
        // write the state back
        indicatorIdState.update(taskIdTimestamp(key, System.currentTimeMillis()))

        // schedule the next timer 30 seconds from the current event time
        ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 20000L)
      }catch {
        case ex: Exception => logger.warn("processEnd registerProcessingTimeTimer error: " + ex.toString)
      }

      if(MainFabConstants.IS_DEBUG){
        val toolName = fdcData.datas.toolName
//        logger.error(s"processElement toolName == ${toolName}")
        if(toolName.contains("tom")){
          val result: (AlarmRuleResult, IndicatorLimitResult) = matchAlarmRule(fdcData.datas,ctx)
          out.collect((result._1.toJson,result._2.toJson))
        }

      }else{
        val result: (AlarmRuleResult, IndicatorLimitResult) = matchAlarmRule(fdcData.datas,ctx)
        out.collect((result._1.toJson,result._2.toJson))
      }

      if (indicatorIdState.value() != null) {
        indicatorIdState.clear()
      }

    } catch {
      case ex: Exception => logger.warn(ErrorCode("008008d011D", System.currentTimeMillis(),
        Map("msg" -> "alarm匹配rule规则失败", "record" -> record), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }

  }


  override def processBroadcastElement(value: JsonNode,
                                       ctx: KeyedBroadcastProcessFunction[String, JsonNode,
                                         JsonNode,
                                         (String, String)]#Context,
                                       out: Collector[(String, String)]): Unit = {

    try {
      value.get("dataType").asText() match {

        case "retargetOnceTrigger" => {
          val configData: ConfigData[EwmaRetargetConfig] = toBean[ConfigData[EwmaRetargetConfig]](value)
          parseEwmaRetarget(configData)
        }

        case "alarmconfig" =>
          val alarmConfig: ConfigData[AlarmRuleConfig] = toBean[ConfigData[AlarmRuleConfig]](value)
          updateRuleConfig(alarmConfig)

          // 特殊处理: 在实时推送alarm配置的开始和结束增加两条消息
        case "alarmConfigAck" =>
          val alarmConfigAck: ConfigData[AlarmRuleConfigAck] = toBean[ConfigData[AlarmRuleConfigAck]](value)
          if(alarmConfigAck.datas.signalType == "START"){
            alarmRuleConfigAckSet.add(alarmConfigAck.datas.controlPlanId)
          }else if(alarmConfigAck.datas.signalType == "END"){
            alarmRuleConfigAckSet.remove(alarmConfigAck.datas.controlPlanId)
          }

        case "alarmSwitchEventConfig" =>
          val alarmSwitchEventConfig = toBean[ConfigData[AlarmSwitchEventConfig]](value)

          //          val alarmSwitchEventConfigList: List[AlarmSwitchEventConfig] = alarmSwitchEventConfig.datas.map(elem => {
          //            updateAlarmSwitchEventConfig(elem)
          //          })
          val alarmSwitchEventConfigList: List[AlarmSwitchEventConfig] = List(updateAlarmSwitchEventConfig(alarmSwitchEventConfig.datas))

          // 侧输出流输出
          ctx.output(AlarmSwitchEventOutput, alarmSwitchEventConfigList)

        case _ => logger.warn(s"alarm job Hbase no mach type: " + value)
      }

      // 待其他配置逻辑处理完毕后，再更新waferN需求的配置，非短路式调用，该方法保证不影响后续流程，自行消化内部异常。
      // 暂时先放在这里，目前waferN的配置是否跟其他配置存在逻辑关系待定。
      waferNRequirementImpl.updateConfig_new(value, this)
    } catch {
      case e:Exception =>logger.warn(s"alarm job add config error: ${value.toJson} Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }

  }

  /**
   *
   * @param configData
   */
  def parseEwmaRetarget(configData: ConfigData[EwmaRetargetConfig]): Unit = {
    try{

      if(judgeEwmaRetargetConfig(configData)){
        val ewmaRetargetConfig: EwmaRetargetConfig  = configData.datas
        val indicatorId = ewmaRetargetConfig.indicatorId
        val specId = ewmaRetargetConfig.specId
        val fdcToolName = ewmaRetargetConfig.fdcToolName
        val fdcChamberName = ewmaRetargetConfig.fdcChamberName
        val key = s"${indicatorId.toString}|${specId.toString}|${fdcToolName}|${fdcChamberName}"
        ewmaRetargetConfigMap.put(key,ewmaRetargetConfig)
      }
    }catch {
      case e:Exception => {
        logger.error(s"EWMA retarget error: ewmaRetargetConfig == ${configData} ; \n " +
          s"ExceptionInfo: ${ExceptionInfo.getExceptionInfo(e)}")
      }
    }
  }

  /**
   * 初步校验一下 ewmaRetargetConfig 策略信息是否正常
   * @param configData
   * @return
   */
  def judgeEwmaRetargetConfig(configData: ConfigData[EwmaRetargetConfig]) = {
    var res = true
    if(!configData.status){
      logger.error(s"ewmaOnceRetarget config 有误 status == ${configData.status} ;\n " +
        s"configData == ${configData.toJson}")
      res = false
    }else{
      val ewmaRetargetConfig: EwmaRetargetConfig = configData.datas

      // 判断 rangeU rangeL
      val rangeU = ewmaRetargetConfig.rangeU
      val rangeL = ewmaRetargetConfig.rangeL
      if(rangeU < rangeL){
        logger.error(s"ewmaOnceRetarget config 有误 rangeU < rangeL;\n " +
          s"configData == ${configData.toJson}")
        res = false
      }

      // 判断 fdcToolName fdcChamberName indicatorId specId
      val indicatorId: Long = ewmaRetargetConfig.indicatorId
      val specId: Long = ewmaRetargetConfig.specId
      val fdcToolName: String = ewmaRetargetConfig.fdcToolName
      val fdcChamberName: String = ewmaRetargetConfig.fdcChamberName
      if(null == indicatorId ||
          null == specId ||
          StringUtils.isBlank(fdcToolName)||
          StringUtils.isBlank(fdcChamberName)){
        logger.error(s"ewmaOnceRetarget config 有误 基础信息异常;\n " +
          s"configData == ${configData.toJson}")
        res = false
      }
    }
    res
  }

  /**
   * 增加alarm 开关的配置
   */
  def updateAlarmSwitchEventConfig(config: AlarmSwitchEventConfig): AlarmSwitchEventConfig = {
    val key = config.toolName + "|" + config.chamberName
    val action = config.action.replace("TURN_", "")
    val res = AlarmSwitchEventConfig(config.toolName, config.chamberName, action, config.timeStamp, config.eventId)
    if (switchEventConfig.contains(key)) {
      switchEventConfig.put(key, res)
    } else {
      switchEventConfig += (key -> res)
    }
    res
  }


  /**
   * 匹配 rule 配置
   * @param indicatorResult
   * @return
   */
  def matchAlarmRule(indicatorResult: IndicatorResult,
                     ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)]#ReadOnlyContext
                    ): (AlarmRuleResult, IndicatorLimitResult) = {
    try {
      //逐层拿key
      val version = indicatorResult.controlPlanVersion.toString
      val indicatorIdKey = indicatorResult.indicatorId.toString
      val toolKey = geneAlarmKey(indicatorIdKey, indicatorResult.toolName)
      val chamberKey = geneAlarmKey(indicatorIdKey, indicatorResult.chamberName)
      val recipeKey = geneAlarmKey(indicatorIdKey, indicatorResult.recipeName)

      // todo 1-  特殊处理 stage
      for (stageName: String <- indicatorResult.stage) {
        val stageKey = geneAlarmKey(indicatorIdKey, stageName)

        if (this.indicatorRuleByStage.contains(stageKey)) {
          //匹配Stage
          val indicatorRuleByStageMap = indicatorRuleByStage(stageKey)
          for(indicatorRuleByStageVersion <- indicatorRuleByStageMap.values) {
            //匹配版本
            if (indicatorRuleByStageVersion.contains(version)) {
              val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByStageVersion(version)
              if (isMatch(alarmRuleConfig, indicatorResult)) {
//                val resultTuple: (AlarmRuleResult, IndicatorLimitResult) = calculateRule(indicatorRule, indicatorResult, stageKey)
                return calculateRule(alarmRuleConfig, indicatorResult, stageKey,ctx)
              }
            }else{
              // 只有在升级alarm配置过程中才会走这块代码逻辑
              if(alarmRuleConfigAckSet.contains(indicatorResult.controlPlanId)) {
                /**
                 * F1P6-212	【一线问题】plan激活后，概率出现alarm-48,49
                 * 特殊处理 stage， indicator数据流带的planVersion比alarm配置的planVersion大的情况
                 */
//                val maxPlan = indicatorRuleByStageVersion.keys.map(_.toLong).max
//                if (indicatorResult.controlPlanVersion > maxPlan) {
//                  val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByStageVersion(maxPlan.toString)
//                  if (isMatch(alarmRuleConfig, indicatorResult)) {
//                    return calculateRule(alarmRuleConfig, indicatorResult, stageKey, ctx)
//                  }
//                }

                val lastVersion = (indicatorResult.controlPlanVersion-1).toString
                if (indicatorRuleByStageVersion.contains(lastVersion)) {
                  val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByStageVersion(lastVersion)
                  if (isMatch(alarmRuleConfig, indicatorResult)) {
                    return calculateRule(alarmRuleConfig, indicatorResult, stageKey,ctx)
                  }
                }
              }
            }
          }
        }
      }


      // todo 2- 特殊处理 product
      for (productName <- indicatorResult.product) {
        val productKey = geneAlarmKey(indicatorIdKey, productName)
        if (this.indicatorRuleByProduct.contains(productKey)) {
          //匹配Product
          val indicatorRuleByProductMap = indicatorRuleByProduct(productKey)
          for(indicatorRuleByProductVersion <- indicatorRuleByProductMap.values) {
            //匹配版本
            if (indicatorRuleByProductVersion.contains(version)) {
              val indicatorRule = indicatorRuleByProductVersion(version)
              if (isMatch(indicatorRule, indicatorResult)) {
                return calculateRule(indicatorRule, indicatorResult, productKey,ctx)
              }
            }else{
              // 只有在升级alarm配置过程中才会走这块代码逻辑
              if(alarmRuleConfigAckSet.contains(indicatorResult.controlPlanId)) {
                /**
                 * 特殊处理 product, indicator数据流带的planVersion比alarm配置的planVersion大的情况
                 */
//                val maxPlan = indicatorRuleByProductVersion.keys.map(_.toLong).max
//                if (indicatorResult.controlPlanVersion > maxPlan) {
//                  val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByProductVersion(maxPlan.toString)
//                  if (isMatch(alarmRuleConfig, indicatorResult)) {
//                    return calculateRule(alarmRuleConfig, indicatorResult, productKey, ctx)
//                  }
//                }

                val lastVersion = (indicatorResult.controlPlanVersion-1).toString
                if (indicatorRuleByProductVersion.contains(lastVersion)) {
                  val indicatorRule = indicatorRuleByProductVersion(lastVersion)
                  if (isMatch(indicatorRule, indicatorResult)) {
                    return calculateRule(indicatorRule, indicatorResult, productKey,ctx)
                  }
                }
              }
            }
          }
        }
      }


      // todo 3 处理 Recipe
      if (this.indicatorRuleByRecipe.contains(recipeKey)) {
        //匹配Recipe
        val indicatorRuleByRecipeMap = indicatorRuleByRecipe(recipeKey)

        for(indicatorRuleByRecipeVersion <- indicatorRuleByRecipeMap.values) {
          //匹配版本
          if (indicatorRuleByRecipeVersion.contains(version)) {
            val indicatorRule = indicatorRuleByRecipeVersion(version)
            if (isMatch(indicatorRule, indicatorResult)) {
              return calculateRule(indicatorRule, indicatorResult, recipeKey,ctx)
            }
          }else{
            // 只有在升级alarm配置过程中才会走这块代码逻辑
            if(alarmRuleConfigAckSet.contains(indicatorResult.controlPlanId)) {
              /**
               * 特殊处理 recipe, indicator数据流带的planVersion比alarm配置的planVersion大的情况
               */
//              val maxPlan = indicatorRuleByRecipeVersion.keys.map(_.toLong).max
//              if (indicatorResult.controlPlanVersion > maxPlan) {
//                val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByRecipeVersion(maxPlan.toString)
//                if (isMatch(alarmRuleConfig, indicatorResult)) {
//                  return calculateRule(alarmRuleConfig, indicatorResult, recipeKey, ctx)
//                }
//              }
              val lastVersion = (indicatorResult.controlPlanVersion-1).toString

              if (indicatorRuleByRecipeVersion.contains(lastVersion)) {
                val indicatorRule = indicatorRuleByRecipeVersion(lastVersion)
                if (isMatch(indicatorRule, indicatorResult)) {
                  return calculateRule(indicatorRule, indicatorResult, recipeKey,ctx)
                }
              }
            }
          }
        }
      }

      // todo 4 处理 Chamber
      if (this.indicatorRuleByChamberId.contains(chamberKey)) {
        //匹配Chamber
        val indicatorRuleByChamberIdMap = indicatorRuleByChamberId(chamberKey)

        for(indicatorRuleByChamberIdVersion <- indicatorRuleByChamberIdMap.values) {
          //匹配版本
          if (indicatorRuleByChamberIdVersion.contains(version)) {
            val indicatorRule = indicatorRuleByChamberIdVersion(version)
            if (isMatch(indicatorRule, indicatorResult)) {
              return calculateRule(indicatorRule, indicatorResult, chamberKey,ctx)
            }
          }else{
            // 只有在升级alarm配置过程中才会走这块代码逻辑
            if(alarmRuleConfigAckSet.contains(indicatorResult.controlPlanId)) {
              /**
               * 特殊处理 recipe, indicator数据流带的planVersion比alarm配置的planVersion大的情况
               */
//              val maxPlan = indicatorRuleByChamberIdVersion.keys.map(_.toLong).max
//              if (indicatorResult.controlPlanVersion > maxPlan) {
//                val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByChamberIdVersion(maxPlan.toString)
//                if (isMatch(alarmRuleConfig, indicatorResult)) {
//                  return calculateRule(alarmRuleConfig, indicatorResult, chamberKey, ctx)
//                }
//              }
              val lastVersion = (indicatorResult.controlPlanVersion-1).toString
              if (indicatorRuleByChamberIdVersion.contains(lastVersion)) {
                val indicatorRule = indicatorRuleByChamberIdVersion(lastVersion)
                if (isMatch(indicatorRule, indicatorResult)) {
                  return calculateRule(indicatorRule, indicatorResult, chamberKey,ctx)
                }
              }

            }
          }
        }
//        logger.warn(s"-45 alarm job EmptyOOCAndOCAP unmatch indicator: ${indicatorResult} ")
      }

      // todo 5 处理 Tool
      if (this.indicatorRuleByTool.contains(toolKey)) {
        //匹配Tool
        val indicatorRuleByToolVersion = indicatorRuleByTool(toolKey)

        //匹配版本
        if (indicatorRuleByToolVersion.contains(version)) {
          val indicatorRule = indicatorRuleByToolVersion(version)
          if(isMatch(indicatorRule, indicatorResult)) {
            return calculateRule(indicatorRule, indicatorResult, toolKey,ctx)
          }
        }else{
          // 只有在升级alarm配置过程中才会走这块代码逻辑
          if(alarmRuleConfigAckSet.contains(indicatorResult.controlPlanId)) {
            /**
             * 特殊处理 Tool, indicator数据流带的planVersion比alarm配置的planVersion大的情况
             */
//            val maxPlan = indicatorRuleByToolVersion.keys.map(_.toLong).max
//            if (indicatorResult.controlPlanVersion > maxPlan) {
//              val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByToolVersion(maxPlan.toString)
//              if (isMatch(alarmRuleConfig, indicatorResult)) {
//                return calculateRule(alarmRuleConfig, indicatorResult, toolKey, ctx)
//              }
//            }
            val lastVersion = (indicatorResult.controlPlanVersion-1).toString
            if (indicatorRuleByToolVersion.contains(lastVersion)) {
              val indicatorRule = indicatorRuleByToolVersion(lastVersion)
              if(isMatch(indicatorRule, indicatorResult)) {
                return calculateRule(indicatorRule, indicatorResult, toolKey,ctx)
              }
            }
          }
        }
      }

      if (this.indicatorRuleByIndicatorId.contains(indicatorIdKey)) {

        // todo 6 处理 IndicatorId
        val indicatorRuleByIndicatorIdVersion = indicatorRuleByIndicatorId(indicatorIdKey)

        //匹配版本
        if (indicatorRuleByIndicatorIdVersion.contains(version)) {
          val indicatorRule = indicatorRuleByIndicatorIdVersion(version)
          return calculateRule(indicatorRule, indicatorResult, indicatorIdKey,ctx)
        } else {
          logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator: ${indicatorResult.indicatorName} version：" +
            s"$version in $indicatorRuleByIndicatorIdVersion")
          // 只有在升级alarm配置过程中才会走这块代码逻辑
          if(alarmRuleConfigAckSet.contains(indicatorResult.controlPlanId)) {
            /**
             * 特殊处理 IndicatorId, indicator数据流带的planVersion和alarm配置的planVersion不一样的情况
             */
//            val maxPlan = indicatorRuleByIndicatorIdVersion.keys.map(_.toLong).max
//            if (indicatorResult.controlPlanVersion > maxPlan) {
//              val alarmRuleConfig: AlarmRuleConfig = indicatorRuleByIndicatorIdVersion(maxPlan.toString)
//              return calculateRule(alarmRuleConfig, indicatorResult, indicatorIdKey, ctx)
//            }
            val lastVersion = (indicatorResult.controlPlanVersion-1).toString
            if (indicatorRuleByIndicatorIdVersion.contains(lastVersion)) {
              val indicatorRule = indicatorRuleByIndicatorIdVersion(lastVersion)
              return calculateRule(indicatorRule, indicatorResult, indicatorIdKey,ctx)
            }
          }
        }
      }

      //没匹配到配置
      EmptyOOCAndOCAP(indicatorResult, -49)

    } catch {
      case ex: NullPointerException => logger.warn(s"alarm job matchAlarmRule NullPointerException: ${ExceptionInfo.getExceptionInfo(ex)} data: $indicatorResult")
        val ruleNull: AlarmRuleResult = GetEmptyRule(indicatorResult,-7, 0,"N/N/N/N/N/N","ON")
        val IndicatorResult = IndicatorLimitResult("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", -7, 0, "N/A", "N/A", "N/A", "")
        (ruleNull, IndicatorResult)
      case ex: Exception => logger.warn(s"alarm job matchAlarmRule data Exception${ExceptionInfo.getExceptionInfo(ex)} data: $indicatorResult ")
        val ruleNull: AlarmRuleResult = GetEmptyRule(indicatorResult,-7, 0,"N/N/N/N/N/N","ON")
        val IndicatorResult = IndicatorLimitResult("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", -7, 0, "N/A", "N/A", "N/A", "")
        (ruleNull, IndicatorResult)
    }
  }

  /**
   * 匹配indicator 和alarm 配置中的 tool chamber recipe product stage是否一样
   * @param indicatorRule AlarmRuleConfig
   * @param indicatorResult IndicatorResult
   * @return
   */
  def isMatch(indicatorRule: AlarmRuleConfig, indicatorResult: IndicatorResult): Boolean = {
    var status = true
    try{

      if(indicatorRule.recipeName.nonEmpty){
        if(indicatorRule.recipeName.get != indicatorResult.recipeName){
          status = false
        }
      }

      if(indicatorRule.chamberName.nonEmpty){
        if(indicatorRule.chamberName.get != indicatorResult.chamberName){
          status = false
        }
      }

      if(indicatorRule.toolName.nonEmpty){
        if(indicatorRule.toolName.get != indicatorResult.toolName){
          status = false
        }
      }

      if(indicatorRule.productName.nonEmpty){
        val productName = indicatorRule.productName.get
        if(!indicatorResult.product.contains(productName)){
          status = false
        }
      }

      if(indicatorRule.stage.nonEmpty){
        val stage = indicatorRule.stage.get
        if(!indicatorResult.stage.contains(stage)){
          status = false
        }
      }

    }catch {
      case ex: Exception => logger.warn(s"alarm job isMatch Exception${ExceptionInfo.getExceptionInfo(ex)}")
    }

    status
  }



  /**
   * 处理从kafka topic新增的Rule配置数据
   */
  def updateRuleConfig(record: ConfigData[AlarmRuleConfig]): Unit = {
    try {

      //RULE配置
      val alarmRuleConfig = record.datas

      //只有tool、chamber、recipe、product、indicatorId能决定全局唯一配置
      if (record.status) {

        addAlarmRuleConfig(alarmRuleConfig,false)

      } else if (!record.status) {
        if (alarmRuleConfig.stage.nonEmpty) {
          val key1 = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.stage.get)
          this.indicatorRuleByStage.remove(key1)
          //删除rule状态
          alarmProcessRuleParent.removeRuleData(key1)
        } else if (alarmRuleConfig.productName.nonEmpty) {
          val key1 = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.productName.get)
          this.indicatorRuleByProduct.remove(key1)
          //删除rule状态
          alarmProcessRuleParent.removeRuleData(key1)
        } else if (alarmRuleConfig.recipeName.nonEmpty) {
          val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.recipeName.get)
          this.indicatorRuleByRecipe.remove(key)
          //删除rule状态
          alarmProcessRule_3.removeRuleData(key)
        } else if (alarmRuleConfig.chamberName.nonEmpty) {
          val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.chamberName.get)
          this.indicatorRuleByChamberId.remove(key)
          //删除rule状态
          alarmProcessRuleParent.removeRuleData(key)
        } else if (alarmRuleConfig.toolName.nonEmpty) {
          val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.toolName.get)
          this.indicatorRuleByTool.remove(key)
          //删除rule状态
          alarmProcessRuleParent.removeRuleData(key)
        } else {
          val key = alarmRuleConfig.indicatorId
          this.indicatorRuleByIndicatorId.remove(key)
          //删除rule状态
          alarmProcessRuleParent.removeRuleData(key)
        }

      } else {
        logger.warn(s"alarm job indicatorRule match fail data: $record ")
      }

    } catch {
      case ex: Exception => logger.warn(s"alarm job indicatorRule Exception${ExceptionInfo.getExceptionInfo(ex)} data: $record ")
    }
  }
  /**
   * 新增rule配置
   * @param alarmRuleConfig
   */
  def addAlarmRuleConfig(alarmRuleConfig: AlarmRuleConfig,isHbaseData:Boolean = true): Unit = {
    val version = alarmRuleConfig.controlPlanVersion.toString

    val recodeKey = s"${alarmRuleConfig.toolName}|${alarmRuleConfig.chamberName}|${alarmRuleConfig.recipeName}|" +
      s"${alarmRuleConfig.productName}|${alarmRuleConfig.stage}"

    if (alarmRuleConfig.stage.nonEmpty) {
      val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.stage.get)

      val indicatorRuleByStageTmp = if(indicatorRuleByStage.contains(key))  indicatorRuleByStage(key)
      else new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]()

      val indicatorRuleByStageTmpRes = updateConfigData(alarmRuleConfig, version, recodeKey, indicatorRuleByStageTmp,isHbaseData)
      indicatorRuleByStage.put(key, indicatorRuleByStageTmpRes)
//      logger.warn(s"indicatorRuleByStage ==> ${alarmRuleConfig.toString}")
    } else if (alarmRuleConfig.productName.nonEmpty) {
      val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.productName.get)

      val indicatorRuleByProductTmp = if(indicatorRuleByProduct.contains(key))  indicatorRuleByProduct(key)
           else new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]()

      val indicatorRuleByProductTmpRes = updateConfigData(alarmRuleConfig, version, recodeKey, indicatorRuleByProductTmp,isHbaseData)
      indicatorRuleByProduct.put(key, indicatorRuleByProductTmpRes)
//      logger.warn(s"indicatorRuleByProduct ==> ${alarmRuleConfig.toString}")
    } else if (alarmRuleConfig.recipeName.nonEmpty) {
      val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.recipeName.get)

      val indicatorRuleByRecipeTmp = if(indicatorRuleByRecipe.contains(key))  indicatorRuleByRecipe(key)
      else new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]()

      val indicatorRuleByRecipeTmpRes = updateConfigData(alarmRuleConfig, version, recodeKey, indicatorRuleByRecipeTmp,isHbaseData)
      indicatorRuleByRecipe.put(key, indicatorRuleByRecipeTmpRes)
//      logger.warn(s"indicatorRuleByRecipe ==> ${alarmRuleConfig.toString}")
    } else if (alarmRuleConfig.chamberName.nonEmpty) {
      val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.chamberName.get)

      val indicatorRuleByChamberIdTmp = if(indicatorRuleByChamberId.contains(key))  indicatorRuleByChamberId(key)
      else new concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]]()

      val indicatorRuleByChamberIdTmpRes = updateConfigData(alarmRuleConfig, version, recodeKey, indicatorRuleByChamberIdTmp,isHbaseData)
      indicatorRuleByChamberId.put(key, indicatorRuleByChamberIdTmpRes)
//      logger.warn(s"indicatorRuleByChamberId ==> ${alarmRuleConfig.toString}")
    } else if (alarmRuleConfig.toolName.nonEmpty) {
      val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.toolName.get)
      val indicatorRuleByToolTmp = this.indicatorRuleByTool
      this.indicatorRuleByTool = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByToolTmp,isHbaseData)
//      logger.warn(s"indicatorRuleByTool ==> ${alarmRuleConfig.toString}")
    } else {
      val key = alarmRuleConfig.indicatorId
      val indicatorRuleByIndicatorIdTmp = this.indicatorRuleByIndicatorId
      this.indicatorRuleByIndicatorId = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByIndicatorIdTmp,isHbaseData)

//      logger.warn(s"indicatorRuleByIndicatorId ==> ${alarmRuleConfig.toString}")
    }

  }


  /**
   * 升级配置版本
   *
   * @param alarmRuleConfig
   * @param version
   * @param key
   * @param indicatorRule
   * @return
   */
  def updateConfigData(alarmRuleConfig: AlarmRuleConfig, version: String, key: String, indicatorRule: concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]],isHbaseData:Boolean = true):
  concurrent.TrieMap[String, concurrent.TrieMap[String, AlarmRuleConfig]] = {
    try {
      // 新增数据
      if (!indicatorRule.contains(key)) {
        val versionMap = concurrent.TrieMap[String, AlarmRuleConfig](version -> alarmRuleConfig)

        indicatorRule.put(key, versionMap)
        if(!isHbaseData){
          logger.warn(s"add new alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
        }

      } else {

        val alarmRuleMap = indicatorRule(key)
        alarmRuleMap += (version -> alarmRuleConfig)
        indicatorRule.put(key, alarmRuleMap)

        if(!isHbaseData){
          logger.warn(s"add upData alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
        }

        // 更新版本, 同一个indicator只保留两个版本
        val versionAndAlarmRuleConfig = indicatorRule(key)
        val keys = versionAndAlarmRuleConfig.keys.map(_.toLong)
        if (keys.size > 2) {
          val s = keys.min
          val delAlarmRuleConfig = versionAndAlarmRuleConfig.remove(s.toString) // 删除低版本的配置
          indicatorRule.put(key, versionAndAlarmRuleConfig)
          if(!isHbaseData){
            logger.warn(s"del version alarm config key：$key  delAlarmRuleConfig :${delAlarmRuleConfig.toJson}")
          }
        }

        val newKeys = versionAndAlarmRuleConfig.keys.map(_.toLong)
        val oldKey = newKeys.min.toString
        val newKey = newKeys.max.toString

        val oldConfig: AlarmRuleConfig = versionAndAlarmRuleConfig(oldKey)
        val newConfig: AlarmRuleConfig = versionAndAlarmRuleConfig(newKey)

        // todo 判断是否清空 所有的统计计数
        judgeResetAllAlarmCount(key, oldConfig, newConfig)
      }
    } catch {
      case ex: Exception => logger.warn(s"alarm job updateData Exception: ${ExceptionInfo.getExceptionInfo(ex)}")
    }
    indicatorRule
  }


  /**
   *
   * 判断两个版本的配置是否 一样
   * 如果一样：返回 false  不清零
   * 如果不一样： 返回 true 清零
   *
   * @param oldConfig
   * @param newConfig
   * @return
   */
  def compareIsClean(oldConfig: AlarmRuleConfig, newConfig: AlarmRuleConfig) = {
    // 由于新旧配置的 controlPlanVersion 决定不是一样的。 所以对比的时候不能对比 controlPlanVersion
    val oldConfig_0version = AlarmRuleConfig(
      controlPlanId = oldConfig.controlPlanId,
      controlPlanVersion = 0,
      toolName = oldConfig.toolName,
      chamberName = oldConfig.chamberName,
      recipeName = oldConfig.recipeName,
      productName = oldConfig.productName,
      stage = oldConfig.stage,
      indicatorId = oldConfig.indicatorId,
      specDefId = oldConfig.specDefId,
      w2wType = oldConfig.w2wType,
      EwmaArgs = oldConfig.EwmaArgs,
      limit = oldConfig.limit,
      rule = oldConfig.rule,
      limitConditionName = oldConfig.limitConditionName,
      indicatorType = oldConfig.indicatorType,
      isEwma = oldConfig.isEwma)

    val newConfig_0version = AlarmRuleConfig(
      controlPlanId = newConfig.controlPlanId,
      controlPlanVersion = 0,
      toolName = newConfig.toolName,
      chamberName = newConfig.chamberName,
      recipeName = newConfig.recipeName,
      productName = newConfig.productName,
      stage = newConfig.stage,
      indicatorId = newConfig.indicatorId,
      specDefId = newConfig.specDefId,
      w2wType = newConfig.w2wType,
      EwmaArgs = newConfig.EwmaArgs,
      limit = newConfig.limit,
      rule = newConfig.rule,
      limitConditionName = newConfig.limitConditionName,
      indicatorType = newConfig.indicatorType,
      isEwma = newConfig.isEwma)

    // 如果 两个配置不同 就需要清零
    oldConfig_0version != newConfig_0version
  }

  def updateEwmaCacheData(w2wKey:String,
                          nowTarget:Double,
                          ewmaKey: String,
                          a_UCL: Option[Double],
                          a_LCL: Option[Double],
                          runId: String,
                          indicatorValue:Option[Double]) = {
    if (ewmaCacheData.contains(ewmaKey)) {
      val alarmRuleMap = ewmaCacheData(ewmaKey)
      alarmRuleMap.put(w2wKey, (Option.apply(nowTarget), a_UCL, a_LCL, runId,indicatorValue))
      ewmaCacheData.put(ewmaKey, alarmRuleMap)
    } else {
      val alarmRuleMapScala = concurrent.TrieMap[String, (Option[Double], Option[Double], Option[Double], String,Option[Double])](w2wKey -> (Option.apply(nowTarget), a_UCL, a_LCL, runId,indicatorValue))
      ewmaCacheData.put(ewmaKey, alarmRuleMapScala)
    }
  }


  /**
   * 更新ewma缓存信息，并侧道输出写入Redis
   * @param w2wKey
   * @param ewmaKey
   * @param newTarget
   * @param UCL
   * @param LCL
   * @param indicatorResult
   * @param ctx
   */
  def updateEwmaCacheDataBeanAndOutput(w2wKey:String,
                                       ewmaKey: String,
                                       newTarget:Double,
                                       UCL: Option[Double],
                                       LCL: Option[Double],
                                       indicatorResult:IndicatorResult,
                                       ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)]#ReadOnlyContext) = {
    val runId = indicatorResult.runId
    val indicatorValue = indicatorResult.indicatorValue.toDouble

    val elemEwmaCache = EwmaCacheData(target = Option.apply(newTarget),
      ucl = UCL,
      lcl = LCL,
      runId = runId,
      lastIndicatorValue = Option(indicatorValue))

    logger.warn(s"update ewmaCacheDataMap \n " +
      s"ewmaKey = ${ewmaKey} \n " +
      s"w2wKey = ${w2wKey} \n " +
      s"elemEwmaCache = ${elemEwmaCache.toJson}")

    if (ewmaCacheDataMap.contains(ewmaKey)) {
      val alarmRuleMap = ewmaCacheDataMap(ewmaKey)
      alarmRuleMap.put(w2wKey,elemEwmaCache)
      ewmaCacheDataMap.put(ewmaKey, alarmRuleMap)
    } else {
      val alarmRuleMap = concurrent.TrieMap[String,EwmaCacheData](w2wKey -> elemEwmaCache)
      ewmaCacheDataMap.put(ewmaKey, alarmRuleMap)
    }

    // todo output ewma cache data
    val alarmEwmaCacheData = AlarmEwmaCacheData(ewmaKey = ewmaKey,
      w2wKey = w2wKey,
      target = Option.apply(newTarget),
      ucl = UCL,
      lcl = LCL,
      runId = runId,
      lastIndicatorValue = Option(indicatorValue),
      dataVersion = indicatorResult.dataVersion,
      configVersion = ProjectConfig.JOB_VERSION)

    val outputJsonNode: JsonNode = beanToJsonNode[RedisCache[AlarmEwmaCacheData]](RedisCache("alarmEwmaCache",alarmEwmaCacheData))

    ctx.output(ewmaCacheOutput, outputJsonNode)

  }


  /**
   * 侧道输出 retarget 结果数据
   * @param dataVersion
   * @param ewmaRetargetConfig
   * @param retargetStatus
   * @param errorType
   * @param errorMessage
   * @param ctx
   */
  def ewmaRetargetResultOutput(runId:String,dataVersion:String, ewmaRetargetConfig: EwmaRetargetConfig,retargetStatus: Boolean, errorType: String, errorMessage: String, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)]#ReadOnlyContext) = {
    val ewmaRetargetResult = EwmaRetargetResult(retargetCreateTime = ewmaRetargetConfig.retargetCreateTime,
      retargetTime = System.currentTimeMillis(),
      moduleName = ewmaRetargetConfig.moduleName,
      fdcToolGroupName = ewmaRetargetConfig.fdcToolGroupName,
      fdcChamberGroupName = ewmaRetargetConfig.fdcChamberGroupName,
      controlPlanName = ewmaRetargetConfig.controlPlanName,
      controlPlanId = ewmaRetargetConfig.controlPlanId,
      indicatorName = ewmaRetargetConfig.indicatorName,
      indicatorId = ewmaRetargetConfig.indicatorId,
      specId = ewmaRetargetConfig.specId,
      limitConditionName = ewmaRetargetConfig.limitConditionName,
      fdcToolName = ewmaRetargetConfig.fdcToolName,
      fdcChamberName = ewmaRetargetConfig.fdcChamberName,
      mesToolName = ewmaRetargetConfig.mesToolName,
      mesChamberName = ewmaRetargetConfig.mesChamberName,
      pmName = ewmaRetargetConfig.pmName,
      retargetType = ewmaRetargetConfig.retargetType,
      rangeU = ewmaRetargetConfig.rangeU,
      rangeL = ewmaRetargetConfig.rangeL,
      retargetStatus = retargetStatus,
      errorType = errorType,
      errorMessage = errorMessage,
      dataVersion = dataVersion,
      configVersion = ProjectConfig.JOB_VERSION,
      runId = runId,
      retargetUser = ewmaRetargetConfig.retargetUser)

    val outputJsonNode: JsonNode = beanToJsonNode[FdcData[EwmaRetargetResult]](FdcData("ewmaRetargetResult", ewmaRetargetResult))

    ctx.output(retargetResultOutput,outputJsonNode)
  }

  /**
   * 计算rule
   *
   */
  def calculateRule(alarmRuleConfig: AlarmRuleConfig,
                    indicatorResult: IndicatorResult,
                    ruleKey: String,
                    ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)]#ReadOnlyContext
                   ): (AlarmRuleResult, IndicatorLimitResult) = {

    val configMissingRatio = indicatorResult.configMissingRatio
    val nowMissingRatio = indicatorResult.missingRatio
    val w2wType = alarmRuleConfig.w2wType
    val switchKey = s"${indicatorResult.toolName}|${indicatorResult.chamberName}"

    val alarmConfigLimit = alarmRuleConfig.limit

    if (nowMissingRatio >= configMissingRatio) {
      //缺点率大于预设值 不算
      val ruleNull = parseAlarmLevelRule(indicatorResult, alarmRuleConfig, "N/A", "N/A", Nil,-5,0,"ON")

      (ruleNull, getIndicatorLimitResult(indicatorResult, -5, 0, "N/N/N/N/N/N", "ON"))

    } else if (switchEventConfig.contains(switchKey) && switchEventConfig(switchKey).action.equals("OFF") ) {

      //卡控alarm 不算
      val ruleNull = parseAlarmLevelRule(indicatorResult, alarmRuleConfig, "N/A", "N/A", Nil,-6,0,"ON")

      (ruleNull, getIndicatorLimitResult(indicatorResult, -6, 0, "N/N/N/N/N/N", "OFF"))
    } else {

      try {
        // ewma主要处理逻辑
        if (alarmRuleConfig.EwmaArgs != null) {

          val newLimit: AlarmRuleLimit = processEwma(indicatorResult,alarmRuleConfig,ctx)
//          logger.warn(s"-------------------------- ewma end ------------------\n " +
//            s"processEwma result newLimit == ${newLimit.toJson}")
          if (newLimit != null) {
            alarmConfigLimit.UCL = newLimit.UCL
            alarmConfigLimit.LCL = newLimit.LCL
          }

        }
      }catch {
        case ex: Exception => logger.error(s"ewma ${indicatorResult.indicatorId} error: ${ExceptionInfo.getExceptionInfo(ex)}")
      }

      //计算 IndicatorLimitResult 中的 alarm_level  ooc_level
      val indicatorLimitResult: IndicatorLimitResult = calculateLimit(indicatorResult, alarmRuleConfig, alarmConfigLimit)

      val ruleListBuffer = new ListBuffer[Rule]()

      val alarmRuleTypeList: Option[List[AlarmRuleType]] = Option(alarmRuleConfig.rule)

      //通过matchWaferNRequirement方法将此需求从主代码中剥离出来，避免污染原框架
      val shouldByPass = waferNRequirementImpl.shouldByPass_new(indicatorResult, this)

      if (alarmRuleTypeList.nonEmpty){  // 在原来的逻辑基础上，增加一个 !shouldByPass的判断，也就是如果控制器说不应该bypass时则走原来流程。

        for(alarmRuleType: AlarmRuleType <-alarmRuleTypeList.get){
          val ruleType: Int = alarmRuleType.ruleType

          ruleType match {
            case 1 => alarmProcessRuleParent = alarmProcessRule_1_2_6
            case 2 => alarmProcessRuleParent = alarmProcessRule_1_2_6
            case 3 => alarmProcessRuleParent = alarmProcessRule_3
            case 4 => alarmProcessRuleParent = alarmProcessRule_4_5
            case 5 => alarmProcessRuleParent = alarmProcessRule_4_5
            case 6 => alarmProcessRuleParent = alarmProcessRule_1_2_6
            case _ =>
            {
              logger.error(s"${alarmRuleType.ruleType} out of [1,6]")
              val ruleNull = parseAlarmLevelRule(indicatorResult, alarmRuleConfig, "N/N/N/N/N/N",  "N/A",
                ruleListBuffer.toList,indicatorLimitResult.alarmLevel, indicatorLimitResult.oocLevel,indicatorLimitResult.switchStatus)
              return (ruleNull, indicatorLimitResult)
            }
          }

          // 调用Alarm 告警计数接口
          val resultRuleList: Option[List[Option[Rule]]] = AlarmProcessObject.alarmProcessRule(alarmProcessRuleParent,
            indicatorResult = indicatorResult,
            alarmRuleType = alarmRuleType,
            alarmRuleLimit = alarmConfigLimit,
            ruleConfigKey = ruleKey,
            w2wType = w2wType,
            indicatorLimitResult = indicatorLimitResult,
            shouldByPass = shouldByPass)

          if (resultRuleList.nonEmpty) {

            for(rule: Option[Rule] <- resultRuleList.get){
              if(rule.nonEmpty) {
                ruleListBuffer.append(rule.get)
              }
            }
          }
        }
      }

      if (ruleListBuffer.nonEmpty) {
        var RuleTrigger1 = ""
        var RuleTrigger2 = ""
        var RuleTrigger3 = ""
        var RuleTrigger4 = ""
        var RuleTrigger5 = ""
        var RuleTrigger6 = ""

        for (rule <- ruleListBuffer) {
          val ruleType = rule.rule
          ruleType match {
            case 1 => RuleTrigger1 = rule.rule.toString
            case 2 => RuleTrigger2 = rule.rule.toString
            case 3 => RuleTrigger3 = rule.rule.toString
            case 4 => RuleTrigger4 = rule.rule.toString
            case 5 => RuleTrigger5 = rule.rule.toString
            case 6 => RuleTrigger6 = rule.rule.toString
            case _ =>
          }
        }

        val ruleTrigger = s"${ruleHasNull(RuleTrigger1)}/${ruleHasNull(RuleTrigger2)}/${ruleHasNull(RuleTrigger3)}/" +
          s"${ruleHasNull(RuleTrigger4)}/${ruleHasNull(RuleTrigger5)}/${ruleHasNull(RuleTrigger6)}"

        val alarmRuleResult: AlarmRuleResult = parseAlarmLevelRule(indicatorResult, alarmRuleConfig,
          indicatorLimitResult.limit, ruleTrigger, ruleListBuffer.toList,indicatorLimitResult.alarmLevel,
          indicatorLimitResult.oocLevel,indicatorLimitResult.switchStatus)

        (alarmRuleResult, indicatorLimitResult)

      } else {

        val ruleNull = parseAlarmLevelRule(indicatorResult, alarmRuleConfig, "N/N/N/N/N/N", "N/A",
          ruleListBuffer.toList,indicatorLimitResult.alarmLevel,indicatorLimitResult.oocLevel,indicatorLimitResult.switchStatus)

        (ruleNull, indicatorLimitResult)
      }
    }
  }

  /**
   * 处理EWMA的逻辑
   * 1- 判断是否有缓存retarget策略
   * 2- 有
   *  2-1 判断是否缓存了上一笔的数据
   *  2-2 判断rangeU 和 rangeL
   * 3- 计算newTarget 和 UCL LCL
   * 4- 判断 USL > UCL > LCL > LSL
   * 5- 计算forbidden zone 的uclLimit lclLimit
   * 6- 判断是否满足forbidden zone
   *
   * @param indicatorResult
   * @param alarmRuleConfig
   * @return
   */
  def processEwma(indicatorResult: IndicatorResult,
                  alarmRuleConfig: AlarmRuleConfig,
                  ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)]#ReadOnlyContext): AlarmRuleLimit = {

    val alarmConfigLimit: AlarmRuleLimit = alarmRuleConfig.limit
    val indicatorValue = indicatorResult.indicatorValue.split("\\|").head.toDouble
    val runId = indicatorResult.runId
    val w2wType = alarmRuleConfig.w2wType

    val toolName = indicatorResult.toolName
    val chamberName = indicatorResult.chamberName
    val recipeName = indicatorResult.recipeName
    val indicatorId: Long = indicatorResult.indicatorId
    val specId: String = alarmRuleConfig.specDefId

    // 缓存的key值
    val w2wKey = if(w2wType == "By Tool-Chamber"){
      s"${toolName}|${chamberName}|"
    }else {
      s"${toolName}|${chamberName}|${recipeName}"
    }

    val ewmaKey = s"${indicatorId.toString}|${specId}"
    val ewmaRetargetKey = s"${indicatorId.toString}|${specId}|${toolName}|${chamberName}"
    val args: EwmaArgs = alarmRuleConfig.EwmaArgs

    processEwmaAndRetarget(indicatorResult,
      ewmaKey,
      w2wKey,
      ewmaRetargetKey,
      args,
      indicatorValue,
      runId,
      alarmConfigLimit,
      ctx)
  }

  /**
   *
   * @param ewmaKey
   * @return
   */
  def clearEwmaCacheData(ewmaKey: String) = {
    if (ewmaCacheData.contains(ewmaKey)) {
      val w2wMap = ewmaCacheData(ewmaKey)
      for (elem <- w2wMap) {
        val value = elem._2
        val target = (None, value._2, value._3, "",None)
        w2wMap.put(elem._1, target)
      }
      ewmaCacheData.put(ewmaKey, w2wMap)
    }
  }

  /**
   *
   * @param ewmaRetargetKey
   * @param runId
   * @return
   */
  def clearEwmaCacheDataByToolAndChamber(ewmaRetargetKey: String,runId:String) = {
    val keyList = ewmaRetargetKey.split("\\|").toList
    val indicatorId: String = keyList(0)
    val specId = keyList(1)
    val toolName = keyList(2)
    val chamberName = keyList(3)

    val ewmaKey1 = s"${indicatorId}|${specId}"

    if (ewmaCacheDataMap.contains(ewmaKey1)) {
      val w2wMap: TrieMap[String, EwmaCacheData] = ewmaCacheDataMap(ewmaKey1)
      for (elem <- w2wMap) {
        val key2 = elem._1
        val toolName_chamberName = key2.split("\\|").toList
        if(toolName_chamberName.length >= 2){
          val cacheToolName = toolName_chamberName(0)
          val cacheChamberName = toolName_chamberName(1)
          if(toolName == cacheToolName && chamberName == cacheChamberName){
            w2wMap.remove(key2)
          }
        }else{
          logger.error(s"clearEwmaCacheDataByToolAndChamber error , ewma key2 error : ${key2};runId == ${runId}")
        }
      }
      ewmaCacheDataMap.put(ewmaKey1, w2wMap)
    }
  }


  /**
   * retarget By toolName chamberName
 *
   * @param ewmaRetargetKey
   * @param args
   * @param indicatorValue
   * @param cacheIndicatorValue
   * @param runId
   * @return
   */
  def processEwmaAndRetarget(indicatorResult: IndicatorResult,
                             ewmaKey:String,
                             w2wKey:String,
                             ewmaRetargetKey:String,
                             args: EwmaArgs,
                             indicatorValue: Double,
                             runId:String,
                             alarmConfigLimit: AlarmRuleLimit,
                             ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (String, String)]#ReadOnlyContext): AlarmRuleLimit = {

    var isRetarget = false
    var cacheRunId = ""
    var cacheUcl: Option[Double] = None
    var cacheLcl: Option[Double] = None

    var newTarget: Double = indicatorValue
    var lastTarget: Double = indicatorValue
    var cacheIndicatorValue: Option[Double] = None

//    logger.warn(s"-------------------------ewma start --------------- \n " +
//      s"ewmaKey = ${ewmaKey} ;\n  " +
//      s"w2wKey = ${w2wKey} ; ")

    if (ewmaCacheDataMap.contains(ewmaKey)) {
      val w2wMap = ewmaCacheDataMap(ewmaKey)
      if (w2wMap.contains(w2wKey)) {
        val ewmaCacheData: EwmaCacheData = w2wMap(w2wKey)
        cacheRunId = ewmaCacheData.runId
        cacheUcl = ewmaCacheData.ucl
        cacheLcl = ewmaCacheData.lcl
        cacheIndicatorValue = ewmaCacheData.lastIndicatorValue
        lastTarget = ewmaCacheData.target.getOrElse(indicatorValue)
//        logger.warn(s"ewmaCacheData == ${ewmaCacheData.toJson}")
      }
    }

    if(runId != cacheRunId){
      // retarget
      if(ewmaRetargetConfigMap.contains(ewmaRetargetKey)){
        val ewmaRetargetConfig = ewmaRetargetConfigMap(ewmaRetargetKey)
        isRetarget = true
        // 判断是否有上一笔Run 的indicator
        if(null != cacheIndicatorValue && cacheIndicatorValue.nonEmpty){

          val rangeU: Double = ewmaRetargetConfig.rangeU
          val rangeL: Double = ewmaRetargetConfig.rangeL

          val lastIndicatorValue: Double = cacheIndicatorValue.get
          val diffIndicatorValue = indicatorValue - lastIndicatorValue
          if(diffIndicatorValue >= rangeL && diffIndicatorValue <= rangeU){

//            EwmaRetargetJudgeResult(isRetarget = isRetarget,
//              retargetStatus = true,
//              errorType = null,
//              errorMessage = null,
//              cacheRunId = cacheRunId,
//              cacheTarget = Option(indicatorValue),
//              cacheUcl = cacheUcl,
//              cacheLcl = cacheUcl)

            newTarget = indicatorValue

            val ewmaLimitJudgeResult: EwmaLimitJudgeResult = getAlarmRuleLimitAndMes(args, alarmConfigLimit, newTarget,cacheUcl,cacheLcl)

            if(null != ewmaLimitJudgeResult){
              alarmConfigLimit.UCL = ewmaLimitJudgeResult.ucl
              alarmConfigLimit.LCL = ewmaLimitJudgeResult.lcl

              if(ewmaLimitJudgeResult.judgeStatus){
                // todo retarget 成功 清理该spec下对应tool + chamber 的缓存数据
                logger.warn(s"retarget successed indicatorId|specId|toolName|chamberName == ${ewmaRetargetKey} \n " +
                  s"runId == ${runId}")
                clearEwmaCacheDataByToolAndChamber(ewmaRetargetKey,runId)
              }else{

                logger.error(s"retarget getAlarmRuleLimitAndMes error judgeStatus == faluse; \n " +
                  s"ewmaLimitJudgeResult == ${ewmaLimitJudgeResult.toJson} " +
                  s"args = ${args} \n " +
                  s"alarmConfigLimit = ${alarmConfigLimit.toJson} \n " +
                  s"runId = ${runId}\n" +
                  s"lastTarget = ${lastTarget}; indicatorValue = ${indicatorValue} ;cacheIndicatorValue = ${cacheIndicatorValue} ; cacheUcl = ${cacheUcl} ; cacheLcl = ${cacheLcl} ")

                // retarget 失败 延用上一笔的target
                newTarget = lastTarget
                alarmConfigLimit.UCL = cacheUcl
                alarmConfigLimit.LCL = cacheLcl
              }
              // todo 侧道输出
              val retargetStatus = ewmaLimitJudgeResult.judgeStatus
              val errorType = ewmaLimitJudgeResult.errorType
              val errorMessage = ewmaLimitJudgeResult.errorMessage
              ewmaRetargetResultOutput(runId,indicatorResult.dataVersion,ewmaRetargetConfig,retargetStatus,errorType,errorMessage,ctx)

            }else{
              logger.error(s"retarget getAlarmRuleLimitAndMes error : 返回值为null ! \n " +
                s"args = ${args} \n " +
                s"alarmConfigLimit = ${alarmConfigLimit.toJson} \n " +
                s"runId = ${runId}\n" +
                s"lastTarget = ${lastTarget} ; indicatorValue = ${indicatorValue} ;cacheIndicatorValue = ${cacheIndicatorValue} ; cacheUcl = ${cacheUcl} ; cacheLcl = ${cacheLcl} ")

              // retarget 失败 延用上一笔的target
              newTarget = lastTarget
              alarmConfigLimit.UCL = cacheUcl
              alarmConfigLimit.LCL = cacheLcl
            }

          }else{
            // todo errorType = 2
            logger.warn(s"retarget failed 不在Range范围内 : indicatorId|specId == ${ewmaRetargetKey} ; \n " +
              s"runId == ${runId}\n " +
              s"currentIndicatorValue == ${indicatorValue} ; \n " +
              s"lastIndicatorValue == ${lastIndicatorValue} ; \n " +
              s"lastTarget == ${lastTarget}" +
              s"rangeU == ${rangeU} ; rangL == ${rangeL} ; \n " +
              s"ewmaArgs = ${args} ")

            // retarget 失败 延用上一笔的target
            newTarget = lastTarget
            alarmConfigLimit.UCL = cacheUcl
            alarmConfigLimit.LCL = cacheLcl

            // todo 侧到输出
            val retargetStatus = false
            val errorType = "PM baseline shift OOC error"
            val errorMessage = s"Indicator value=${indicatorValue};" +
              s"Pre indicator value=${cacheIndicatorValue.get} ; " +
              s"args= ${args.toJson};" +
              s"PM Range U=${rangeU};" +
              s"PM Range L=${rangeL}"
            ewmaRetargetResultOutput(runId,indicatorResult.dataVersion,ewmaRetargetConfig,retargetStatus,errorType,errorMessage,ctx)

          }
        }else{
          // todo errorType = 1
          logger.warn(s"retarget failed 没有上一笔 IndicatorValue : indicatorId|specId == ${ewmaRetargetKey} ; \n " +
            s"runId == ${runId}\n " +
            s"currentIndicatorValue == ${indicatorValue} ; \n " +
            s"cacheIndicatorValue == ${cacheIndicatorValue} ; \n " +
            s"lastTarget == ${lastTarget} ; cacheUcl == ${cacheUcl} ; cacheLcl == ${cacheLcl}")

          // retarget 失败 延用上一笔的target
          newTarget = lastTarget
          alarmConfigLimit.UCL = cacheUcl
          alarmConfigLimit.LCL = cacheLcl

          // todo 侧到输出
          val retargetStatus = false
          val errorType = "No pre data message error"
          val errorMessage = "No pre data message error"
          ewmaRetargetResultOutput(runId,indicatorResult.dataVersion,ewmaRetargetConfig,retargetStatus,errorType,errorMessage,ctx)
        }

        // 触发了retarget 就删除策略
        ewmaRetargetConfigMap.remove(ewmaRetargetKey)
      }else{

        // todo 没有 retarget 策略
        newTarget = if(lastTarget == Double.NaN){
          indicatorValue
        }else{
          val λ = args.lambdaValue
          λ * lastTarget + (1 - λ) * indicatorValue
        }

        val ewmaLimitJudgeResult: EwmaLimitJudgeResult = getAlarmRuleLimitAndMes(args, alarmConfigLimit, newTarget,cacheUcl,cacheLcl)
        if(null != ewmaLimitJudgeResult){
          alarmConfigLimit.UCL = ewmaLimitJudgeResult.ucl
          alarmConfigLimit.LCL = ewmaLimitJudgeResult.lcl
          if(!ewmaLimitJudgeResult.judgeStatus){

            logger.error(s"no retarget getAlarmRuleLimitAndMes error judgeStatus == false; \n " +
              s"ewmaLimitJudgeResult == ${ewmaLimitJudgeResult.toJson} \n" +
              s"args = ${args} \n " +
              s"alarmConfigLimit = ${alarmConfigLimit} \n " +
              s"runId = ${runId} \n" +
              s"lastTarget = ${lastTarget} ; indicatorValue = ${indicatorValue} ;cacheIndicatorValue = ${cacheIndicatorValue} ; lastTarget = ${lastTarget} ; cacheUcl = ${cacheUcl} ; cacheLcl = ${cacheLcl} ")

            // forbidden zone失败 延用上一笔的target
            newTarget = lastTarget
            alarmConfigLimit.UCL = cacheUcl
            alarmConfigLimit.LCL = cacheLcl
          }
        }else{
          logger.error(s"no retarget getAlarmRuleLimitAndMes error : 返回值为null ! \n " +
            s"args = ${args} \n " +
            s"alarmConfigLimit = ${alarmConfigLimit} \n " +
            s"runId = ${runId} \n" +
            s"lastTarget = ${lastTarget} ; indicatorValue = ${indicatorValue} ;cacheIndicatorValue = ${cacheIndicatorValue} ;cacheUcl = ${cacheUcl} ; cacheLcl = ${cacheLcl} ")

          newTarget = lastTarget
          alarmConfigLimit.UCL = cacheUcl
          alarmConfigLimit.LCL = cacheLcl
        }
      }

      // 更新缓存 and output
      updateEwmaCacheDataBeanAndOutput(w2wKey,ewmaKey,newTarget,alarmConfigLimit.UCL,alarmConfigLimit.LCL,indicatorResult,ctx)

    }else{
      logger.warn(ErrorCode("007011d011C", System.currentTimeMillis(),
        Map("indicatorResult" -> indicatorResult), "runId的indicator存在重复计算现象").toString)
    }

    alarmConfigLimit
  }


  /**
   * 判断计算出的cl有没有超限，超限则丢弃
   *
   * @param ucl
   * @param lcl
   * @param upLimit
   * @param lowLimit
   * @return
   */
  def judgeValue(ucl:Double, lcl:Double, upLimit:Double, lowLimit:Double):(Option[Double],Option[Double])={
    if(upLimit>ucl && ucl>lowLimit && upLimit>lcl && lcl>lowLimit ){
      (Option.apply(ucl),Option.apply(lcl))
    }else{
      null
    }
  }


  /**
   *
   * @param usl
   * @param ucl
   * @param lcl
   * @param lsl
   * @param upLimit
   * @param lowLimit
   * @param cache_UCL
   * @param cache_LCL
   * @return
   */
  def judgeValueMes(usl:Double,ucl:Double, lcl:Double,lsl:Double, upLimit:Double, lowLimit:Double,cache_UCL: Option[Double],cache_LCL: Option[Double]): EwmaLimitJudgeResult ={

    if(usl>ucl && ucl>lcl && lcl>lsl){
      if(upLimit>ucl && ucl>lowLimit && upLimit>lcl && lcl>lowLimit ){
        EwmaLimitJudgeResult(judgeStatus = true,
          ucl = Option.apply(ucl),
          lcl = Option.apply(lcl),
          errorType = "",
          errorMessage = "")
      }else{
        EwmaLimitJudgeResult(
          judgeStatus = false,
          ucl = cache_UCL,
          lcl = cache_LCL,
          errorType = "Control limit not in forbidden zone fail",
          errorMessage = s"ucl=${ucl};lcl=${lcl};uclLimit=${upLimit};lclLimit=${lowLimit}")
      }
    }else{
      EwmaLimitJudgeResult(judgeStatus = false,
        ucl = cache_UCL,
        lcl = cache_LCL,
        errorType = "USL>UCL>LCL>LSL rule fail",
        errorMessage = s"usl=${usl};ucl=${ucl};lcl=${lcl};lsl=${lsl}")
    }
  }

  def getAlarmRuleLimit(args:EwmaArgs, limit: AlarmRuleLimit, target:Double):AlarmRuleLimit={
    try{
      val usl=BigDecimal(limit.USL.get)
      val lsl=BigDecimal(limit.LSL.get)
      val bdTarget=BigDecimal(target)
      val bdDeltaUCL=BigDecimal(args.deltaUCL)
      val bdDeltaLCL=BigDecimal(args.deltaLCL)

      val cls = args.deltaType match {
        case "Num" =>
          val ucl = (bdTarget + bdDeltaUCL).toDouble
          val uclLimit = (usl - bdDeltaUCL*0.1).toDouble
          val lcl = (bdTarget + bdDeltaLCL).toDouble
          val lclLimit = (lsl - bdDeltaLCL*0.1).toDouble
          judgeValue(ucl,lcl,uclLimit,lclLimit)
        case "Percent" =>
          val ucl = (bdTarget * (1+bdDeltaUCL)).toDouble
          val uclLimit = (usl - target * bdDeltaUCL*0.1).toDouble
          val lcl = (bdTarget * (1+bdDeltaLCL)).toDouble
          val lclLimit = (lsl - target * bdDeltaLCL*0.1).toDouble
          judgeValue(ucl,lcl,uclLimit,lclLimit)
        case _ =>
          logger.warn(s"${this.getClass.getSimpleName}#getAlarmRuleLimit deltaType not match: ${args.deltaType}")
          null
      }
      if (cls!=null){
        return AlarmRuleLimit(
          USL=limit.USL,
          UBL=None,
          UCL=cls._1,
          LCL=cls._2,
          LBL=None,
          LSL=limit.LSL
        )
      }
    }catch {
      case e:Exception=>
        logger.error(s"${ExceptionInfo.getExceptionInfo(e)}")
    }
    null
  }

  def getAlarmRuleLimitAndMes(args:EwmaArgs, limit: AlarmRuleLimit, target:Double,cache_UCL: Option[Double],cache_LCL: Option[Double]) = {
    try{
      val usl=BigDecimal(limit.USL.get)
      val lsl=BigDecimal(limit.LSL.get)
      val bdTarget=BigDecimal(target)
      val bdDeltaUCL=BigDecimal(args.deltaUCL)
      val bdDeltaLCL=BigDecimal(args.deltaLCL)

      val ewmaLimitJudgeResult: EwmaLimitJudgeResult = args.deltaType match {
        case "Num" =>
          val ucl = (bdTarget + bdDeltaUCL).toDouble
          val uclLimit = (usl - bdDeltaUCL*0.1).toDouble
          val lcl = (bdTarget + bdDeltaLCL).toDouble
          val lclLimit = (lsl - bdDeltaLCL*0.1).toDouble
          judgeValueMes(usl.toDouble,ucl,lcl,lsl.toDouble,uclLimit,lclLimit,cache_UCL,cache_LCL)
        case "Percent" =>
          val ucl = (bdTarget * (1+bdDeltaUCL)).toDouble
          val uclLimit = (usl - target * bdDeltaUCL*0.1).toDouble
          val lcl = (bdTarget * (1+bdDeltaLCL)).toDouble
          val lclLimit = (lsl - target * bdDeltaLCL*0.1).toDouble
          judgeValueMes(usl.toDouble,ucl,lcl,lsl.toDouble,uclLimit,lclLimit,cache_UCL,cache_LCL)
        case _ =>
          logger.warn(s"${this.getClass.getSimpleName}#getAlarmRuleLimit deltaType not match: ${args.deltaType}")
          EwmaLimitJudgeResult(judgeStatus = false,
            ucl = cache_UCL,
            lcl = cache_LCL,
            errorType = "deltaType error",
            errorMessage = s"deltaType error")
      }
      ewmaLimitJudgeResult
    }catch {
      case e:Exception=>
        logger.error(s"getAlarmRuleLimitAndMes try catch Execption: ${ExceptionInfo.getExceptionInfo(e)}")

        EwmaLimitJudgeResult(judgeStatus = false,
          ucl = cache_UCL,
          lcl = cache_LCL,
          errorType = "异常 execption",
          errorMessage = s"异常 execption")
    }

  }


  /**
   * 没有匹配到OOC,OCAP配置,返回indicator值入库 alarmLevel = -1
   *
   * @return
   */
  def EmptyOOCAndOCAP(indicatorResult: IndicatorResult, alarmLevel: Int): (AlarmRuleResult, IndicatorLimitResult) = {
    val ruleNull: AlarmRuleResult = GetEmptyRule(indicatorResult,alarmLevel, 0,"N/N/N/N/N/N","ON")
    (ruleNull, getIndicatorLimitResult(indicatorResult, alarmLevel, 0, "N/N/N/N/N/N", "ON"))
  }

  /**
   * get空的rule
   * @param indicatorResult
   * @return
   */
  def GetEmptyRule(indicatorResult: IndicatorResult,
                   alarmLevel: Int,
                   oocLevel: Int,
                   limit: String,
                   switchStatus: String
                  ): AlarmRuleResult = {
    AlarmRuleResult(
      indicatorResult.controlPlanVersion.toInt,
      indicatorResult.chamberName,
      indicatorResult.chamberId,
      indicatorResult.indicatorCreateTime,
      System.currentTimeMillis(),
      indicatorResult.indicatorId,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.toolId,
      limit,
      "N/A",
      indicatorResult.indicatorValue,
      indicatorResult.indicatorName,
      if(indicatorResult.algoClass == null) "" else indicatorResult.algoClass,
      indicatorResult.controlPlanId,
      indicatorResult.controlPlanName,
      -1,
      indicatorResult.configMissingRatio,
      indicatorResult.runStartTime,
      indicatorResult.runEndTime,
      indicatorResult.windowStartTime,
      indicatorResult.windowEndTime,
      indicatorResult.windowDataCreateTime,
      indicatorResult.locationId,
      indicatorResult.locationName,
      indicatorResult.moduleId,
      indicatorResult.moduleName,
      indicatorResult.toolGroupId,
      indicatorResult.toolGroupName,
      indicatorResult.chamberGroupId,
      indicatorResult.chamberGroupName,
      indicatorResult.recipeGroupName,
      indicatorResult.recipeName,
      indicatorResult.recipeId,
      indicatorResult.product,
      indicatorResult.stage,
      indicatorResult.materialName,
      indicatorResult.pmStatus,
      indicatorResult.pmTimestamp,
      indicatorResult.area,
      indicatorResult.section,
      indicatorResult.mesChamberName,
      indicatorResult.lotMESInfo,
      Nil,
      switchStatus = switchStatus,
      unit = if(indicatorResult.unit == null)""else indicatorResult.unit,
      alarmLevel = alarmLevel,
      oocLevel = oocLevel,
      dataVersion = indicatorResult.dataVersion,
      configVersion = ProjectConfig.JOB_VERSION,
      cycleIndex = indicatorResult.cycleIndex,
      specId = "0",
      limitConditionName = "",
      indicatorType = "",
      isEwma = false)
  }

  /**
   * 计算 limit 计算 alarm_level
   * @param indicatorResult
   * @param alarmConfig
   * @return
   */
  def calculateLimit(indicatorResult: IndicatorResult, alarmConfig: AlarmRuleConfig, alarmConfigLimit: AlarmRuleLimit): IndicatorLimitResult = {

    val limit = alarmConfigLimit

    var USL: Option[Double] = None
    var UBL: Option[Double] = None
    var UCL: Option[Double] = None

    var LCL: Option[Double] = None
    var LBL: Option[Double] = None
    var LSL: Option[Double] = None

    if (null != limit){
      USL= limit.USL
      UBL= limit.UBL
      UCL= limit.UCL
      LCL= limit.LCL
      LBL= limit.LBL
      LSL= limit.LSL
    }


    try {

      // 当时cycle window 的时候一个RunData 会有多个indicatorValue
      val valueList = indicatorResult.indicatorValue.split("\\|").map(_.toDouble)
      val alarmLevelList: ListBuffer[Int] = ListBuffer()

      valueList.foreach(value => {

        val alarmLevel = if (USL.nonEmpty && value > USL.get) {
          3
        } else if (LSL.nonEmpty && value < LSL.get) {
          -3
        } else if (UBL.nonEmpty && value > UBL.get) {
          2
        } else if (LBL.nonEmpty && value < LBL.get) {
          -2
        } else if (UCL.nonEmpty && value > UCL.get) {
          1
        } else if (LCL.nonEmpty && value < LCL.get) {
          -1
        }else{
          0
        }

        if (alarmLevel != 0) alarmLevelList.append(alarmLevel)
      })

      val oocLevel=if (alarmLevelList.nonEmpty) {
        val max = alarmLevelList.max
        val min = alarmLevelList.min
        if(max==min){
          max
        }else if(max.abs >= min.abs){
          max
        }else{
          min
        }
      }else{
        0
      }

      val LimitSUB = s"${LCL.getOrElse("")}/${UCL.getOrElse("")}/${LBL.getOrElse("")}/${UBL.getOrElse("")}/${LSL.getOrElse("")}/${USL.getOrElse("")}"
      getIndicatorLimitResult(indicatorResult, oocLevel, oocLevel, LimitSUB, "ON")

    } catch {
      case ex: NumberFormatException =>
        logger.warn(s"alarm job limit data Format err ${ExceptionInfo.getExceptionInfo(ex)} IndicatorRuleConfig:$alarmConfig indicatorResult:$indicatorResult ")

        getIndicatorLimitResult(indicatorResult, -7, 0, "N/N/N/N/N/N", "ON")
      case ex: NullPointerException => logger.warn(s"alarm job limit data null err：${ExceptionInfo.getExceptionInfo(ex)} IndicatorRuleConfig :$alarmConfig indicatorResult:$indicatorResult ")
        getIndicatorLimitResult(indicatorResult, -7, 0, "N/N/N/N/N/N", "ON")

      case ex: Exception => logger.warn(s"alarm job limit data  err：${ExceptionInfo.getExceptionInfo(ex)} IndicatorRuleConfig :$alarmConfig indicatorResult:$indicatorResult ")
        getIndicatorLimitResult(indicatorResult, -7, 0, "N/N/N/N/N/N", "ON")
    }
  }


  /**
   * 获取key
   *
   * @return
   */
  def geneAlarmKey(ags: String*): String = {
    ags.reduceLeft((a, b) => s"$a|$b")
  }



  /**
   * 字符串为空
   */
  def ruleHasNull(str: String): String = {
    if (str == null || str == "" || str.length == 0 || str == "#") {
      "N"
    } else {
      str
    }
  }

  /**
   * 获取匹配后的IndicatorResult
   *
   * @param alarmLevel 线的level
   * @param limit      各种线
   * @return
   */
  def getIndicatorLimitResult(indicatorResult: IndicatorResult,
                              alarmLevel: Int,
                              oocLevel: Int,
                              limit: String, switchStatus: String): IndicatorLimitResult = {
    IndicatorLimitResult(indicatorResult.runId
      , indicatorResult.toolName
      , indicatorResult.chamberName
      , indicatorResult.indicatorValue
      , indicatorResult.indicatorId.toString
      , indicatorResult.indicatorName
      , alarmLevel
      , oocLevel
      , indicatorResult.runEndTime.toString
      , limit
      , switchStatus
      , if(indicatorResult.unit == null) "" else indicatorResult.unit)
  }


  /**
   * 组装 AlarmRuleResult
   * @param indicatorResult
   * @param alarmConfig
   * @param limit
   * @param ruleTrigger
   * @param ruleList
   * @param alarmLevel
   * @param oocLevel
   * @param switchStatus
   * @return
   */
  def parseAlarmLevelRule(indicatorResult: IndicatorResult,
                          alarmConfig: AlarmRuleConfig,
                          limit: String,
                          ruleTrigger: String,
                          ruleList: List[Rule],
                          alarmLevel:Int,
                          oocLevel:Int,
                          switchStatus:String): AlarmRuleResult ={
    AlarmRuleResult(
      indicatorResult.controlPlanVersion.toInt,
      indicatorResult.chamberName,
      indicatorResult.chamberId,
      indicatorResult.indicatorCreateTime,
      //alarm创建的时间应该是当前时间
      System.currentTimeMillis(),
      indicatorResult.indicatorId,
      indicatorResult.runId,
      indicatorResult.toolName,
      indicatorResult.toolId,
      limit,
      ruleTrigger,
      indicatorResult.indicatorValue,
      indicatorResult.indicatorName,
      if(indicatorResult.algoClass == null) "" else indicatorResult.algoClass,
      indicatorResult.controlPlanId,
      indicatorResult.controlPlanName,
      indicatorResult.missingRatio,
      indicatorResult.configMissingRatio,
      indicatorResult.runStartTime,
      indicatorResult.runEndTime,
      indicatorResult.windowStartTime,
      indicatorResult.windowEndTime,
      indicatorResult.windowDataCreateTime,
      indicatorResult.locationId,
      indicatorResult.locationName,
      indicatorResult.moduleId,
      indicatorResult.moduleName,
      indicatorResult.toolGroupId,
      indicatorResult.toolGroupName,
      indicatorResult.chamberGroupId,
      indicatorResult.chamberGroupName,
      indicatorResult.recipeGroupName,
      indicatorResult.recipeName,
      indicatorResult.recipeId,
      indicatorResult.product,
      indicatorResult.stage,
      indicatorResult.materialName,
      indicatorResult.pmStatus,
      indicatorResult.pmTimestamp,
      indicatorResult.area,
      indicatorResult.section,
      indicatorResult.mesChamberName,
      indicatorResult.lotMESInfo,
      ruleList,
      switchStatus = switchStatus,
      unit = if(indicatorResult.unit == null) "" else indicatorResult.unit ,
      alarmLevel = alarmLevel,
      oocLevel = oocLevel,
      dataVersion = indicatorResult.dataVersion,
      configVersion = ProjectConfig.JOB_VERSION,
      cycleIndex = indicatorResult.cycleIndex,
      specId = alarmConfig.specDefId,
      limitConditionName = alarmConfig.limitConditionName,
      indicatorType = alarmConfig.indicatorType,
      isEwma = alarmConfig.isEwma
    )
  }


  /**
   * 初始化 ewmaCacheDataMap
   */
  def initAlarmEwmaCacheMap() = {
    try {
//      val redisAlarmEwmaCacheList = RedisUtil.alarmEwmaCacheList
      val redisAlarmEwmaCacheList = getStringDataByPrefixKey[AlarmEwmaCacheData]("alarmEwmaCache")
      logger.warn(s"read redis Finish: size = ${redisAlarmEwmaCacheList.size}")

      redisAlarmEwmaCacheList.foreach(redisCache => {
        val alarmEwmaCacheData: AlarmEwmaCacheData = redisCache.datas
        val ewmaKey = alarmEwmaCacheData.ewmaKey
        val w2wKey = alarmEwmaCacheData.w2wKey
        val ewmaCacheDataBean = EwmaCacheData(target = alarmEwmaCacheData.target,
          ucl = alarmEwmaCacheData.ucl,
          lcl = alarmEwmaCacheData.lcl,
          runId = alarmEwmaCacheData.runId,
          lastIndicatorValue = alarmEwmaCacheData.lastIndicatorValue)

        if (ewmaCacheDataMap.contains(ewmaKey)) {
          val ewmaCacheDataW2wMap: TrieMap[String, EwmaCacheData] = ewmaCacheDataMap(ewmaKey)
          ewmaCacheDataW2wMap.put(w2wKey,ewmaCacheDataBean)
          ewmaCacheDataMap.put(ewmaKey, ewmaCacheDataW2wMap)
        } else {
          val ewmaCacheDataW2wMap = concurrent.TrieMap[String, EwmaCacheData](w2wKey -> ewmaCacheDataBean)
          ewmaCacheDataMap.put(ewmaKey, ewmaCacheDataW2wMap)
        }
      })

      logger.warn(s"initAlarmEwmaCacheMap Finish: size = ${ewmaCacheDataMap.size}")
    }catch {
      case exception: Exception => logger.warn(ErrorCode("007007d001C", System.currentTimeMillis(),
        Map("function" -> "initAlarmEwmaCacheMap","msg" -> "redis读取失败!!"), exception.toString).toString)
    }
  }
  /**
   * 初始化 ewmaCacheData
   */
  def initAlarmEwmaCache(): Unit = {
    try {
      val redisAlarmEwmaCacheList = RedisUtil.alarmEwmaCacheList
      redisAlarmEwmaCacheList.foreach(redisCache => {
        val alarmEwmaCacheData: AlarmEwmaCacheData = redisCache.datas
        val ewmaKey = alarmEwmaCacheData.ewmaKey
        val w2wKey = alarmEwmaCacheData.w2wKey
        val ucl: Option[Double] = alarmEwmaCacheData.ucl
        val lcl: Option[Double] = alarmEwmaCacheData.lcl
        val target: Option[Double] = alarmEwmaCacheData.target
        if (ewmaCacheData.contains(ewmaKey)) {
          val alarmRuleMap = ewmaCacheData(ewmaKey)
          alarmRuleMap.put(w2wKey, (target, ucl, lcl, "",None))
          ewmaCacheData.put(ewmaKey, alarmRuleMap)
        } else {
          val alarmRuleMapScala = concurrent.TrieMap[String, (Option[Double], Option[Double], Option[Double], String,Option[Double])](w2wKey -> (target, ucl, lcl, "",None))
          ewmaCacheData.put(ewmaKey, alarmRuleMapScala)
        }
      })

      logger.warn(s"initAlarmEwmaCache Finish: size = ${redisAlarmEwmaCacheList.size}")
    }catch {
      case exception: Exception => logger.warn(ErrorCode("007007d001C", System.currentTimeMillis(),
        Map("function" -> "initAlarmEwmaCache","msg" -> "redis读取失败!!"), exception.toString).toString)
    }

  }

  /**
   * 打印所有的 AlarmConfig 信息
   */
  def showAllAlarmConfig() = {
    logger.warn(s" indicatorRuleByStage == ${indicatorRuleByStage.toJson}")
    logger.warn(s" indicatorRuleByProduct == ${indicatorRuleByProduct.toJson}")
    logger.warn(s" indicatorRuleByRecipe == ${indicatorRuleByRecipe.toJson}")
    logger.warn(s" indicatorRuleByChamberId == ${indicatorRuleByChamberId.toJson}")
    logger.warn(s" indicatorRuleByTool == ${indicatorRuleByTool.toJson}")
    logger.warn(s" indicatorRuleByIndicatorId == ${indicatorRuleByIndicatorId.toJson}")
  }


  /**
   * 新配置 重置 统计计数策略
   * @param key
   * @param oldConfig
   * @param newConfig
   */
  def judgeResetAllAlarmCount(key: String, oldConfig: AlarmRuleConfig, newConfig: AlarmRuleConfig) = {

    if(ProjectConfig.NEW_ALARM_CONFIG_RESET_ALL_ALARM_COUNT){
      //TODO 用户要求升版 不清理状态 做了对比，如果配置相同不清理，不相同，就清理
      val is_clean = compareIsClean(oldConfig, newConfig)
      if (is_clean) {
        //如果配置变了，要更新计数状态
        alarmProcessRule_1_2_6.removeRuleData(key)
        alarmProcessRule_3.removeRuleData(key)
        alarmProcessRule_4_5.removeRuleData(key)
      } else {
        logger.warn(s"not removeRuleData is_clean == ${is_clean}; key == ${key}")
      }
    }
  }

}

