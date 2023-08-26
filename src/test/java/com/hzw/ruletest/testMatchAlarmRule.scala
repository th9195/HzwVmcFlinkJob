//package com.hzw.ruletest
//
//import com.hzw.fdc.controller.wafern.WaferNByPassController
//import com.hzw.fdc.function.online.MainFabAlarm.AlarmSwitchEventConfig
//import com.hzw.fdc.json.MarshallableImplicits.Marshallable
//import com.hzw.fdc.scalabean._
//import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants}
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.mutable.ListBuffer
//import scala.collection.{concurrent, mutable}
//
///**
// * @author ：gdj
// * @date ：Created in 2021/11/5 15:29
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//class testMatchAlarmRule {
//
//  private val logger: Logger = LoggerFactory.getLogger(classOf[testMatchAlarmRule])
//
//  //业务逻辑从上到下逐级匹配
//  // key为toolid + chamberid + indicatorid +recipe+product + stage 6个
//  var indicatorRuleByStage = new mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]
//  // key为toolid + chamberid + indicatorid +recipe+product 5个
//  var indicatorRuleByProduct = new mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]
//  // key为toolid + chamberid + indicatorid +recipe 4个
//  var indicatorRuleByRecipe = new mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]
//  // key为toolid + chamberid + indicatorid  3个
//  var indicatorRuleByChamberId = new mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]
//  // key为toolid + indicatorid  2个
//  var indicatorRuleByTool = new mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]
//  // key为indicatorid  1个
//  var indicatorRuleByIndicatorId = new mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]
//
//  //外面的key为indicatorRule配置的 Key 里面的key为(toolid + chamberid + indicatorid+ruletype) or
//  // (toolid + chamberid + recipe + indicatorid+ruletype) 用于type4和5
//  var increaseContinue = new concurrent.TrieMap[String, concurrent.TrieMap[String, (CountData,Long)]]
//
//  //外面的key为indicatorRule配置的 Key 里面的key为(toolid + chamberid + indicatorid+ruletype) or
//  // (toolid + chamberid + recipe + indicatorid+ruletype) 用于type1和2和6 7
//  var increaseUpLow = new concurrent.TrieMap[String, concurrent.TrieMap[String, UpLowData]]
//
//  //外面的key为indicatorRule配置的 Key 里面的key为(toolid + chamberid + indicatorid+ruletype) or
//  // (toolid + chamberid + recipe + indicatorid+ruletype) 用于type3
//  var increaseQueue = new concurrent.TrieMap[String, concurrent.TrieMap[String, mutable.Queue[Double]]]
//
//  // 控制alarm开关的配置 key: toolid + chamberid
//  val switchEventConfig = new concurrent.TrieMap[String, AlarmSwitchEventConfig]
//
//  // wafer N 需求的 状态对象
//  val waferNRequirementImpl = new WaferNByPassController()
//
//
//  /**
//   * 匹配 rule 配置
//   * @param indicatorResult
//   * @return
//   */
//  def matchAlarmRule(indicatorResult: IndicatorResult): (AlarmRuleResult, IndicatorLimitResult) = {
//    try {
//      //逐层拿key
//      val version = indicatorResult.controlPlanVersion.toString
//      val indicatorIdKey = indicatorResult.indicatorId.toString
//      val toolKey = geneAlarmKey(indicatorIdKey, indicatorResult.toolName)
//      val chamberKey = geneAlarmKey(toolKey, indicatorResult.chamberName)
//      val recipeKey = geneAlarmKey(chamberKey, indicatorResult.recipeName)
//
//      val resultList = new ListBuffer[(AlarmRuleResult, IndicatorLimitResult) ]
//
//      // 特殊处理 stage
//      if (resultList.isEmpty) {
//        for (productName <- indicatorResult.product) {
//          for (stageName <- indicatorResult.stage) {
//            val productKey = geneAlarmKey(recipeKey, productName)
//            val stageKey = geneAlarmKey(productKey, stageName)
//
//            if (this.indicatorRuleByStage.contains(stageKey)) {
//              //匹配Stage
//              val indicatorRuleByStageVersion = indicatorRuleByStage(stageKey)
//              //匹配版本
//              if (indicatorRuleByStageVersion.contains(version)) {
//                val indicatorRule = indicatorRuleByStageVersion(version)
//                return calculateRule(indicatorRule, indicatorResult, stageKey)
//              } else {
//                logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator version：$version in $indicatorRuleByStageVersion")
//                return EmptyOOCAndOCAP(indicatorResult)
//              }
//            }
//          }
//        }
//      }
//
//      // 特殊处理product
//      if (resultList.isEmpty) {
//        for (productName <- indicatorResult.product) {
//          val productKey = geneAlarmKey(recipeKey, productName)
//          if (this.indicatorRuleByProduct.contains(productKey)) {
//            //匹配Stage
//            val indicatorRuleByProductVersion = indicatorRuleByProduct(productKey)
//            //匹配版本
//            if (indicatorRuleByProductVersion.contains(version)) {
//              val indicatorRule = indicatorRuleByProductVersion(version)
//              return calculateRule(indicatorRule, indicatorResult, productKey)
//            } else {
//              logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator version：$version in $indicatorRuleByProductVersion")
//              return EmptyOOCAndOCAP(indicatorResult)
//            }
//          }
//        }
//      }
//
//      if (this.indicatorRuleByRecipe.contains(recipeKey)) {
//        //匹配Recipe
//        val indicatorRuleByRecipeVersion = indicatorRuleByRecipe(recipeKey)
//        //匹配版本
//        if (indicatorRuleByRecipeVersion.contains(version)) {
//          val indicatorRule = indicatorRuleByRecipeVersion(version)
//          calculateRule(indicatorRule, indicatorResult, recipeKey)
//        } else {
//          logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator version：$version in $indicatorRuleByRecipeVersion")
//          EmptyOOCAndOCAP(indicatorResult)
//        }
//
//      } else if (this.indicatorRuleByChamberId.contains(chamberKey)) {
//        //匹配Chamber
//        val indicatorRuleByChamberIdVersion = indicatorRuleByChamberId(chamberKey)
//        //匹配版本
//        if (indicatorRuleByChamberIdVersion.contains(version)) {
//          val indicatorRule = indicatorRuleByChamberIdVersion(version)
//          calculateRule(indicatorRule, indicatorResult, chamberKey)
//        } else {
//          logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator version：$version in $indicatorRuleByChamberIdVersion")
//          EmptyOOCAndOCAP(indicatorResult)
//        }
//
//      } else if (this.indicatorRuleByTool.contains(toolKey)) {
//        //匹配Tool
//        val indicatorRuleByToolVersion = indicatorRuleByTool(toolKey)
//        //匹配版本
//        if (indicatorRuleByToolVersion.contains(version)) {
//          val indicatorRule = indicatorRuleByToolVersion(version)
//          calculateRule(indicatorRule, indicatorResult, toolKey)
//        } else {
//          logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator version：$version in $indicatorRuleByToolVersion")
//          EmptyOOCAndOCAP(indicatorResult)
//        }
//
//      } else if (this.indicatorRuleByIndicatorId.contains(indicatorIdKey)) {
//        //匹配Tool
//        val indicatorRuleByIndicatorIdVersion = indicatorRuleByIndicatorId(indicatorIdKey)
//        //匹配版本
//        if (indicatorRuleByIndicatorIdVersion.contains(version)) {
//          val indicatorRule = indicatorRuleByIndicatorIdVersion(version)
//          calculateRule(indicatorRule, indicatorResult, indicatorIdKey)
//        } else {
//          logger.warn(s"alarm job EmptyOOCAndOCAP unmatch indicator version：$version in $indicatorRuleByIndicatorIdVersion")
//          EmptyOOCAndOCAP(indicatorResult)
//        }
//
//      } else {
//        //没匹配到配置
//        EmptyOOCAndOCAP(indicatorResult)
//      }
//    } catch {
//      case ex: NullPointerException => logger.warn(s"alarm job matchAlarmRule NullPointerException: ${ExceptionInfo.getExceptionInfo(ex)} data: $indicatorResult")
//        val ruleNull: AlarmRuleResult = GetEmptyRule(indicatorResult,-7, 0,"N/N/N/N/N/N","ON")
//        val IndicatorResult = IndicatorLimitResult("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", -7, 0, "N/A", "N/A", "N/A", "")
//        (ruleNull, IndicatorResult)
//      case ex: Exception => logger.warn(s"alarm job matchAlarmRule data Exception${ExceptionInfo.getExceptionInfo(ex)} data: $indicatorResult ")
//        val ruleNull: AlarmRuleResult = GetEmptyRule(indicatorResult,-7, 0,"N/N/N/N/N/N","ON")
//        val IndicatorResult = IndicatorLimitResult("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", -7, 0, "N/A", "N/A", "N/A", "")
//        (ruleNull, IndicatorResult)
//    }
//  }
//
//
//  /**
//   * 处理从kafka topic新增的Rule配置数据
//   */
//  def updateRuleConfig(record: ConfigData[AlarmRuleConfig]): Unit = {
//    try {
//
//      //RULE配置
//      val alarmRuleConfig = record.datas
//
//      //只有tool、chamber、recipe、product、indicatorId能决定全局唯一配置
//      if (record.status) {
//
//        addAlarmRuleConfig(alarmRuleConfig)
//
//      } else if (!record.status) {
//        if (alarmRuleConfig.stage.nonEmpty) {
//          if (alarmRuleConfig.productName.nonEmpty && alarmRuleConfig.recipeName.nonEmpty &&
//            alarmRuleConfig.chamberName.nonEmpty && alarmRuleConfig.toolName.nonEmpty) {
//            val key = geneAlarmKey(alarmRuleConfig.indicatorId,
//              alarmRuleConfig.toolName.get,
//              alarmRuleConfig.chamberName.get,
//              alarmRuleConfig.recipeName.get,
//              alarmRuleConfig.productName.get,
//              alarmRuleConfig.stage.get)
//            this.indicatorRuleByProduct.remove(key)
//            //删除rule状态
//            removeRuleData(key)
//          } else {
//            logger.warn(s"alarm rule config by stage error : alarmRuleConfig:$alarmRuleConfig")
//          }
//        } else if (alarmRuleConfig.productName.nonEmpty) {
//          if (alarmRuleConfig.recipeName.nonEmpty && alarmRuleConfig.chamberName.nonEmpty &&
//            alarmRuleConfig.toolName.nonEmpty) {
//            val key = geneAlarmKey(alarmRuleConfig.indicatorId,
//              alarmRuleConfig.toolName.get,
//              alarmRuleConfig.chamberName.get,
//              alarmRuleConfig.recipeName.get,
//              alarmRuleConfig.productName.get)
//            this.indicatorRuleByProduct.remove(key)
//            //删除rule状态
//            removeRuleData(key)
//          } else {
//            logger.warn(s"alarm rule config by PRODUCT_ID error : alarmRuleConfig:$alarmRuleConfig")
//          }
//        } else if (alarmRuleConfig.recipeName.nonEmpty) {
//          if (alarmRuleConfig.chamberName.nonEmpty && alarmRuleConfig.toolName.nonEmpty) {
//            val key = geneAlarmKey(alarmRuleConfig.indicatorId,
//              alarmRuleConfig.toolName.get,
//              alarmRuleConfig.chamberName.get,
//              alarmRuleConfig.recipeName.get)
//            this.indicatorRuleByRecipe.remove(key)
//            //删除rule状态
//            removeRuleData(key)
//          } else {
//            logger.warn(s"alarm rule config by Recipe error : alarmRuleConfig:$alarmRuleConfig")
//          }
//        } else if (alarmRuleConfig.chamberName.nonEmpty) {
//          if (alarmRuleConfig.toolName.nonEmpty) {
//            val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.toolName.get, alarmRuleConfig.chamberName.get)
//            this.indicatorRuleByChamberId.remove(key)
//            //删除rule状态
//            removeRuleData(key)
//          } else {
//            logger.warn(s"alarm rule config by Chamber error : alarmRuleConfig:$alarmRuleConfig")
//          }
//        } else if (alarmRuleConfig.toolName.nonEmpty) {
//          val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.toolName.get)
//          this.indicatorRuleByTool.remove(key)
//          //删除rule状态
//          removeRuleData(key)
//        } else {
//          val key = alarmRuleConfig.indicatorId
//          this.indicatorRuleByIndicatorId.remove(key)
//          //删除rule状态
//          removeRuleData(key)
//        }
//
//
//      } else {
//        logger.warn(s"alarm job indicatorRule match fail data: $record ")
//      }
//
//
//
//
//    } catch {
//      case ex: Exception => logger.warn(s"alarm job indicatorRule Exception${ExceptionInfo.getExceptionInfo(ex)} data: $record ")
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
//    if (alarmRuleConfig.stage.nonEmpty) {
//      if (alarmRuleConfig.stage.nonEmpty && alarmRuleConfig.recipeName.nonEmpty && alarmRuleConfig.chamberName.nonEmpty&& alarmRuleConfig.toolName.nonEmpty) {
//        val key = geneAlarmKey(alarmRuleConfig.indicatorId,
//          alarmRuleConfig.toolName.get,
//          alarmRuleConfig.chamberName.get,
//          alarmRuleConfig.recipeName.get,
//          alarmRuleConfig.productName.get,
//          alarmRuleConfig.stage.get)
//        //删除rule状态
//        removeRuleData(key)
//
//        this.indicatorRuleByStage = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByStage)
//
//      } else {
//        logger.warn(s"alarm rule config by PRODUCT_ID error : alarmRuleConfig:$alarmRuleConfig")
//      }
//    } else if (alarmRuleConfig.productName.nonEmpty) {
//      if (alarmRuleConfig.recipeName.nonEmpty && alarmRuleConfig.chamberName.nonEmpty && alarmRuleConfig.toolName.nonEmpty) {
//        val key = geneAlarmKey(alarmRuleConfig.indicatorId,
//          alarmRuleConfig.toolName.get,
//          alarmRuleConfig.chamberName.get,
//          alarmRuleConfig.recipeName.get,
//          alarmRuleConfig.productName.get)
//        //删除rule状态
//        removeRuleData(key)
//
//        this.indicatorRuleByProduct = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByProduct)
//      } else {
//        logger.warn(s"alarm rule config by PRODUCT_ID error : alarmRuleConfig:$alarmRuleConfig")
//      }
//    } else if (alarmRuleConfig.recipeName.nonEmpty) {
//      if (alarmRuleConfig.chamberName.nonEmpty && alarmRuleConfig.toolName.nonEmpty) {
//        val key = geneAlarmKey(
//          alarmRuleConfig.indicatorId,
//          alarmRuleConfig.toolName.get,
//          alarmRuleConfig.chamberName.get,
//          alarmRuleConfig.recipeName.get)
//        //删除rule状态
//        removeRuleData(key)
//        this.indicatorRuleByRecipe = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByRecipe)
//      } else {
//        logger.warn(s"alarm rule config by Recipe error : alarmRuleConfig:$alarmRuleConfig")
//      }
//    } else if (alarmRuleConfig.chamberName.nonEmpty) {
//      if (alarmRuleConfig.toolName.nonEmpty) {
//        val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.toolName.get, alarmRuleConfig.chamberName.get)
//        //删除rule状态
//        removeRuleData(key)
//
//        this.indicatorRuleByChamberId = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByChamberId)
//
//      } else {
//        logger.warn(s"alarm rule config by Chamber error : alarmRuleConfig:$alarmRuleConfig")
//      }
//    } else if (alarmRuleConfig.toolName.nonEmpty) {
//      val key = geneAlarmKey(alarmRuleConfig.indicatorId, alarmRuleConfig.toolName.get)
//      //删除rule状态
//      removeRuleData(key)
//
//      this.indicatorRuleByTool = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByTool)
//    } else {
//      val key = alarmRuleConfig.indicatorId
//      //删除rule状态
//      removeRuleData(key)
//
//      this.indicatorRuleByIndicatorId = updateConfigData(alarmRuleConfig, version, key, indicatorRuleByIndicatorId)
//    }
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
//  def updateConfigData(alarmRuleConfig: AlarmRuleConfig, version: String, key: String, indicatorRule: mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]]):
//  mutable.HashMap[String, mutable.HashMap[String, AlarmRuleConfig]] = {
//    try {
//      // 新增数据
//      if (indicatorRule.contains(key)) {
//        val alarmRuleMap = indicatorRule(key)
//        alarmRuleMap += (version -> alarmRuleConfig)
//        indicatorRule.put(key, alarmRuleMap)
//        logger.warn(s"add upData alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//      } else {
//        val versionMap = mutable.HashMap[String, AlarmRuleConfig](version -> alarmRuleConfig)
//
//        indicatorRule.put(key, versionMap)
//        logger.warn(s"add new alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//      }
//
//      // 更新版本, 同一个indicator只保留两个版本
//      val versionAndAlarmRuleConfig = indicatorRule(key)
//      val k = versionAndAlarmRuleConfig.keys.map(_.toLong)
//      if (k.size > 2) {
//        val s = k.min
//        versionAndAlarmRuleConfig.remove(s.toString)
//        indicatorRule.put(key, versionAndAlarmRuleConfig)
//        logger.warn(s"add upData version alarm config key：$key  alarmRuleConfig :${alarmRuleConfig.toJson}")
//      }
//    } catch {
//      case ex: Exception => logger.warn(s"alarm job updateData Exception: ${ExceptionInfo.getExceptionInfo(ex)}")
//    }
//    indicatorRule
//  }
//
//
//  /**
//   * 删除rule状态
//   *
//   */
//  def removeRuleData(key: String): Unit = {
//
//    //TODO 用户要求升版 不清理状态 可能有BUG phase4 需要仔细捋一下
//    //    this.increaseContinue.remove(key)
//    //    this.increaseUpLow.remove(key)
//    //    this.increaseQueue.remove(key)
//  }
//
//
//  /**
//   * 计算rule
//   *
//   * @param alarmConfig
//   * @param indicatorResult
//   * @param ruleKey
//   * @return
//   */
//  def calculateRule(alarmConfig: AlarmRuleConfig, indicatorResult: IndicatorResult, ruleKey: String): (AlarmRuleResult, IndicatorLimitResult) = {
//    val configMissingRatio = indicatorResult.configMissingRatio
//    val nowMissingRatio = indicatorResult.missingRatio
//    val w2wType = alarmConfig.w2wType
//    val switchKey = s"${indicatorResult.toolName}|${indicatorResult.chamberName}"
//
//    if (nowMissingRatio >= configMissingRatio) {
//      //缺点率大于预设值 不算
//      val ruleNull = parseAlarmLevelRule(indicatorResult, alarmConfig, "N/A", "N/A", Nil,-5,0,"ON")
//
//      (ruleNull, getIndicatorLimitResult(indicatorResult, -5, 0, "N/N/N/N/N/N", "ON"))
//
//    } else if (switchEventConfig.contains(switchKey) && switchEventConfig(switchKey).action.equals("OFF") ) {
//
//      //卡控alarm 不算
//      val ruleNull = parseAlarmLevelRule(indicatorResult, alarmConfig, "N/A", "N/A", Nil,-6,0,"ON")
//
//      (ruleNull, getIndicatorLimitResult(indicatorResult, -6, 0, "N/N/N/N/N/N", "OFF"))
//    } else {
//
//
//      //匹配limit
//      val limitResult = calculateLimit(indicatorResult, alarmConfig)
//
//
//      val ruleListBuffer = new ListBuffer[Rule]()
//
//      val ruleConfigList = Option(alarmConfig.rule)
//
//      //通过matchWaferNRequirement方法将此需求从主代码中剥离出来，避免污染原框架
//
//      //测试
//      val shouldByPass = true
//
//
//      if (ruleConfigList.nonEmpty){  // 在原来的逻辑基础上，增加一个 !shouldByPass的判断，也就是如果控制器说不应该bypass时则走原来流程。
//
//        for(ruleConfig<-ruleConfigList.get){
//
//          ruleConfig.ruleType match {
//            case 1 =>{
//
//              val level1 =  processRule(indicatorResult = indicatorResult,
//                rule = ruleConfig,
//                limit = alarmConfig.limit,
//                ruleConfigKey = ruleKey,
//                w2wType = w2wType,
//                limitResult = limitResult,
//                shouldByPass = shouldByPass
//              )
//              if (level1.nonEmpty) {
//                ruleListBuffer.append(level1.get)
//              }
//            }
//            case 2 =>{
//              val level2 = processRule(indicatorResult = indicatorResult,
//                rule = ruleConfig,
//                limit = alarmConfig.limit,
//                ruleConfigKey = ruleKey,
//                w2wType = w2wType,
//                limitResult = limitResult,
//                shouldByPass = shouldByPass
//              )
//              if (level2.nonEmpty) {
//                ruleListBuffer.append(level2.get)
//              }
//            }
//            case 3 =>{
//              val level3 = processRule3(indicatorResult = indicatorResult,
//                rule = ruleConfig,
//                limit = alarmConfig.limit,
//                ruleConfigKey = ruleKey,
//                w2wType = w2wType,
//                limitResult = limitResult,
//                shouldByPass = shouldByPass)
//              if (null != level3) {
//                ruleListBuffer.append(level3)
//              }
//            }
//            case 4 => {
//              val level4 = processRule45(indicatorResult = indicatorResult,
//                rule = ruleConfig,
//                limit = alarmConfig.limit,
//                ruleConfigKey = ruleKey,
//                w2wType = w2wType,
//                limitResult = limitResult,
//                shouldByPass = shouldByPass)
//              if (level4.nonEmpty) {
//                ruleListBuffer.append(level4.get)
//              }
//            }
//            case 5 =>
//            {
//              val level5 = processRule45(indicatorResult = indicatorResult,
//                rule = ruleConfig,
//                limit = alarmConfig.limit,
//                ruleConfigKey = ruleKey,
//                w2wType = w2wType,
//                limitResult = limitResult,
//                shouldByPass = shouldByPass)
//              if (level5.nonEmpty) {
//                ruleListBuffer.append(level5.get)
//              }
//            }
//            case 6 =>
//            {
//              val level6 = processRule(indicatorResult = indicatorResult,
//                rule = ruleConfig,
//                limit = alarmConfig.limit,
//                ruleConfigKey = ruleKey,
//                w2wType = w2wType,
//                limitResult = limitResult,
//                shouldByPass = shouldByPass)
//              if (level6.nonEmpty) {
//                ruleListBuffer.append(level6.get)
//              }
//            }
//            case _ =>
//          }
//
//        }
//
//      }
//
//      if (ruleListBuffer.nonEmpty) {
//
//
//        var RuleTrigger1 = ""
//        var RuleTrigger2 = ""
//        var RuleTrigger3 = ""
//        var RuleTrigger4 = ""
//        var RuleTrigger5 = ""
//        var RuleTrigger6 = ""
//
//
//        for (rule <- ruleListBuffer) {
//          rule.rule match {
//            case 1 => RuleTrigger1 = rule.rule.toString
//            case 2 => RuleTrigger2 = rule.rule.toString
//            case 3 => RuleTrigger3 = rule.rule.toString
//            case 4 => RuleTrigger4 = rule.rule.toString
//            case 5 => RuleTrigger5 = rule.rule.toString
//            case 6 => RuleTrigger6 = rule.rule.toString
//            case _ =>
//          }
//        }
//
//
//        val RuleTrigger = s"${ruleHasNull(RuleTrigger1)}/${ruleHasNull(RuleTrigger2)}/${ruleHasNull(RuleTrigger3)}/${ruleHasNull(RuleTrigger4)}/${ruleHasNull(RuleTrigger5)}/${ruleHasNull(RuleTrigger6)}"
//
//        val rule = parseAlarmLevelRule(indicatorResult, alarmConfig, limitResult.limit, RuleTrigger, ruleListBuffer.toList,limitResult.alarmLevel,limitResult.oocLevel,limitResult.switchStatus)
//
//        (rule, limitResult)
//
//      } else {
//
//        val ruleNull = parseAlarmLevelRule(indicatorResult, alarmConfig, "N/A", "N/A", ruleListBuffer.toList,limitResult.alarmLevel,limitResult.oocLevel,limitResult.switchStatus)
//
//        (ruleNull, limitResult)
//      }
//    }
//  }
//
//  /**
//   * 没有匹配到OOC,OCAP配置,返回indicator值入库 alarmLevel = -1
//   *
//   * @return
//   */
//  def EmptyOOCAndOCAP(indicatorResult: IndicatorResult): (AlarmRuleResult, IndicatorLimitResult) = {
//    val ruleNull: AlarmRuleResult = GetEmptyRule(indicatorResult,-4, 0,"N/N/N/N/N/N","ON")
//    (ruleNull, getIndicatorLimitResult(indicatorResult, -4, 0, "N/N/N/N/N/N", "ON"))
//  }
//
//  /**
//   * get空的rule
//   * @param indicatorResult
//   * @return
//   */
//  def GetEmptyRule(indicatorResult: IndicatorResult,
//                   alarmLevel: Int,
//                   oocLevel: Int,
//                   limit: String,
//                   switchStatus: String
//                  ): AlarmRuleResult = {
//    AlarmRuleResult(
//      indicatorResult.controlPlanVersion.toInt,
//      indicatorResult.chamberName,
//      indicatorResult.indicatorCreateTime,
//      System.currentTimeMillis(),
//      indicatorResult.indicatorId,
//      indicatorResult.runId,
//      indicatorResult.toolName,
//      limit,
//      "N/A",
//      indicatorResult.indicatorValue,
//      indicatorResult.indicatorName,
//      if(indicatorResult.algoClass == null) "" else indicatorResult.algoClass,
//      indicatorResult.controlPlanId,
//      indicatorResult.controlPlanName,
//      -1,
//      indicatorResult.configMissingRatio,
//      indicatorResult.runStartTime,
//      indicatorResult.runEndTime,
//      indicatorResult.windowStartTime,
//      indicatorResult.windowEndTime,
//      indicatorResult.windowDataCreateTime,
//      indicatorResult.locationId,
//      indicatorResult.locationName,
//      indicatorResult.moduleId,
//      indicatorResult.moduleName,
//      indicatorResult.toolGroupId,
//      indicatorResult.toolGroupName,
//      indicatorResult.chamberGroupId,
//      indicatorResult.chamberGroupName,
//      indicatorResult.recipeName,
//      indicatorResult.product,
//      indicatorResult.stage,
//      indicatorResult.materialName,
//      indicatorResult.pmStatus,
//      indicatorResult.pmTimestamp,
//      indicatorResult.area,
//      indicatorResult.section,
//      indicatorResult.mesChamberName,
//      indicatorResult.lotMESInfo,
//      Nil,
//      switchStatus = switchStatus,
//      unit = if(indicatorResult.unit == null)""else indicatorResult.unit,
//      alarmLevel = alarmLevel,
//      oocLevel = oocLevel,
//      dataVersion = indicatorResult.dataVersion
//    )
//  }
//
//  /**
//   * 计算limit
//   * @param indicatorResult
//   * @param alarmConfig
//   * @return
//   */
//  def calculateLimit(indicatorResult: IndicatorResult, alarmConfig: AlarmRuleConfig): IndicatorLimitResult = {
//
//    val limit = alarmConfig.limit
//
//    val USL= limit.USL
//    val UBL= limit.UBL
//    val UCL= limit.UCL
//
//    val LCL= limit.LCL
//    val LBL= limit.LBL
//    val LSL= limit.LSL
//
//
//    try {
//
//      val valueList = indicatorResult.indicatorValue.split("\\|").map(_.toDouble)
//      val alarmLevelList: ListBuffer[Int] = ListBuffer()
//
//      valueList.foreach(value => {
//
//        val alarmLevel = if (USL.nonEmpty && value > USL.get) {
//          3
//        } else if (LSL.nonEmpty && value < LSL.get) {
//          -3
//        } else if (UBL.nonEmpty && value > UBL.get) {
//          2
//        } else if (LBL.nonEmpty && value < LBL.get) {
//          -2
//        } else if (UCL.nonEmpty && value > UCL.get) {
//          1
//        } else if (LCL.nonEmpty && value < LCL.get) {
//          -1
//        }else{
//          0
//        }
//
//        if (alarmLevel != 0) alarmLevelList.append(alarmLevel)
//      })
//
//      val oocLevel=if (alarmLevelList.nonEmpty) {
//        val max =alarmLevelList.max
//        val min=  alarmLevelList.min
//        if(max==min){
//          max
//        }else if(max.abs >= min.abs){
//          max
//        }else{
//          min
//        }
//
//      }else{
//        0
//      }
//
//      val LimitSUB = s"${LCL.getOrElse("")}/${UCL.getOrElse("")}/${LBL.getOrElse("")}/${UBL.getOrElse("")}/${LSL.getOrElse("")}/${USL.getOrElse("")}"
//      getIndicatorLimitResult(indicatorResult, oocLevel, oocLevel, LimitSUB, "ON")
//
//
//    } catch {
//      case ex: NumberFormatException =>
//        logger.warn(s"alarm job limit data Format err ${ExceptionInfo.getExceptionInfo(ex)} IndicatorRuleConfig:$alarmConfig indicatorResult:$indicatorResult ")
//
//        getIndicatorLimitResult(indicatorResult, -7, 0, "N/N/N/N/N/N", "ON")
//      case ex: NullPointerException => logger.warn(s"alarm job limit data null err：${ExceptionInfo.getExceptionInfo(ex)} IndicatorRuleConfig :$alarmConfig indicatorResult:$indicatorResult ")
//        getIndicatorLimitResult(indicatorResult, -7, 0, "N/N/N/N/N/N", "ON")
//
//      case ex: Exception => logger.warn(s"alarm job limit data  err：${ExceptionInfo.getExceptionInfo(ex)} IndicatorRuleConfig :$alarmConfig indicatorResult:$indicatorResult ")
//        getIndicatorLimitResult(indicatorResult, -7, 0, "N/N/N/N/N/N", "ON")
//    }
//  }
//
//  /**
//   *  RULE 1 : Total X point out of limit （X=1），默认总是存在
//   *  RULE 2 : 总共几个点超过limit
//   *  RULE 6 : 连续几个点超过limit
//   * @param indicatorResult
//   * @param rule
//   * @param limit
//   * @param ruleConfigKey
//   * @param w2wType
//   * @param limitResult
//   * @return
//   */
//  def processRule(indicatorResult: IndicatorResult,
//                  rule: AlarmRuleType,
//                  limit:AlarmRuleLimit,
//                  ruleConfigKey: String,
//                  w2wType: String,
//                  limitResult: IndicatorLimitResult,
//                  shouldByPass:Boolean): Option[Rule] = {
//    val mytype = rule.ruleType
//
//    val ruleKeyTuple = getRuleKey(w2wType, indicatorResult)
//    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +mytype
//    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +mytype)
//
//    try {
//
//      var isRuleNew = true
//      val isRuleConfigNew = !this.increaseUpLow.contains(ruleConfigKey)
//
//      val increaseMapData = if (isRuleConfigNew) {
//
//        UpLowData(leave1up = 0, leave1low = 0, leave2up = 0, leave2low = 0, leave3up = 0, leave3low = 0,
//          ruleActionIndexUp1 = 0L, ruleActionIndexLow1 = 0L, ruleActionIndexUp2 = 0L, ruleActionIndexLow2 = 0L,
//          ruleActionIndexUp3 = 0L, ruleActionIndexLow3 = 0L)
//      } else {
//        val ruleGroupData = this.increaseUpLow(ruleConfigKey)
//
//        isRuleNew = !ruleGroupData.contains(ruleKey)
//        if (isRuleNew) {
//          UpLowData(leave1up = 0, leave1low = 0, leave2up = 0, leave2low = 0, leave3up = 0, leave3low = 0,
//            ruleActionIndexUp1 = 0L, ruleActionIndexLow1 = 0L, ruleActionIndexUp2 = 0L, ruleActionIndexLow2 = 0L,
//            ruleActionIndexUp3 = 0L, ruleActionIndexLow3 = 0L)
//        } else {
//          ruleGroupData(ruleKey)
//        }
//      }
//
//      //      logger.warn(s" ruleKey:$ruleKey  w2wKeyList:$w2wKeyList  limitResult$limitResult   increaseMapData: $increaseMapData  increaseUpLow : $increaseUpLow")
//      //是否超过limit 3/2/1
//      if (limitResult.alarmLevel.equals(3)) {
//
//        //计数累加1
//        increaseMapData.setLeave1up(increaseMapData.getLeave1up + 1)
//        increaseMapData.setLeave2up(increaseMapData.getLeave2up + 1)
//        increaseMapData.setLeave3up(increaseMapData.getLeave3up + 1)
//
//        //Rule6 连续情况需清零
//        if(mytype==6) {
//          increaseMapData.setLeave1low(0)
//          increaseMapData.setLeave2low(0)
//          increaseMapData.setLeave3low(0)
//        }
//
//        //判断3套配置 返回是否报警
//        judgeRule(increaseMapData=increaseMapData,
//          rule=rule,
//          limit=limit,
//          ruleConfigKey=ruleConfigKey,
//          ruleKey=ruleKey,
//          isRuleConfigNew=isRuleConfigNew,
//          isRuleNew=isRuleNew,
//          w2wKeyList=w2wKeyList,
//          shouldByPass=shouldByPass,
//          indicatorBypassCondition = indicatorResult.bypassCondition)
//
//      } else if (limitResult.alarmLevel.equals(-3)) {
//
//        //Rule6 连续情况需清零
//        if(mytype==6) {
//          increaseMapData.setLeave1up(0)
//          increaseMapData.setLeave2up(0)
//          increaseMapData.setLeave3up(0)
//        }
//
//        //计数累加1
//        increaseMapData.setLeave1low(increaseMapData.getLeave1low + 1)
//        increaseMapData.setLeave2low(increaseMapData.getLeave2low + 1)
//        increaseMapData.setLeave3low(increaseMapData.getLeave3low + 1)
//
//        //判断3套配置 返回是否报警
//        judgeRule(increaseMapData=increaseMapData,
//          rule=rule,
//          limit=limit,
//          ruleConfigKey=ruleConfigKey,
//          ruleKey=ruleKey,
//          isRuleConfigNew=isRuleConfigNew,
//          isRuleNew=isRuleNew,
//          w2wKeyList=w2wKeyList,
//          shouldByPass=shouldByPass,
//          indicatorBypassCondition = indicatorResult.bypassCondition)
//
//      } else if (limitResult.alarmLevel.equals(2)) {
//        //计数累加1
//        increaseMapData.setLeave1up(increaseMapData.getLeave1up + 1)
//        increaseMapData.setLeave2up(increaseMapData.getLeave2up + 1)
//
//        //Rule6 连续情况需清零
//        if(mytype==6) {
//          increaseMapData.setLeave3up(0)
//          //计数累加1
//          increaseMapData.setLeave1low(0)
//          increaseMapData.setLeave2low(0)
//          increaseMapData.setLeave3low(0)
//        }
//
//        //判断3套配置 返回是否报警
//        judgeRule(increaseMapData=increaseMapData,
//          rule=rule,
//          limit=limit,
//          ruleConfigKey=ruleConfigKey,
//          ruleKey=ruleKey,
//          isRuleConfigNew=isRuleConfigNew,
//          isRuleNew=isRuleNew,
//          w2wKeyList=w2wKeyList,
//          shouldByPass=shouldByPass,
//          indicatorBypassCondition = indicatorResult.bypassCondition)
//
//      } else if (limitResult.alarmLevel.equals(-2)) {
//
//
//        //Rule6 连续情况需清零
//        if(mytype==6) {
//          increaseMapData.setLeave1up(0)
//          increaseMapData.setLeave2up(0)
//          increaseMapData.setLeave3up(0)
//
//          increaseMapData.setLeave3low(0)
//        }
//        //计数累加1
//        increaseMapData.setLeave1low(increaseMapData.getLeave1low + 1)
//        increaseMapData.setLeave2low(increaseMapData.getLeave2low + 1)
//
//        //判断3套配置 返回是否报警
//        judgeRule(increaseMapData=increaseMapData,
//          rule=rule,
//          limit=limit,
//          ruleConfigKey=ruleConfigKey,
//          ruleKey=ruleKey,
//          isRuleConfigNew=isRuleConfigNew,
//          isRuleNew=isRuleNew,
//          w2wKeyList=w2wKeyList,
//          shouldByPass=shouldByPass,
//          indicatorBypassCondition = indicatorResult.bypassCondition)
//
//      } else if (limitResult.alarmLevel.equals(1)) {
//
//        //计数累加1
//        increaseMapData.setLeave1up(increaseMapData.getLeave1up + 1)
//
//        //Rule6 连续情况需清零
//        if(mytype==6) {
//          increaseMapData.setLeave2up(0)
//          increaseMapData.setLeave3up(0)
//          //计数累加1
//          increaseMapData.setLeave1low(0)
//          increaseMapData.setLeave2low(0)
//          increaseMapData.setLeave3low(0)
//        }
//        //判断3套配置 返回是否报警
//        judgeRule(increaseMapData=increaseMapData,
//          rule=rule,
//          limit=limit,
//          ruleConfigKey=ruleConfigKey,
//          ruleKey=ruleKey,
//          isRuleConfigNew=isRuleConfigNew,
//          isRuleNew=isRuleNew,
//          w2wKeyList=w2wKeyList,
//          shouldByPass=shouldByPass,
//          indicatorBypassCondition = indicatorResult.bypassCondition)
//      } else if (limitResult.alarmLevel.equals(-1)) {
//
//        //Rule6 连续情况需清零
//        if(mytype==6) {
//          increaseMapData.setLeave1up(0)
//          increaseMapData.setLeave2up(0)
//          increaseMapData.setLeave3up(0)
//
//          increaseMapData.setLeave2low(0)
//          increaseMapData.setLeave3low(0)
//        }
//        //计数累加1
//        increaseMapData.setLeave1low(increaseMapData.getLeave1low + 1)
//        //判断3套配置 返回是否报警
//        judgeRule(increaseMapData=increaseMapData,
//          rule=rule,
//          limit=limit,
//          ruleConfigKey=ruleConfigKey,
//          ruleKey=ruleKey,
//          isRuleConfigNew=isRuleConfigNew,
//          isRuleNew=isRuleNew,
//          w2wKeyList=w2wKeyList,
//          shouldByPass=shouldByPass,
//          indicatorBypassCondition = indicatorResult.bypassCondition)
//
//      } else {
//        if(mytype==6){
//          //Rule6 连续情况需清零
//          increaseMapData.setLeave1up(0)
//          increaseMapData.setLeave2up(0)
//          increaseMapData.setLeave3up(0)
//          //计数累加1
//          increaseMapData.setLeave1low(0)
//          increaseMapData.setLeave2low(0)
//          increaseMapData.setLeave3low(0)
//        }
//        //数据正常没有超过limit
//
//        None
//      }
//
//    } catch {
//      case ex: Exception => logger.warn(s" processRule $mytype  error:${ExceptionInfo.getExceptionInfo(ex)} indicatorResult:$indicatorResult ruleConfigKey: $ruleConfigKey ruleKey: $ruleKey rulesConfig :$rule")
//        None
//    }
//  }
//
//
//  /**
//   * RULE 3 : X out of N point out of limit(limit可选)，至少选择一个limit
//   * 连续N个点里面有X个点超过limit
//   * @param indicatorResult
//   * @param rule
//   * @param limit
//   * @param ruleConfigKey
//   * @param w2wType
//   * @param limitResult
//   * @return
//   */
//  def processRule3(indicatorResult: IndicatorResult,
//                   rule: AlarmRuleType,
//                   limit:AlarmRuleLimit,
//                   ruleConfigKey: String,
//                   w2wType: String,
//                   limitResult: IndicatorLimitResult,
//                   shouldByPass:Boolean): Rule = {
//    val mytype = rule.ruleType
//
//    val ruleKeyTuple = getRuleKey(w2wType, indicatorResult)
//    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +mytype
//    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +mytype)
//
//    try {
//
//      //是否新rule数据
//
//      val isRuleConfigNew = !this.increaseQueue.contains(ruleConfigKey)
//
//      var isRuleNew = true
//      val increaseMapData: mutable.Queue[Double] = if (isRuleConfigNew) {
//        mutable.Queue[Double]()
//      } else {
//        val ruleGroupData = this.increaseQueue(ruleConfigKey)
//
//        isRuleNew = !ruleGroupData.contains(ruleKey)
//        if (isRuleNew) {
//          mutable.Queue[Double]()
//        } else {
//          ruleGroupData(ruleKey)
//        }
//      }
//
//
//      //取出最大的N
//      val NList = rule.USLorRule45.map(_.N) ++ rule.UBL.map(_.N) ++ rule.UCL.map(_.N) ++ rule.LCL.map(_.N) ++ rule.LBL.map(_.N) ++ rule.LSL.map(_.N)
//
//      val maxContinueN = NList.max
//
//      if(maxContinueN.nonEmpty){
//
//
//        //取出生效的indicator 值
//        val oocLevel = limitResult.oocLevel
//        val indicatorValueList = indicatorResult.indicatorValue.split("\\|").map(_.toDouble)
//        val value= if(oocLevel > 0){
//          indicatorValueList.max
//        }else{
//          indicatorValueList.min
//        }
//
//        //往队列添加新数据，并且删除旧数据
//        if (increaseMapData.size < maxContinueN.get) {
//          increaseMapData += value
//        } else {
//          while (increaseMapData.size >= maxContinueN.get) {
//            increaseMapData.dequeue()
//          }
//          increaseMapData += value
//        }
//
//
//        /**
//         * 清除旧数据
//         */
//        def cleanOldData():Unit = {
//          if (!isRuleNew && !isRuleConfigNew) {
//            val ruleConfigData = this.increaseQueue(ruleConfigKey)
//            ruleConfigData.remove(ruleKey)
//            this.increaseQueue.update(ruleConfigKey, ruleConfigData)
//          }
//        }
//
//        /**
//         * 更新Map
//         */
//        def upDataMap(): Unit = {
//
//          if (isRuleConfigNew) {
//            //没建立RuleConfig配置对应的map
//            this.increaseQueue += (ruleConfigKey -> concurrent.TrieMap[String, mutable.Queue[Double]](ruleKey -> increaseMapData))
//
//          } else if (isRuleNew) {
//            //没建立rule配置对应的map
//            val ruleDataMap = this.increaseQueue(ruleConfigKey) += (ruleKey -> increaseMapData)
//            this.increaseQueue += (ruleConfigKey -> ruleDataMap)
//          } else {
//            //有历史数据map
//            //没超过阈值，保存
//            val ruleDataMap = this.increaseQueue(ruleConfigKey) += (ruleKey -> increaseMapData)
//            this.increaseQueue += (ruleConfigKey -> ruleDataMap)
//          }
//
//
//        }
//
//
//        /**
//         *
//         * 获取Rule，并且更新状态。
//         * 现有业务逻辑是：
//         * 1.同一套index 的报警，只触发第一次超过该limit的情况。
//         * 2.如果是最大的一套，触发后需要清零当前limit，如果不是最大的一套，不清零当前limit。
//         * 3.清理状态，limit之间需要隔离。
//         *
//         * @param alarmInfo
//         * @param alarmLevel
//         * @param ruleActionIndex
//         * @return （rule，是否清理limit）
//         */
//        def getRule(alarmInfo:(List[AlarmRuleParam],Int),alarmLevel:Int,ruleActionIndex:Long): (Option[Rule],Boolean) ={
//
//
//          val index = alarmInfo._1.head.index
//          if(index < alarmInfo._2){
//            //不是最大的一套，不清除状态
//            if(ruleActionIndex >= index){
//              //不是第一次触发该index，不报警
//              (None,false)
//            }else{
//              //是第一次触发该index，报警
//              (Option(Rule(alarmLevel = alarmLevel, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit,alarmInfo = alarmInfo._1)),false)
//            }
//
//          }else{
//            //是最大的一套，清除当前limit计数
//
//            (Option(Rule(alarmLevel = alarmLevel, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit,alarmInfo = alarmInfo._1)),true)
//          }
//
//
//        }
//
//        /**
//         * 计算rule3
//         * @param increaseMapData
//         * @param ruleLimitConfigList
//         * @param isMorethan 是大于还是小于
//         * @param limit
//         * @return
//         */
//        def calculateRule3Alarm(increaseMapData: mutable.Queue[Double],
//                                ruleLimitConfigList:List[AlarmRuleParam],
//                                isMorethan: Boolean,
//                                limit:Option[Double]): (List[AlarmRuleParam],Int) = {
//
//          val map = ruleLimitConfigList.filter(x=>x.X.nonEmpty).map(x => (x.index, x)).sortBy(x => - x._1)
//
//          val alarmInfo = ListBuffer[AlarmRuleParam]()
//
//          //最大的一套配置Index
//          val configMaxIndex=if(map.nonEmpty){map.head._1} else 0
//
//          if (limit.nonEmpty) {
//            //从第三套配置开始遍历，有报警就跳出
//            for ((k, ruleLimitConfig) <- map if alarmInfo.isEmpty) {
//              //筛选出和当前limit配置一样的数据条数N
//              var ContinueNQueue = increaseMapData
//
//              //N X 为null 就是没配
//              if(ruleLimitConfig.N.nonEmpty && ruleLimitConfig.X.nonEmpty){
//
//                while (ContinueNQueue.size > ruleLimitConfig.N.get) {
//                  ContinueNQueue = ContinueNQueue.tail
//                }
//
//                //筛选大于或者小小于 Level3Up 的
//                val res = if (isMorethan) {
//                  for (data <- ContinueNQueue if data > limit.get) yield {
//                    data
//                  }
//                } else {
//                  for (data <- ContinueNQueue if data < limit.get) yield {
//                    data
//                  }
//                }
//                //报警，输出的配置
//                if (res.size >= ruleLimitConfig.X.get) {
//                  alarmInfo.append(ruleLimitConfig)
//                }
//
//              }
//
//
//            }
//          }
//
//          (alarmInfo.toList,configMaxIndex)
//
//        }
//
//
//        val alarmInfo =  calculateRule3Alarm(increaseMapData,ruleLimitConfigList=rule.USLorRule45,isMorethan = true,limit=limit.USL)
//
//        if(alarmInfo._1.nonEmpty){
//
//
//          //          //获取报警信息
//          //          val tuple = getRule(alarmInfo = alarmInfo,alarmLevel = 3,ruleActionIndex = increaseMapData.getRuleActionIndexUp3)
//          //
//          //          //更新报警index
//          //          increaseMapData.setRuleActionIndexUp3(alarmInfo._1.head.index)
//          //
//          //          //是否清除limit数据
//          //          if(tuple._2){
//          //            increaseMapData.setLeave3up(0)
//          //            increaseMapData.setRuleActionIndexUp3(0L)
//          //          }
//          //          //更新数据
//          //          upDataMap()
//          //
//          //          //返回报警
//          //          tuple._1
//
//
//
//
//
//
//          //如果不是最大的一套，就不清除
//          if(alarmInfo._1.head.index < alarmInfo._2){
//            upDataMap()
//          }else{
//            //map里清除标记
//            cleanOldData()
//          }
//
//          //根据 notification和action 开关，过滤是否报警
//          val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//            indicatorBypassCondition=indicatorResult.bypassCondition,
//            alarmInfo = alarmInfo._1 )
//          //报警
//          Rule(alarmLevel = 3, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit, alarmInfo = byPassAlarmInfo)
//        }else{
//
//          val alarmInfo = calculateRule3Alarm(increaseMapData,ruleLimitConfigList=rule.LSL,isMorethan = false,limit=limit.LSL)
//          if(alarmInfo._1.nonEmpty){
//            //如果不是最大的一套，就不清除
//            if(alarmInfo._1.head.index < alarmInfo._2){
//              upDataMap()
//            }else{
//              //map里清除标记
//              cleanOldData()
//            }
//
//
//            //根据 notification和action 开关，过滤是否报警
//            val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//              indicatorBypassCondition=indicatorResult.bypassCondition,
//              alarmInfo = alarmInfo._1 )
//            //报警
//            Rule(alarmLevel = -3, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit, alarmInfo = byPassAlarmInfo)
//
//          }else{
//            val alarmInfo =  calculateRule3Alarm(increaseMapData,ruleLimitConfigList=rule.UBL,isMorethan = true,limit=limit.UBL)
//
//            if(alarmInfo._1.nonEmpty){
//              //如果不是最大的一套，就不清除
//              if(alarmInfo._1.head.index < alarmInfo._2){
//                upDataMap()
//              }else{
//                //map里清除标记
//                cleanOldData()
//              }
//              //根据 notification和action 开关，过滤是否报警
//              val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//                indicatorBypassCondition=indicatorResult.bypassCondition,
//                alarmInfo = alarmInfo._1 )
//              //报警
//              Rule(alarmLevel = 2, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit, alarmInfo = byPassAlarmInfo)
//            }else{
//
//              val alarmInfo = calculateRule3Alarm(increaseMapData,ruleLimitConfigList=rule.LBL,isMorethan = false,limit=limit.LBL)
//              if(alarmInfo._1.nonEmpty){
//                //如果不是最大的一套，就不清除
//                if(alarmInfo._1.head.index < alarmInfo._2){
//                  upDataMap()
//                }else{
//                  //map里清除标记
//                  cleanOldData()
//                }
//                //根据 notification和action 开关，过滤是否报警
//                val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//                  indicatorBypassCondition=indicatorResult.bypassCondition,
//                  alarmInfo = alarmInfo._1 )
//                //报警
//                Rule(alarmLevel = -2, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit, alarmInfo = byPassAlarmInfo)
//              }else{
//
//                val alarmInfo = calculateRule3Alarm(increaseMapData,ruleLimitConfigList=rule.UCL,isMorethan = true,limit=limit.UCL)
//                if(alarmInfo._1.nonEmpty){
//                  //如果不是最大的一套，就不清除
//                  if(alarmInfo._1.head.index < alarmInfo._2){
//                    upDataMap()
//                  }else{
//                    //map里清除标记
//                    cleanOldData()
//                  }
//                  //根据 notification和action 开关，过滤是否报警
//                  val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//                    indicatorBypassCondition=indicatorResult.bypassCondition,
//                    alarmInfo = alarmInfo._1 )
//                  //报警
//                  Rule(alarmLevel = 1, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit, alarmInfo = byPassAlarmInfo)
//                }else{
//
//                  val alarmInfo = calculateRule3Alarm(increaseMapData,ruleLimitConfigList=rule.LCL,isMorethan = false,limit=limit.LCL)
//                  if(alarmInfo._1.nonEmpty){
//                    //如果不是最大的一套，就不清除
//                    if(alarmInfo._1.head.index < alarmInfo._2){
//                      upDataMap()
//                    }else{
//                      //map里清除标记
//                      cleanOldData()
//                    }
//                    //根据 notification和action 开关，过滤是否报警
//                    val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//                      indicatorBypassCondition=indicatorResult.bypassCondition,
//                      alarmInfo = alarmInfo._1 )
//                    //报警
//                    Rule(alarmLevel = -1, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit, alarmInfo = byPassAlarmInfo)
//                  }else{
//                    //                  for(w2wKey <- w2wKeyList) {
//                    //                    if (isRuleConfigNew) {
//                    //                      //没建立RuleConfig配置对应的map
//                    //                      this.increaseQueue += (ruleConfigKey -> concurrent.TrieMap[String, mutable.Queue[Double]](w2wKey -> increaseMapData))
//                    //
//                    //                    } else if (isRuleNew) {
//                    //                      //没建立rule配置对应的map
//                    //                      val ruleDataMap = this.increaseQueue(ruleConfigKey) += (w2wKey -> increaseMapData)
//                    //                      this.increaseQueue += (ruleConfigKey -> ruleDataMap)
//                    //                    } else {
//                    //                      //有历史数据map
//                    //                      //没超过阈值，保存
//                    //                      val ruleDataMap = this.increaseQueue(ruleConfigKey) += (w2wKey -> increaseMapData)
//                    //                      this.increaseQueue += (ruleConfigKey -> ruleDataMap)
//                    //                    }
//                    //                  }
//                    upDataMap()
//                    null
//                  }
//                }
//              }
//            }
//          }
//        }
//
//
//      }else{
//        logger.warn(s" processRule $mytype  error: N is null  indicatorResult:$indicatorResult ruleConfigKey: $ruleConfigKey ruleKey: $ruleKey rulesConfig :$rule")
//
//        null
//
//      }
//
//
//
//
//    } catch {
//      case ex:Exception => logger.warn(s" processRule $mytype  error:${ExceptionInfo.getExceptionInfo(ex)} indicatorResult:$indicatorResult ruleConfigKey: $ruleConfigKey ruleKey: $ruleKey rulesConfig :$rule")
//        null
//    }
//  }
//
//  /**
//   * RULE4 :连续上升
//   * RULE5 :连续下降
//   * @param indicatorResult
//   * @param rule
//   * @param limit
//   * @param ruleConfigKey
//   * @param w2wType
//   * @param limitResult
//   * @return
//   */
//  def processRule45(indicatorResult: IndicatorResult,
//                    rule: AlarmRuleType,
//                    limit:AlarmRuleLimit,
//                    ruleConfigKey: String,
//                    w2wType: String,
//                    limitResult: IndicatorLimitResult,
//                    shouldByPass:Boolean): Option[Rule] = {
//
//    val mytype = rule.ruleType
//
//    val ruleKeyTuple = getRuleKey(w2wType, indicatorResult)
//    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +mytype
//    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +mytype)
//
//    try {
//
//
//      val indicatorValue= indicatorResult.indicatorValue.toDouble
//
//
//
//      //查groupConfig状态
//      if (!this.increaseContinue.contains(ruleConfigKey)) {
//
//        this.increaseContinue += (ruleConfigKey ->
//          concurrent.TrieMap[String, (CountData,Long)](ruleKey ->
//            (CountData(0, indicatorValue),0L)))
//        None
//      } else {
//        //groupConfig by Rule 状态
//        val ruleGroupData = this.increaseContinue(ruleConfigKey)
//        //查具体到数据的indicator状态
//        if (!ruleGroupData.contains(ruleKey)) {
//
//          //初始化添加配置状态
//          //            for(w2wKey <- w2wKeyList){
//          //              ruleGroupData += (w2wKey -> CountData(0, indicatorValue))
//          //            }
//          ruleGroupData += (ruleKey -> (CountData(0, indicatorValue),0L))
//          this.increaseContinue += (ruleConfigKey -> ruleGroupData)
//
//          None
//
//        } else {
//
//          val countData = ruleGroupData(ruleKey)._1
//
//          val actionIndex = ruleGroupData(ruleKey)._2
//
//
//          //rule4 连续上升 打断上升清零
//          if(mytype==4 && countData.getValue >= indicatorValue){
//
//            countData.setCount(0)
//            countData.setValue(indicatorValue)
//
//            //              for(w2wKey <- w2wKeyList){
//            //                ruleGroupData += (w2wKey -> countData)
//            //              }
//
//            ruleGroupData += (ruleKey -> (countData,0L))
//
//            this.increaseContinue += (ruleConfigKey -> ruleGroupData)
//            None
//
//
//            //rule5 连续下降 打断下降清零
//          }else if(mytype==5 && countData.getValue <= indicatorValue){
//
//            countData.setCount(0)
//            countData.setValue(indicatorValue)
//            //              for(w2wKey <- w2wKeyList){
//            //                ruleGroupData += (w2wKey -> cd)
//            //              }
//
//            ruleGroupData += (ruleKey -> (countData,0L))
//            this.increaseContinue += (ruleConfigKey -> ruleGroupData)
//            None
//
//          }else{
//
//            /**
//             *
//             * @param countData
//             * @param ruleLimitConfigList
//             * @return
//             */
//            def calculateRule45Alarm(countData:CountData,ruleLimitConfigList:List[AlarmRuleParam]): (List[AlarmRuleParam],Int) = {
//
//              //拿到所有action 配置
//              val map = ruleLimitConfigList.filter(x=>x.X.nonEmpty).map(x => (x.index, x)).sortBy(x => - x._1)
//
//              val alarmInfo=ListBuffer[AlarmRuleParam]()
//
//              //最大的一套配置Index
//              val configMaxIndex=if(map.nonEmpty){map.head._1} else 0
//              for ((k,ruleLimitConfig) <- map if alarmInfo.isEmpty) {
//
//                //X为null 即为没配
//                if(ruleLimitConfig.X.nonEmpty){
//                  //约定 只会配置连续上升两个点以上
//                  if(ruleLimitConfig.X.get >= 2 && countData.getCount >= ruleLimitConfig.X.get - 1){
//                    alarmInfo.append(ruleLimitConfig)
//
//                  }
//                }
//
//
//              }
//              (alarmInfo.toList,configMaxIndex)
//            }
//
//
//
//
//
//
//            /**
//             *
//             * 获取Rule，并且更新状态。
//             * 现有业务逻辑是：
//             * 1.同一套index 的报警，只触发第一次连续上升/下降的情况。
//             * 2.如果是最大的一套，触发后需要清零当前计数，如果不是最大的一套，不清零当前计数。
//             *
//             * @param alarmInfo
//             * @param alarmLevel
//             * @param ruleActionIndex
//             * @return （rule，是否清理limit）
//             */
//            def getRule(alarmInfo:(List[AlarmRuleParam],Int),ruleActionIndex:Long): (Option[Rule],Boolean) ={
//
//              //index代表当前是第几套
//              val index = alarmInfo._1.head.index
//              if(index < alarmInfo._2){
//                //不是最大的一套，不清除状态
//                if(ruleActionIndex >= index){
//                  //不是第一次触发该index，不报警
//                  (None,false)
//                }else{
//
//                  //根据 notification和action 开关，过滤是否报警
//                  val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//                    indicatorBypassCondition=indicatorResult.bypassCondition,
//                    alarmInfo = alarmInfo._1 )
//
//                  //是第一次触发该index，报警
//                  (Option(Rule(alarmLevel = limitResult.alarmLevel, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit,alarmInfo = byPassAlarmInfo)),false)
//                }
//
//              }else{
//                //根据 notification和action 开关，过滤是否报警
//                val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//                  indicatorBypassCondition=indicatorResult.bypassCondition,
//                  alarmInfo = alarmInfo._1 )
//
//                //是最大的一套，清除当前limit计数
//
//                (Option(Rule(alarmLevel = limitResult.alarmLevel, rule = rule.ruleType, ruleTypeName = rule.ruleTypeName,limit = limit,alarmInfo = byPassAlarmInfo)),true)
//              }
//
//
//            }
//
//            val alarmInfo = calculateRule45Alarm(countData = countData, rule.USLorRule45)
//
//            if(alarmInfo._1.nonEmpty){
//
//
//              //获取报警信息
//              val tuple = getRule(alarmInfo = alarmInfo,ruleActionIndex = actionIndex)
//
//
//              //是否清除limit数据
//              if(tuple._2){
//                //增加
//                countData.setCount(countData.getCount + 1)
//                countData.setValue(indicatorValue)
//
//                ruleGroupData += (ruleKey -> (countData,alarmInfo._1.head.index))
//                this.increaseContinue += (ruleConfigKey -> ruleGroupData)
//              }else{
//                //增加
//                countData.setCount(0)
//                countData.setValue(indicatorValue)
//
//                //                for(w2wKey <- w2wKeyList){
//                //                  ruleGroupData += (w2wKey -> cd)
//                //                }
//
//                ruleGroupData += (ruleKey -> (countData,0L))
//                this.increaseContinue += (ruleConfigKey -> ruleGroupData)
//              }
//
//
//
//
//              //返回报警
//              tuple._1
//
//
//
//            }else{
//
//              //增加
//              countData.setCount(countData.getCount + 1)
//              countData.setValue(indicatorValue)
//
//              //                for(w2wKey <- w2wKeyList){
//              //                  ruleGroupData += (w2wKey -> cd)
//              //                }
//
//              ruleGroupData += (ruleKey -> (countData,actionIndex))
//              this.increaseContinue += (ruleConfigKey -> ruleGroupData)
//
//
//              None
//            }
//
//
//          }
//
//        }
//
//      }
//
//    } catch {
//      case ex: Exception => logger.warn(s" processRule $mytype  error:${ExceptionInfo.getExceptionInfo(ex)} indicatorResult:$indicatorResult ruleConfigKey: $ruleConfigKey ruleKey: $ruleKey rulesConfig :$rule")
//        None
//    }
//  }
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
//   * 获取超过limit的点数
//   *
//   */
//  def getOutOfLimitPoint(increaseMapData: mutable.Queue[Double], ContinueX: Int, limitData: String, isMorethan: Boolean): Int = {
//    //筛选出和当前limit配置一样的数据条数
//    var Level3UpContinueXQueue = increaseMapData
//    while (Level3UpContinueXQueue.size > ContinueX) {
//      Level3UpContinueXQueue = Level3UpContinueXQueue.tail
//    }
//    //筛选大于Level3Up 的
//    val res = if (isMorethan) {
//      for (data <- Level3UpContinueXQueue if data > limitData.toDouble) yield {
//        data
//      }
//    } else {
//      for (data <- Level3UpContinueXQueue if data < limitData.toDouble) yield {
//        data
//      }
//    }
//    res.size
//  }
//
//
//
//
//
//  /**
//   * 字符串为空
//   */
//  def ruleHasNull(str: String): String = {
//    if (str == null || str == "" || str.length == 0 || str == "#") {
//      "N"
//    } else {
//      str
//    }
//  }
//
//  /**
//   * 获取匹配后的IndicatorResult
//   *
//   * @param alarmLevel 线的level
//   * @param limit      各种线
//   * @return
//   */
//  def getIndicatorLimitResult(indicatorResult: IndicatorResult,
//                              alarmLevel: Int,
//                              oocLevel: Int,
//                              limit: String, switchStatus: String): IndicatorLimitResult = {
//    IndicatorLimitResult(indicatorResult.runId
//      , indicatorResult.toolName
//      , indicatorResult.chamberName
//      , indicatorResult.indicatorValue
//      , indicatorResult.indicatorId.toString
//      , indicatorResult.indicatorName
//      , alarmLevel
//      , oocLevel
//      , indicatorResult.runEndTime.toString
//      , limit
//      , switchStatus
//      , if(indicatorResult.unit == null) "" else indicatorResult.unit)
//  }
//
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
//
//  /**
//   * 判断3套配置 返回是否报警
//   * @param increaseMapData
//   * @param rule
//   * @param limit
//   * @param ruleConfigKey
//   * @param ruleKey
//   * @param isRuleConfigNew
//   * @param isRuleNew
//   * @param w2wKeyList
//   * @return
//   */
//  def judgeRule(increaseMapData:UpLowData,
//                rule: AlarmRuleType,
//                limit:AlarmRuleLimit,
//                ruleConfigKey: String,
//                ruleKey:String,
//                isRuleConfigNew: Boolean,
//                isRuleNew: Boolean,
//                w2wKeyList: ListBuffer[String],
//                shouldByPass:Boolean,
//                indicatorBypassCondition:Option[ByPassCondition]): Option[Rule] = {
//
//    /**
//     * 状态里清除标记,只清除当前线的标记
//     */
//    //    def cleanMap(): Unit = {
//    //
//    //      if (!isRuleNew && !isRuleConfigNew) {
//    //        val ruleConfigData = this.increaseUpLow(ruleConfigKey)
//    //
//    //        ruleConfigData.put(ruleKey,increaseMapData)
//    //        ruleConfigData.remove(ruleKey)
//    //        this.increaseUpLow.update(ruleConfigKey, ruleConfigData)
//    //      }
//    //    }
//
//    /**
//     * 更新状态，
//     */
//    def upDataMap(): Unit = {
//
//      if (isRuleConfigNew) {
//        //没建立RuleConfig配置对应的map
//        this.increaseUpLow += (ruleConfigKey -> concurrent.TrieMap[String, UpLowData](ruleKey -> increaseMapData))
//      } else if (isRuleNew) {
//        //没建立rule配置对应的map
//        val ruleDataMap = this.increaseUpLow(ruleConfigKey) += (ruleKey -> increaseMapData)
//        this.increaseUpLow += (ruleConfigKey -> ruleDataMap)
//      } else {
//        //有历史数据map
//        //没超过阈值，保存
//        val ruleDataMap = this.increaseUpLow(ruleConfigKey) += (ruleKey -> increaseMapData)
//        this.increaseUpLow += (ruleConfigKey -> ruleDataMap)
//      }
//
//    }
//
//    /**
//     *
//     * 获取Rule，并且更新状态。
//     * 现有业务逻辑是：
//     * 1.同一套index 的报警，只触发第一次超过该limit的情况。
//     * 2.如果是最大的一套，触发后需要清零当前limit，如果不是最大的一套，不清零当前limit。
//     * 3.清理状态，limit之间需要隔离。
//     *
//     * @param alarmInfo
//     * @param alarmLevel
//     * @param ruleActionIndex
//     * @return （rule，是否清理limit）
//     */
//    def getRule(alarmInfo:(List[AlarmRuleParam],Int),
//                alarmLevel:Int,
//                ruleActionIndex:Long): (Option[Rule],Boolean) ={
//      //根据 notification和action 开关，过滤是否报警
//    val  byPassAlarmInfo= filterByPass(shouldByPass=shouldByPass,
//      indicatorBypassCondition=indicatorBypassCondition,
//      alarmInfo = alarmInfo._1 )
//
//      val index = alarmInfo._1.head.index
//      if(index < alarmInfo._2){
//        //不是最大的一套，不清除状态
//        if(ruleActionIndex >= index){
//          //不是第一次触发该index，不报警,不清除状态
//          (None,false)
//        }else{
//          //是第一次触发该index，报警,不清除状态
//          (Option(Rule(alarmLevel = alarmLevel,
//            rule = rule.ruleType,
//            ruleTypeName = rule.ruleTypeName,
//            limit = limit,
//            alarmInfo = byPassAlarmInfo
//          )),
//            false)
//        }
//      }else{
//        //是最大的一套，清除当前limit计数
//        (Option(Rule(alarmLevel = alarmLevel,
//          rule = rule.ruleType,
//          ruleTypeName = rule.ruleTypeName,
//          limit = limit,
//          alarmInfo = byPassAlarmInfo
//        )),
//          true)
//      }
//    }
//
//
//
//
//    val ruleList = ListBuffer[Option[Rule]]()
//
//    val alarmInfoUSL = calculateAlarm(rule.USLorRule45,increaseMapData.leave3up)
//    //logger.warn(s"alarmInfo $alarmInfo" )
//
//
//    if(alarmInfoUSL._1.nonEmpty){
//
//      //获取报警信息
//      val tuple = getRule(alarmInfo = alarmInfoUSL,alarmLevel = 3,ruleActionIndex = increaseMapData.getRuleActionIndexUp3)
//
//      //更新报警index
//      increaseMapData.setRuleActionIndexUp3(alarmInfoUSL._1.head.index)
//
//      //是否清除limit数据
//      if(tuple._2){
//        increaseMapData.setLeave3up(0)
//        increaseMapData.setRuleActionIndexUp3(0L)
//      }
//      //更新数据
//      upDataMap()
//
//      //返回报警
//      ruleList.append(tuple._1)
//
//    }
//
//    val alarmInfoLSL = calculateAlarm(rule.LSL,increaseMapData.leave3low)
//
//    if(alarmInfoLSL._1.nonEmpty){
//
//      //获取报警信息
//      val tuple = getRule(alarmInfo = alarmInfoLSL,alarmLevel = -3,ruleActionIndex = increaseMapData.getRuleActionIndexLow3)
//
//      //更新报警index
//      increaseMapData.setRuleActionIndexLow3(alarmInfoLSL._1.head.index)
//
//      //是否清除limit数据
//      if(tuple._2){
//        increaseMapData.setLeave3low(0)
//        increaseMapData.setRuleActionIndexLow3(0L)
//      }
//      //更新数据
//      upDataMap()
//
//      //返回报警
//      ruleList.append(tuple._1)
//
//    }
//
//    val alarmInfoUBL = calculateAlarm(rule.UBL,increaseMapData.leave2up)
//
//    if(alarmInfoUBL._1.nonEmpty){
//
//      //获取报警信息
//      val tuple = getRule(alarmInfo = alarmInfoUBL,alarmLevel = 2,ruleActionIndex = increaseMapData.getRuleActionIndexUp2)
//
//      //更新报警index
//      increaseMapData.setRuleActionIndexUp2(alarmInfoUBL._1.head.index)
//
//      //是否清除limit数据
//      if(tuple._2){
//        increaseMapData.setLeave2up(0)
//        increaseMapData.setRuleActionIndexUp2(0L)
//      }
//      //更新数据
//      upDataMap()
//
//      //返回报警
//      ruleList.append(tuple._1)
//    }
//
//
//    val alarmInfoLBL = calculateAlarm(rule.LBL,increaseMapData.leave2low)
//
//    if(alarmInfoLBL._1.nonEmpty){
//
//
//      //获取报警信息
//      val tuple = getRule(alarmInfo = alarmInfoLBL,alarmLevel = -2,ruleActionIndex = increaseMapData.getRuleActionIndexLow2)
//
//      //更新报警index
//      increaseMapData.setRuleActionIndexLow2(alarmInfoLBL._1.head.index)
//
//      //是否清除limit数据
//      if(tuple._2){
//        increaseMapData.setLeave2low(0)
//        increaseMapData.setRuleActionIndexLow2(0L)
//      }
//      //更新数据
//      upDataMap()
//
//      //返回报警
//      ruleList.append(tuple._1)
//
//
//
//    }
//
//
//    val alarmInfoUCL = calculateAlarm(rule.UCL,increaseMapData.leave1up)
//
//    if(alarmInfoUCL._1.nonEmpty){
//
//
//      //获取报警信息
//      val tuple = getRule(alarmInfo = alarmInfoUCL,alarmLevel = 1,ruleActionIndex = increaseMapData.getRuleActionIndexUp1)
//
//      //更新报警index
//      increaseMapData.setRuleActionIndexUp1(alarmInfoUCL._1.head.index)
//
//      //是否清除limit数据
//      if(tuple._2){
//        increaseMapData.setLeave1up(0)
//        increaseMapData.setRuleActionIndexUp1(0L)
//      }
//      //更新数据
//      upDataMap()
//
//      //返回报警
//      ruleList.append(tuple._1)
//    }
//
//    val alarmInfoLCL = calculateAlarm(rule.LCL,increaseMapData.leave1low)
//
//    if(alarmInfoLCL._1.nonEmpty){
//      //获取报警信息
//      val tuple = getRule(alarmInfo = alarmInfoLCL,alarmLevel = -1,ruleActionIndex = increaseMapData.getRuleActionIndexLow1)
//
//      //更新报警index
//      increaseMapData.setRuleActionIndexLow1(alarmInfoLCL._1.head.index)
//
//      //是否清除limit数据
//      if(tuple._2){
//        increaseMapData.setLeave1low(0)
//        increaseMapData.setRuleActionIndexLow1(0L)
//      }
//      //更新数据
//      upDataMap()
//
//      //返回报警
//      ruleList.append(tuple._1)
//    }
//
//    //                for(w2wKey <- w2wKeyList) {
//    //                  if (isRuleConfigNew) {
//    //                    //没建立RuleConfig配置对应的map
//    //                    this.increaseUpLow += (ruleConfigKey -> concurrent.TrieMap[String, UpLowData](w2wKey -> increaseMapData))
//    //                  } else if (isRuleNew) {
//    //                    //没建立rule配置对应的map
//    //                    val ruleDataMap = this.increaseUpLow(ruleConfigKey) += (w2wKey -> increaseMapData)
//    //                    this.increaseUpLow += (ruleConfigKey -> ruleDataMap)
//    //                  } else {
//    //                    //有历史数据map
//    //                    //没超过阈值，保存
//    //                    val ruleDataMap = this.increaseUpLow(ruleConfigKey) += (w2wKey -> increaseMapData)
//    //                    this.increaseUpLow += (ruleConfigKey -> ruleDataMap)
//    //                  }
//    //                }
//
//    upDataMap()
//
//    //TODO 实现还是有点问题，phase4改，phase3零时这样
//    val maybeRules = ruleList.filter(_.nonEmpty)
//    if(maybeRules.nonEmpty){
//
//      val alarmLevelList= maybeRules.map(_.get.alarmLevel)
//      val alarmLevel=if(alarmLevelList.head >= 0){
//        alarmLevelList.max
//      }else{
//        alarmLevelList.min
//      }
//      val actionList = maybeRules.map(_.get.alarmInfo).reduce(_ ++ _).flatMap(_.action)
//
//      val headAlarmInfo = maybeRules.head.get.alarmInfo.head
//      Option(Rule(
//        alarmLevel = alarmLevel,
//        rule = rule.ruleType,
//        ruleTypeName = rule.ruleTypeName,
//        limit = limit,
//        alarmInfo = List(AlarmRuleParam(index = headAlarmInfo.index,
//          X = headAlarmInfo.X,
//          N = headAlarmInfo.N,
//          action = actionList))))
//    }else{
//      None
//    }
//
//
//
//
//  }
//
//
//
//
//  /**
//   * 计算alarm
//   * @param ruleLimitConfigList
//   * @param limitCount
//   * @return
//   */
//  def calculateAlarm(ruleLimitConfigList:List[AlarmRuleParam],limitCount:Int): (List[AlarmRuleParam],Int) = {
//
//    //拿到所有action 配置
//    val map = ruleLimitConfigList.filter(x=>x.X.nonEmpty).map(x => (x.index, x)).sortBy(x => - x._1)
//
//    val alarmInfo = ListBuffer[AlarmRuleParam]()
//
//
//    val configMaxIndex=if(map.nonEmpty){map.head._1} else 0
//
//    for ((k,ruleLimitConfig) <- map if alarmInfo.isEmpty) {
//
//      if(ruleLimitConfig.X.nonEmpty){
//        if(limitCount >= ruleLimitConfig.X.get){
//          alarmInfo.append(ruleLimitConfig)
//        }
//      }
//    }
//
//    (alarmInfo.toList,configMaxIndex)
//
//
//  }
//
//
//  def parseAlarmLevelRule(indicatorResult: IndicatorResult,
//                          alarmConfig: AlarmRuleConfig,
//                          LIMIT: String,
//                          RuleTrigger: String,
//                          ruleList: List[Rule],
//                          alarmLevel:Int,
//                          oocLevel:Int,
//                          switchStatus:String): AlarmRuleResult ={
//    AlarmRuleResult(
//      indicatorResult.controlPlanVersion.toInt,
//      indicatorResult.chamberName,
//      indicatorResult.indicatorCreateTime,
//      //alarm创建的时间应该是当前时间
//      System.currentTimeMillis(),
//      indicatorResult.indicatorId,
//      indicatorResult.runId,
//      indicatorResult.toolName,
//      LIMIT,
//      RuleTrigger,
//      indicatorResult.indicatorValue,
//      indicatorResult.indicatorName,
//      if(indicatorResult.algoClass == null) "" else indicatorResult.algoClass,
//      indicatorResult.controlPlanId,
//      indicatorResult.controlPlanName,
//      indicatorResult.missingRatio,
//      indicatorResult.configMissingRatio,
//      indicatorResult.runStartTime,
//      indicatorResult.runEndTime,
//      indicatorResult.windowStartTime,
//      indicatorResult.windowEndTime,
//      indicatorResult.windowDataCreateTime,
//      indicatorResult.locationId,
//      indicatorResult.locationName,
//      indicatorResult.moduleId,
//      indicatorResult.moduleName,
//      indicatorResult.toolGroupId,
//      indicatorResult.toolGroupName,
//      indicatorResult.chamberGroupId,
//      indicatorResult.chamberGroupName,
//      indicatorResult.recipeName,
//      indicatorResult.product,
//      indicatorResult.stage,
//      indicatorResult.materialName,
//      indicatorResult.pmStatus,
//      indicatorResult.pmTimestamp,
//      indicatorResult.area,
//      indicatorResult.section,
//      indicatorResult.mesChamberName,
//      indicatorResult.lotMESInfo,
//      ruleList,
//      switchStatus = switchStatus,
//      unit = if(indicatorResult.unit == null) "" else indicatorResult.unit ,
//      alarmLevel = alarmLevel,
//      oocLevel = oocLevel,
//      dataVersion = indicatorResult.dataVersion
//    )
//  }
//
//
//  /**
//   * 根据 notification和action 开关，过滤是否报警
//   * @param shouldByPass
//   * @param indicatorBypassCondition
//   * @param alarmInfo
//   * @return
//   */
//  def filterByPass(shouldByPass:Boolean,
//                   indicatorBypassCondition:Option[ByPassCondition],
//                   alarmInfo:List[AlarmRuleParam]
//                  ): List[AlarmRuleParam] ={
//    //过滤过by pass 的 alarm info
//     if(shouldByPass){
//      alarmInfo.map(y=>
//        AlarmRuleParam(index = y.index,
//          X = y.X,
//          N = y.N,
//          action = y.action.filter(z=>{
//            if(indicatorBypassCondition.nonEmpty){
//              //过滤不符合条件的indicator BypassCondition
//              val notificationSwitch= indicatorBypassCondition.get.notificationSwitch
//              val actionSwitch=  indicatorBypassCondition.get.actionSwitch
//              if(notificationSwitch&&actionSwitch) {
//                //都关闭就全部过滤
//                false
//              }else if(notificationSwitch){
//                //是否等于notification 等于就false过滤掉
//                !z.sign.getOrElse("").equals(MainFabConstants.notification)
//              }else if(actionSwitch){
//                //是否等于action 等于就false过滤掉
//                !z.sign.getOrElse("").equals(MainFabConstants.action)
//              }else{
//                //都没关
//                true
//              }
//              //没有by pass 配置理论上不应该走到这里来。
//            }else true
//
//          }
//          ))
//      )
//    }else{
//      //没有 ByPass 触发，都不过滤
//      alarmInfo
//    }
//  }
//
//
//}
