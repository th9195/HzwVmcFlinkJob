package com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules

import com.hzw.fdc.function.online.MainFabAlarm.IndicatorAlarmProcessFunctionNew
import com.hzw.fdc.scalabean.{AlarmRuleAction, AlarmRuleLimit, AlarmRuleParam, AlarmRuleType, ByPassCondition, CountData, Count_1_2_6, Count_1_2_6_merge_by_level, Count_3_merge_by_level, IndicatorLimitResult, IndicatorResult, Rule}
import com.hzw.fdc.util.MainFabConstants
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer

/**
 * AlarmProcessRule
 *
 * @desc:
 * @author tobytang
 * @date 2022/6/16 11:15
 * @since 1.0.0
 * @update 2022/6/16 11:15
 * */

@SerialVersionUID(1L)
class AlarmProcessRuleParent extends  Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmProcessRuleParent])

  // key1 为indicatorRule配置的 Key
  // key2 (根据配置 w2wType) 为(toolid + chamberid + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + stage + indicatorid + ruletype) or
  // # 第几套规则
  // Count_1_2_6 18个属性(类型都是Int计数): level(告警级别)_index(组号)
  //    例如: USL_1;UBL_1;UCL_1;LCL_1;LBL_1;LSL_1;
  //         USL_2;UBL_2;UCL_2;LCL_2;LBL_2;LSL_2;......
  var countMap_Rule_1_2_6 = new concurrent.TrieMap[String, concurrent.TrieMap[String, Count_1_2_6]]
  var countMap_Rule_1_2_6_merge_by_level = new concurrent.TrieMap[String, concurrent.TrieMap[String, Count_1_2_6_merge_by_level]]

  // key1 : indicatorRule配置的 Key
  // key2 (根据配置 w2wType) 为(toolid + chamberid + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + stage + indicatorid + ruletype) or
  // value : Queue 存储 所有配置中最大的 N 个数据
  var countQueue_Rule_3 = new concurrent.TrieMap[String, concurrent.TrieMap[String, mutable.Queue[Double]]]

  // key1 : indicatorRule配置的 Key
  // key2 (根据配置 w2wType) 为(toolid + chamberid + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + stage + indicatorid + ruletype) or
  // value : Count_3_merge_by_level
  var countMap_Rule_3_merge_by_level = new concurrent.TrieMap[String, concurrent.TrieMap[String, Count_3_merge_by_level]]


  //key1为indicatorRule配置的 Key
  // key2 (根据配置 w2wType) 为(toolid + chamberid + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + indicatorid + ruletype) or
  // (toolid + chamberid + recipe + product + stage + indicatorid + ruletype) 可以细化到第几套Rule
  //value: CountData List数组 没有 告警级别的概念， 只有组号; List长度最多为3
  var countContinueIncreaseMap_Rule_4_5 = new concurrent.TrieMap[String, concurrent.TrieMap[String, ListBuffer[CountData]]]
  var countContinueIncreaseMap_Rule_4_5_merge_by_level = new concurrent.TrieMap[String, concurrent.TrieMap[String, ListBuffer[CountData]]]


  // todo 定义一个抽象方法 processRule
  def processRule(indicatorResult: IndicatorResult,
                  alarmRuleType: AlarmRuleType,
                  alarmRuleLimit: AlarmRuleLimit,
                  ruleConfigKey: String,
                  w2wType: String,
                  indicatorLimitResult: IndicatorLimitResult,
                  shouldByPass: Boolean): Option[List[Option[Rule]]] = {

    logger.error(s"This is AlarmProcessRuleParent Parent Class")

    None
  }


  /**
   *   alarm job 计算W2W时增加recipe、product、stage配置
   */
  def getRuleKey(w2wType: String, indicatorResult: IndicatorResult): (String, ListBuffer[String]) = {

    val w2wKeyList: ListBuffer[String] = ListBuffer()

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

    if(w2wType.contains("Stage")){
      for(stageKey <- stageKeyList) {
        w2wKeyList.append(stageKey)
      }
    }else if(w2wType.contains("Product")){
      for (productKey <- productKeyList) {
        w2wKeyList.append(productKey)
      }
    }else{
      w2wKeyList.append(key)
    }

    var ruleKey = indicatorResult.toolName + "|" + chamberName
    if(w2wType.contains("Recipe")){
      ruleKey = ruleKey + "|" + recipeName
    }

    if(w2wType.contains("Product")){
      ruleKey = ruleKey + "|" + indicatorResult.product.head
    }

    if(w2wType.contains("Stage")){
      ruleKey = ruleKey + "|" + indicatorResult.stage.head
    }

    (ruleKey, w2wKeyList)
  }


  /**
   * 根据 notification和action 开关，过滤是否报警
   * @param shouldByPass
   * @param indicatorBypassCondition
   * @param alarmInfo
   * @return
   */
  def filterByPass(shouldByPass:Boolean,
                   indicatorBypassCondition:Option[ByPassCondition],
                   alarmRuleParamList:List[AlarmRuleParam]
                  ): List[AlarmRuleParam] ={
    //过滤过by pass 的 alarm info
    if(shouldByPass){
      alarmRuleParamList.map(alarmRuleParam=>
        AlarmRuleParam(
          index = alarmRuleParam.index,
          X = alarmRuleParam.X,
          N = alarmRuleParam.N,
          action = alarmRuleParam.action.filter((alarmRuleAction: AlarmRuleAction) =>{
            if(indicatorBypassCondition.nonEmpty){
              //过滤不符合条件的indicator BypassCondition
              val notificationSwitch= indicatorBypassCondition.get.notificationSwitch
              val actionSwitch = indicatorBypassCondition.get.actionSwitch
              if(notificationSwitch && actionSwitch) {
                //都关闭就全部过滤
                false
              }else if(notificationSwitch){
                //是否等于notification 等于就false过滤掉
                !alarmRuleAction.sign.getOrElse("").equals(MainFabConstants.notification)
              }else if(actionSwitch){
                //是否等于action 等于就false过滤掉
                !alarmRuleAction.sign.getOrElse("").equals(MainFabConstants.action)
              }else{
                //都没关
                true
              }
              //没有by pass 配置理论上不应该走到这里来。
            }else true
          }))
      )
    }else{
      //没有 ByPass 触发，都不过滤
      alarmRuleParamList
    }
  }


  /**
   * 1- 根据 notification和action 开关，过滤是否报警
   * 2- 组装 Rule 样例类
   * @param alarmRuleType
   * @param alarmRuleLimit
   * @param alarmRuleParamList
   * @param alarmLevel
   * @param shouldByPass
   * @param indicatorBypassCondition
   * @return
   */
  def getRule(alarmRuleType:AlarmRuleType,
              alarmRuleLimit:AlarmRuleLimit,
              alarmRuleParamList:List[AlarmRuleParam],
              alarmLevel:Int,
              shouldByPass:Boolean,
              indicatorBypassCondition:Option[ByPassCondition] ): Option[Rule] ={

    //todo 根据 notification和action 开关，过滤是否报警
    val byPassAlarmRuleParamList: List[AlarmRuleParam] = filterByPass(shouldByPass=shouldByPass,
      indicatorBypassCondition=indicatorBypassCondition,
      alarmRuleParamList = alarmRuleParamList)

    if(byPassAlarmRuleParamList.nonEmpty){
      // todo 组装 Rule 样例类
      Option(Rule(alarmLevel = alarmLevel,
        rule = alarmRuleType.ruleType,
        ruleTypeName = alarmRuleType.ruleTypeName,
        limit = alarmRuleLimit,
        alarmInfo = byPassAlarmRuleParamList))
    }else{
      None
    }
  }

  /**
   * 删除所有rule状态
   */
  def removeRuleData(key1: String): Unit = {
    this.countMap_Rule_1_2_6_merge_by_level.remove(key1)
    this.countMap_Rule_3_merge_by_level.remove(key1)
    this.countContinueIncreaseMap_Rule_4_5.remove(key1)

    logger.warn(s"removeRuleData key = ${key1}")
  }

}
