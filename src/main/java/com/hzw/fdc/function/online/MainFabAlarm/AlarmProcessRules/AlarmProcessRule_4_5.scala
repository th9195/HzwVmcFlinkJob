package com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules
import com.hzw.fdc.scalabean.{AlarmRuleLimit, AlarmRuleParam, AlarmRuleType, CountData,  IndicatorLimitResult, IndicatorResult, RULE_CONSTANT, Rule}
import com.hzw.fdc.util.ExceptionInfo
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 * AlarmProcessRule_4
 *
 * @desc:
 * @author tobytang
 * @date 2022/6/16 13:15
 * @since 1.0.0
 * @update 2022/6/16 13:15
 * */
class AlarmProcessRule_4_5 extends AlarmProcessRuleParent {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmProcessRule_4_5])

  override def processRule(
                            indicatorResult: IndicatorResult,
                            alarmRuleType: AlarmRuleType,
                            alarmRuleLimit: AlarmRuleLimit,
                            ruleConfigKey: String,
                            w2wType: String,
                            indicatorLimitResult: IndicatorLimitResult,
                            shouldByPass: Boolean): Option[List[Option[Rule]]] = {

//    logger.warn(s"----------- start processRule -----------")

    val ruleType = alarmRuleType.ruleType
    val ruleKeyTuple = getRuleKey(w2wType, indicatorResult)
    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +ruleType
    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +ruleType)

    // 收集所有的Rule
    val ruleList = ListBuffer[Option[Rule]]()

    try {

      // 获取 最新 indicatorValue
      val indicatorValue = indicatorResult.indicatorValue.toDouble

      // 新增key1 key2 并初始化当前值 为 最新的 indicatorValue
      val is_new = updateKeys_countContinueIncreaseMap_Rule_4_5(ruleConfigKey, ruleKey, indicatorValue)

      if(!is_new){

        // 获取 Rule4_5的配置信息(注意: 没有级别的概念，直接取USLorRule45 包含3个组的配置)
        val alarmRuleParamList = alarmRuleType.USLorRule45

        //收集单个级别的 AlarmRuleParam
        val singleLevelAlarmRuleParamList = ListBuffer[AlarmRuleParam]()

        //获取有效的 AlarmRuleParam
        val validAlarmRuleParamList = alarmRuleParamList.filter(alarmRuleParam => alarmRuleParam.X.nonEmpty)

        validAlarmRuleParamList.foreach(alarmRuleParam => {
          val actionIndex: Int = alarmRuleParam.index

          // todo 保证index [1,2,3]
          if(actionIndex < 1 || actionIndex > 3){
            logger.error(s"index = ${actionIndex} out of [1,3]")
            return None
          }
          // 获取当前组的 countData
          var countData: Option[CountData] = getCountData(ruleConfigKey, ruleKey, actionIndex)
          if(countData.nonEmpty){

            if(4 == ruleType && indicatorValue <= countData.get.getValue){
              // todo Rule 4(递增)  最新值小于等于当前值 清零
              // 清零 更新 当前值
              countData = updateCountData_4_5(ruleConfigKey,ruleKey,actionIndex,indicatorValue,true)
            }else if (5 == ruleType && indicatorValue >= countData.get.getValue){
              // todo Rule 5(递减)  最新值大于等于当前值 清零
              // 清零 更新 当前值
              countData = updateCountData_4_5(ruleConfigKey,ruleKey,actionIndex,indicatorValue,true)
            }else{
              // 累计 加1 不清零
              countData = updateCountData_4_5(ruleConfigKey,ruleKey,actionIndex,indicatorValue,false)

              // todo  如果 count >= X 记录告警
              if(countData.get.count >= alarmRuleParam.X.get){
                // 记录告警
                singleLevelAlarmRuleParamList.append(alarmRuleParam)
              }
            }
          }
        })

        if(singleLevelAlarmRuleParamList.nonEmpty){

          //获取报警信息
          val rule: Option[Rule] = getRule(
            alarmRuleType = alarmRuleType,
            alarmRuleLimit = alarmRuleLimit,
            alarmRuleParamList = singleLevelAlarmRuleParamList.toList,
            alarmLevel = indicatorLimitResult.alarmLevel ,
            shouldByPass=shouldByPass,
            indicatorBypassCondition = indicatorResult.bypassCondition)

          if(rule.nonEmpty){
            ruleList.append(rule)
          }
        }

      }
      // 返回告警列表
      Option(ruleList.toList)
    } catch {
      case ex: Exception => logger.warn(s" processRule $ruleType  error:${ExceptionInfo.getExceptionInfo(ex)} indicatorResult:$indicatorResult key1: $ruleConfigKey key2: $ruleKey alarmRuleType :$alarmRuleType")
        None
    }
  }


  /**
   * 新建CountData
   * 初始化 count = 0
   * 初始化 value = 0.0
   * @return
   */
  def initCountData(count:Int = 0 , indicatorValue:Double = 0.0):CountData = {
    CountData(count = count,value = indicatorValue)
  }

  /**
   * 初始化 CountData List 3 个
   * @param indicatorValue
   * @return
   */
  def initCountDataList(indicatorValue:Double = 0.0) = {
    val countDataList = new ListBuffer[CountData]()

    // 有三个组
    for(index <- Range(0,RULE_CONSTANT.RULE_INDEX_NUM)){
      countDataList.append(initCountData(indicatorValue = indicatorValue))
    }
    countDataList
  }



  /**
   * key1  key2  level  index 获取计数和当前值
   * @param key1
   * @param key2
   * @param level_index
   * @return
   */
  def getCountData(key1:String,key2:String,index:Int):Option[CountData] = {

    if( countContinueIncreaseMap_Rule_4_5.contains(key1) &&   // key1 存在
        countContinueIncreaseMap_Rule_4_5(key1).contains(key2) &&  // key2 存在
        countContinueIncreaseMap_Rule_4_5(key1)(key2).length >= index){ // 存储了当前组 index 的数据

      Option(countContinueIncreaseMap_Rule_4_5(key1)(key2)(index-1))

    }else{
      None
    }
  }


  /**
   * 根据key1 key2 index 更新CountData 中的计数 和 当前值
   * @param key1
   * @param key2
   * @param index
   * @param indicatorValue
   * @param isZero
   * @return
   */
  def updateCountData_4_5(key1:String,key2:String,index:Int,indicatorValue:Double,isZero:Boolean=false) = {
    if( countContinueIncreaseMap_Rule_4_5.contains(key1) &&   // key1 存在
      countContinueIncreaseMap_Rule_4_5(key1).contains(key2) ){

      if(countContinueIncreaseMap_Rule_4_5(key1)(key2).length >= index){ // 存储了当前组 index 的数据

        val countContinueIncreaseMapKey1_Rule_4_5 = countContinueIncreaseMap_Rule_4_5(key1)
        val countDataList: ListBuffer[CountData] = countContinueIncreaseMapKey1_Rule_4_5(key2)

        // 取出对应的 countData
        val countData: CountData = countDataList(index - 1)

        if (isZero){
          countData.setCount(0)  // todo 清零
        }else{
          countData.setCount(countData.getCount + 1) // todo 累计 + 1
        }

        countData.setValue(indicatorValue)  // todo  更新当前值

        // 将更新后的值 放回 缓存中
        countDataList(index - 1) = countData
        countContinueIncreaseMapKey1_Rule_4_5.put(key2,countDataList)
        countContinueIncreaseMap_Rule_4_5.put(key1,countContinueIncreaseMapKey1_Rule_4_5)

        Option(countData)
      }else {
        logger.error(s"updateCountData_4_5 : key1=${key1},key2=${key2},index")
        None
      }

    }else{
      logger.error(s"updateCountData_4_5 : key1=${key1},key2=${key2},index=${index}")
      None
    }
  }


  /**
   * 更新(新增) countContinueIncreaseMap_Rule_4_5 的  key1 key2  并初始化当前值
   * @param key1
   * @param key2
   * @return  is_new :  如何没有记录 key1 + key2 说明没有记录这个组合的数据
   */
  def updateKeys_countContinueIncreaseMap_Rule_4_5(key1:String,key2:String,indicatorValue:Double) = {
    var is_new = false
    // todo : 新增不存在的key1  key2
    if(!countContinueIncreaseMap_Rule_4_5.contains(key1)){

      // todo 1 : key1 不存在
      val countContinueIncreaseMapKey1_Rule_4_5 = new TrieMap[String, ListBuffer[CountData]]()
      countContinueIncreaseMapKey1_Rule_4_5.put(key2,initCountDataList(indicatorValue = indicatorValue))
      countContinueIncreaseMap_Rule_4_5.put(key1,countContinueIncreaseMapKey1_Rule_4_5)
      is_new = true
    }else{

      // todo 2 : key1 存在
      val countContinueIncreaseMapKey1_Rule_4_5 = countContinueIncreaseMap_Rule_4_5(key1)

      if (!countContinueIncreaseMapKey1_Rule_4_5.contains(key2)){

        // todo 3 : key2 不存在
        countContinueIncreaseMapKey1_Rule_4_5.put(key2,initCountDataList(indicatorValue = indicatorValue))
        countContinueIncreaseMap_Rule_4_5.put(key1,countContinueIncreaseMapKey1_Rule_4_5)

        is_new = true
      }
    }

    is_new
  }

  /**
   * 清空缓存
   * @param key1
   */
  override def removeRuleData(key1: String): Unit = {
    this.countContinueIncreaseMap_Rule_4_5.remove(key1)
    logger.warn(s"countContinueIncreaseMap_Rule_4_5 removeRuleData key = ${key1}")
  }

}
