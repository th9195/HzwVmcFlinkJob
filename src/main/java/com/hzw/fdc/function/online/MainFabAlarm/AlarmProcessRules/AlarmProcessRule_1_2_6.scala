package com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules

import com.hzw.fdc.scalabean.{AlarmRuleLimit, AlarmRuleParam, AlarmRuleType, ByPassCondition, Count_1_2_6, Count_1_2_6_merge_by_level, IndicatorLimitResult, IndicatorResult, RULE_CONSTANT, Rule}
import com.hzw.fdc.util.ExceptionInfo
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.mutable.Map
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 * AlarmProcessRule_1
 *
 * @desc:
 * @author tobytang
 * @date 2022/6/16 11:20
 * @since 1.0.0
 * @update 2022/6/16 11:20
 * */
class AlarmProcessRule_1_2_6 extends AlarmProcessRuleParent {

  val logger: Logger = LoggerFactory.getLogger(classOf[AlarmProcessRule_1_2_6])

  override def processRule(
                            indicatorResult: IndicatorResult,
                            alarmRuleType: AlarmRuleType,
                            alarmRuleLimit: AlarmRuleLimit,
                            ruleConfigKey: String,
                            w2wType: String,
                            indicatorLimitResult: IndicatorLimitResult,
                            shouldByPass: Boolean): Option[List[Option[Rule]]] = {

//    logger.warn(s"----------- start processRule -----------")

    // 第几套规则
    val ruleType = alarmRuleType.ruleType

    val oocLevel = indicatorLimitResult.oocLevel

    val ruleKeyTuple: (String, ListBuffer[String]) = getRuleKey(w2wType, indicatorResult)

    // countMap_Rule_1数据结构的 key2
    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +ruleType

    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +ruleType)

    try {

      // 更新 countMap_Rule_1_2_6 如果 key1 key2 不存在需要添加进去
//      updateKeys_countMap_Rule_1_2_6(ruleConfigKey,ruleKey)
      updateKeys_countMap_Rule_1_2_6_merge_by_level(ruleConfigKey,ruleKey)

      //根据alarmLevel 6 个级别的告警 分别更新对应的计数或者清零
//      update_all_count(indicatorLimitResult,ruleType,ruleConfigKey,ruleKey)
      update_all_count_merge_by_level(indicatorLimitResult,ruleType,ruleConfigKey,ruleKey)


//      val resultRuleList: Option[List[Option[Rule]]] = judgeRule(
//        alarmRuleType = alarmRuleType,
//        alarmRuleLimit = alarmRuleLimit,
//        ruleConfigKey = ruleConfigKey,
//        ruleKey = ruleKey,
//        w2wKeyList = w2wKeyList,
//        shouldByPass = shouldByPass,
//        indicatorBypassCondition = indicatorResult.bypassCondition)

      val resultRuleList: Option[List[Option[Rule]]] = judgeRule_merge_by_level(
        alarmRuleType = alarmRuleType,
        alarmRuleLimit = alarmRuleLimit,
        ruleConfigKey = ruleConfigKey,
        ruleKey = ruleKey,
        w2wKeyList = w2wKeyList,
        shouldByPass = shouldByPass,
        indicatorBypassCondition = indicatorResult.bypassCondition,
        oocLevel)

      resultRuleList

    } catch {
      case ex: Exception => logger.warn(s" ruleType $ruleType  error:${ExceptionInfo.getExceptionInfo(ex)} indicatorResult:$indicatorResult key1: $ruleConfigKey key2: $ruleKey alarmRuleType :$alarmRuleType")
        None
    }

  }

  /**
   * 更新(新增) countMap_Rule_1_2_6  key1 key2
   * @param level_index
   * @param key1
   * @param key2
   * @return
   */
  def updateKeys_countMap_Rule_1_2_6(key1:String,key2:String):Unit = {

    // todo 0 : 新增不存在的key1  key2
    if(!countMap_Rule_1_2_6.contains(key1)){

      // todo 1 : key1 不存在
      // 新建计数样例类并存入Map
      val countMapKey1_Rule_1_2_6 = new TrieMap[String, Count_1_2_6]()
      countMapKey1_Rule_1_2_6.put(key2,initCount_1_2_6())
      countMap_Rule_1_2_6.put(key1,countMapKey1_Rule_1_2_6)

    }else{
      // todo 2 : key1 存在
      val countMapKey1_Rule_1_2_6 = countMap_Rule_1_2_6(key1)

      if (!countMapKey1_Rule_1_2_6.contains(key2)){

        // todo 3 : key2 不存在
        countMapKey1_Rule_1_2_6.put(key2,initCount_1_2_6())
        countMap_Rule_1_2_6.put(key1,countMapKey1_Rule_1_2_6)
      }
    }

  }


  /**
   * 更新(新增) countMap_Rule_1_2_6  key1 key2  合并相同级别的计数
   * @param level_index
   * @param key1
   * @param key2
   * @return
   */
  def updateKeys_countMap_Rule_1_2_6_merge_by_level(key1:String,key2:String):Unit = {

    // todo 0 : 新增不存在的key1  key2
    if(!countMap_Rule_1_2_6_merge_by_level.contains(key1)){

      // todo 1 : key1 不存在
      // 新建计数样例类并存入Map
      val countMapKey1_Rule_1_2_6 = new TrieMap[String, Count_1_2_6_merge_by_level]()
      countMapKey1_Rule_1_2_6.put(key2,initCount_1_2_6_merge_by_level())
      countMap_Rule_1_2_6_merge_by_level.put(key1,countMapKey1_Rule_1_2_6)

    }else{
      // todo 2 : key1 存在
      val countMapKey1_Rule_1_2_6 = countMap_Rule_1_2_6_merge_by_level(key1)

      if (!countMapKey1_Rule_1_2_6.contains(key2)){

        // todo 3 : key2 不存在
        countMapKey1_Rule_1_2_6.put(key2,initCount_1_2_6_merge_by_level())
        countMap_Rule_1_2_6_merge_by_level.put(key1,countMapKey1_Rule_1_2_6)
      }
    }

  }


  /**
   * 根据告警级别 更新统计 计数
   * @param indicatorLimitResult
   * @param ruleType
   * @param ruleConfigKey
   * @param ruleKey
   */
  def update_all_count(indicatorLimitResult: IndicatorLimitResult,ruleType:Int,ruleConfigKey:String,ruleKey:String) = {
    if (indicatorLimitResult.alarmLevel.equals(3)) {
      // alarmLevel == 3
      //todo UP  S-B-C需要累加  1-2-3 组
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL")
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL")
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL")

      // todo 如果是第六套规则 LCL,LBL,LSL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL",true)
      }

    } else if (indicatorLimitResult.alarmLevel.equals(-3)) {
      // alarmLevel == -3
      //todo LOW  S-B-C需要累加
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL")
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL")
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL")

      // todo 如果是第六套规则,USL,UBL,UCL,LCL,LBL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL",true)
      }

    } else if (indicatorLimitResult.alarmLevel.equals(2)) {
      // alarmLevel == 2
      //todo UP  B-C需要累加
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL")
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL")

      // todo 如果是第六套规则  USL,LCL,LBL,LSL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL",true)
      }

    } else if (indicatorLimitResult.alarmLevel.equals(-2)) {
      // alarmLevel == -2
      //todo LOW  B-C需要累加
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL")
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL")

      // todo 如果是第六套规则  USL,UBL,UCL,LSL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL",true)

      }

    } else if (indicatorLimitResult.alarmLevel.equals(1)) {
      // alarmLevel == 1
      //todo UP  C需要累加
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL")

      // todo 如果是第六套规则 USL,UBL,LCL,LBL,LSL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL",true)
      }

    } else if (indicatorLimitResult.alarmLevel.equals(-1)) {
      // alarmLevel == -1
      //todo LOW  C需要累加
      update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL")
      // todo 如果是第六套规则 USL,UBL,LCL,LBL,LSL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL",true)
      }

    } else {
      // alarmLevel == 0
      // todo 没有累加
      // todo 如果是第六套规则 USL,UBL,LCL,LCL,LBL,LSL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"USL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"UCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LCL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LBL",true)
        update_count_1_2_6_by_level(ruleType,ruleConfigKey,ruleKey,"LSL",true)
      }

    }
  }
  /**
   *
   * @param indicatorLimitResult
   * @param ruleType
   * @param ruleConfigKey
   * @param ruleKey
   */
  def update_all_count_merge_by_level(indicatorLimitResult: IndicatorLimitResult,ruleType:Int,ruleConfigKey:String,ruleKey:String) = {
    // todo alarmLevel == 3 或者 -3
    if (indicatorLimitResult.alarmLevel.equals(3) || indicatorLimitResult.alarmLevel.equals(-3)) {

      //todo UP  S-B-C需要累加  1-2-3 组
      update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"SL")
      update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"BL")
      update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"CL")

    }else if (indicatorLimitResult.alarmLevel.equals(2) || indicatorLimitResult.alarmLevel.equals(-2)){
      // todo alarmLevel == 2 或者 -2
      //todo UP  B-C需要累加
      update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"BL")
      update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"CL")

      // todo 如果是第六套规则  SL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"SL",true)
      }

    }else if (indicatorLimitResult.alarmLevel.equals(1) || indicatorLimitResult.alarmLevel.equals(-1)) {
      // todo alarmLevel == 1 或者 -1
      //todo UP  C需要累加
      update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"CL")

      // todo 如果是第六套规则 SL,BL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"BL",true)
        update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"SL",true)
      }
    }else {
      // todo alarmLevel == 0
      // todo 没有累加
      // todo 如果是第六套规则 SL,BL,CL 需要清零
      if (6 == ruleType){
        update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"CL",true)
        update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"BL",true)
        update_count_1_2_6_merge_by_level(ruleType,ruleConfigKey,ruleKey,"SL",true)
      }
    }
  }


  /**
   * 判断3套配置 返回是否报警
   * @param increaseMapData
   * @param rule
   * @param limit
   * @param ruleConfigKey
   * @param ruleKey
   * @param isRuleConfigNew
   * @param isRuleNew
   * @param w2wKeyList
   * @return
   */
  def judgeRule(alarmRuleType:AlarmRuleType,
                alarmRuleLimit:AlarmRuleLimit,
                ruleConfigKey: String,
                ruleKey:String,
                w2wKeyList: ListBuffer[String],
                shouldByPass:Boolean,
                indicatorBypassCondition:Option[ByPassCondition]): Option[List[Option[Rule]]] = {


    // 收集所有的Rule
    val ruleList = ListBuffer[Option[Rule]]()

    // 获取 第几套规则
    val ruleType = alarmRuleType.ruleType

    // todo  遍历告警级别 [-3,-2,-1,1,2,3]
    for(alarm_level <- Range(-3,4)  if alarm_level != 0){

      // 根据告警级别 选择 不同级别的 AlarmRuleParamList 和 级别名称 (USL,UBL,......)
      val alarmRuleParamList_and_level: (List[AlarmRuleParam], String, Option[Double]) = alarm_level match {
        case 3 => (alarmRuleType.USLorRule45, RULE_CONSTANT.USL,alarmRuleLimit.USL)
        case 2 => (alarmRuleType.UBL, RULE_CONSTANT.UBL,alarmRuleLimit.UBL)
        case 1 => (alarmRuleType.UCL, RULE_CONSTANT.UCL,alarmRuleLimit.UCL)
        case -1 => (alarmRuleType.LCL, RULE_CONSTANT.LCL,alarmRuleLimit.LCL)
        case -2 => (alarmRuleType.LBL, RULE_CONSTANT.LBL,alarmRuleLimit.LBL)
        case -3 => (alarmRuleType.LSL, RULE_CONSTANT.LSL,alarmRuleLimit.LSL)
      }

      val alarmRuleParamList: List[AlarmRuleParam] = alarmRuleParamList_and_level._1
      val alarm_level_str: String = alarmRuleParamList_and_level._2
      val currentLevelLimitValue = alarmRuleParamList_and_level._3


      // 计算单个级别 告警
      val singleLevelAlarmRuleParamList: List[AlarmRuleParam] = calculateAlarm(ruleType = ruleType,
        key1 = ruleConfigKey,
        key2 = ruleKey,
        alarmRuleParamList = alarmRuleParamList,
        alarm_level_str = alarm_level_str)

      if(singleLevelAlarmRuleParamList.nonEmpty){
        // 组装报警信息
        val rule: Option[Rule] = getRule(
          alarmRuleType = alarmRuleType,
          alarmRuleLimit = alarmRuleLimit,
          alarmRuleParamList = singleLevelAlarmRuleParamList,
          alarmLevel = alarm_level,
          shouldByPass=shouldByPass,
          indicatorBypassCondition = indicatorBypassCondition)

        //todo 收集所有正常的Rule : 必须当前级别的 Limit 有值
        if(rule.nonEmpty && currentLevelLimitValue.nonEmpty){
          ruleList.append(rule)
        }
      }
    }

    if(ruleList.nonEmpty){
      Option(ruleList.toList)
    }else{
      None
    }
  }

  /**
   * 判断3套配置 返回是否报警   合并相同级别统计计数
   * @param increaseMapData
   * @param rule
   * @param limit
   * @param ruleConfigKey
   * @param ruleKey
   * @param isRuleConfigNew
   * @param isRuleNew
   * @param w2wKeyList
   * @return
   */
  def judgeRule_merge_by_level(alarmRuleType:AlarmRuleType,
                              alarmRuleLimit:AlarmRuleLimit,
                              ruleConfigKey: String,
                              ruleKey:String,
                              w2wKeyList: ListBuffer[String],
                              shouldByPass:Boolean,
                              indicatorBypassCondition:Option[ByPassCondition],
                              oocLevel:Int): Option[List[Option[Rule]]] = {


    // 收集所有的Rule
    val ruleList = ListBuffer[Option[Rule]]()

    // 获取 第几套规则
    val ruleType = alarmRuleType.ruleType

    // todo  遍历告警级别 [-3,-2,-1,1,2,3]
    var startIndex = -3
    var endIndex = 4
    if(oocLevel > 0 && oocLevel < 4){
      startIndex = 1
    }else if (oocLevel < 0 && oocLevel > -4){
      endIndex = 0
    }else{
      startIndex = 0
      endIndex = 0
    }
    for(alarm_level <- Range(startIndex,endIndex)  if alarm_level != 0){

      // 根据告警级别 选择 不同级别的 AlarmRuleParamList 和 级别名称 (USL,UBL,......)
      val alarmRuleParamList_and_level: (List[AlarmRuleParam], String, Option[Double]) = alarm_level match {
        case 3 => (alarmRuleType.USLorRule45, RULE_CONSTANT.SL,alarmRuleLimit.USL)
        case 2 => (alarmRuleType.UBL, RULE_CONSTANT.BL,alarmRuleLimit.UBL)
        case 1 => (alarmRuleType.UCL, RULE_CONSTANT.CL,alarmRuleLimit.UCL)
        case -1 => (alarmRuleType.LCL, RULE_CONSTANT.CL,alarmRuleLimit.LCL)
        case -2 => (alarmRuleType.LBL, RULE_CONSTANT.BL,alarmRuleLimit.LBL)
        case -3 => (alarmRuleType.LSL, RULE_CONSTANT.SL,alarmRuleLimit.LSL)
      }

      val alarmRuleParamList: List[AlarmRuleParam] = alarmRuleParamList_and_level._1
      val alarm_level_str: String = alarmRuleParamList_and_level._2
      val currentLevelLimitValue = alarmRuleParamList_and_level._3


      // 计算单个级别 告警
      val singleLevelAlarmRuleParamList: List[AlarmRuleParam] = calculateAlarm_merge_by_level(ruleType = ruleType,
        key1 = ruleConfigKey,
        key2 = ruleKey,
        alarmRuleParamList = alarmRuleParamList,
        alarm_level_str = alarm_level_str)

      if(singleLevelAlarmRuleParamList.nonEmpty){
        // 组装报警信息
        val rule: Option[Rule] = getRule(
          alarmRuleType = alarmRuleType,
          alarmRuleLimit = alarmRuleLimit,
          alarmRuleParamList = singleLevelAlarmRuleParamList,
          alarmLevel = alarm_level,
          shouldByPass=shouldByPass,
          indicatorBypassCondition = indicatorBypassCondition)

        //todo 收集所有正常的Rule : 必须当前级别的 Limit 有值
        if(rule.nonEmpty && currentLevelLimitValue.nonEmpty){
          ruleList.append(rule)
        }
      }
    }

    if(ruleList.nonEmpty){
      Option(ruleList.toList)
    }else{
      None
    }
  }


  /**
   * 判断是否告警： count > X
   * @param ruleType
   * @param key1
   * @param key2
   * @param alarmRuleParamList
   * @param rule_level
   * @return
   */
  def calculateAlarm(ruleType:Int,
                     key1:String,
                     key2:String,
                     alarmRuleParamList:List[AlarmRuleParam],
                     alarm_level_str:String) : List[AlarmRuleParam] = {

    // 过滤出有效的 AlarmRuleParam 配置数据
    val validAlarmRuleParamList: List[AlarmRuleParam] = alarmRuleParamList.filter(x => x.X.nonEmpty)

    // 收集符合条件的 AlarmRuleParam
    val singleLevelAlarmRuleParamList = ListBuffer[AlarmRuleParam]()

    validAlarmRuleParamList.foreach((alarmRuleParam: AlarmRuleParam) => {

      // 获取 组号
      val index: Int = alarmRuleParam.index

      // 获取 级别_组号
      val level_index = alarm_level_str + '_' + index.toString

      // 获取当前组合 累计计数
      val count = getOneCount_1_2_6(key1 = key1,key2 = key2,level_index = level_index)


      // todo 判断该组合的累计计数是否超过了配置数，如果超过生成告警数据;
      if (count.nonEmpty && count.get >= alarmRuleParam.X.get) {
        // 生成告警数据
        singleLevelAlarmRuleParamList.append(alarmRuleParam)

        // todo rule1,rule2生成了告警就需要清零,但是rule6 不需要清零
        if(6 != ruleType){
          update_count_1_2_6_by_level_index(key1,key2,level_index,true)
        }
      }
    })

    singleLevelAlarmRuleParamList.toList
  }


  /**
   * 判断是否告警： count > X  合并相同级别的统计计数
   * @param ruleType
   * @param key1
   * @param key2
   * @param alarmRuleParamList
   * @param rule_level
   * @return
   */
  def calculateAlarm_merge_by_level(ruleType:Int,
                     key1:String,
                     key2:String,
                     alarmRuleParamList:List[AlarmRuleParam],
                     alarm_level_str:String) : List[AlarmRuleParam] = {

    // 过滤出有效的 AlarmRuleParam 配置数据
    val validAlarmRuleParamList: List[AlarmRuleParam] = alarmRuleParamList.filter(x => x.X.nonEmpty)

    // 收集符合条件的 AlarmRuleParam
    val singleLevelAlarmRuleParamList = ListBuffer[AlarmRuleParam]()

    validAlarmRuleParamList.foreach((alarmRuleParam: AlarmRuleParam) => {

      // 获取 组号
      val index: Int = alarmRuleParam.index

      // 获取 级别_组号
      val level_index = alarm_level_str + '_' + index.toString

      // 获取当前组合 累计计数
      val count = getOneCount_1_2_6_merge_by_level(key1 = key1,key2 = key2,level_index = level_index)

      // todo 判断该组合的累计计数是否超过了配置数，如果超过生成告警数据;
      if (count.nonEmpty && count.get >= alarmRuleParam.X.get) {
        // 生成告警数据
        singleLevelAlarmRuleParamList.append(alarmRuleParam)

        // todo rule1,rule2生成了告警就需要清零,但是rule6 不需要清零
        if(6 != ruleType){
          update_count_1_2_6_merge_by_level_index(key1,key2,level_index,true)
        }
      }
    })

    singleLevelAlarmRuleParamList.toList
  }



  /**
   * 创建对象 Count_1_2_6
   */
  def initCount_1_2_6() = {
    Count_1_2_6(
      USL_1 = 0,
      UBL_1 = 0,
      UCL_1 = 0,
      LCL_1 = 0,
      LBL_1 = 0,
      LSL_1 = 0,
      USL_2 = 0,
      UBL_2 = 0,
      UCL_2 = 0,
      LCL_2 = 0,
      LBL_2 = 0,
      LSL_2 = 0,
      USL_3 = 0,
      UBL_3 = 0,
      UCL_3 = 0,
      LCL_3 = 0,
      LBL_3 = 0,
      LSL_3 = 0
    )
  }

  /**
   * 创建对象 Count_1_2_6 合并相同级别的计数
   */
  def initCount_1_2_6_merge_by_level() = {
    Count_1_2_6_merge_by_level(
      CL_1 = 0,
      BL_1 = 0,
      SL_1 = 0,
      CL_2 = 0,
      BL_2 = 0,
      SL_2 = 0,
      CL_3 = 0,
      BL_3 = 0,
      SL_3 = 0
    )
  }

  /**
   * 同一个级别的 3组 计数都需要更新
   * @param ruleType
   * @param key1
   * @param key2
   * @param level
   * @param isZero
   */
  def update_count_1_2_6_by_level(ruleType:Int,key1:String,key2:String,level:String,isZero:Boolean=false)= {

    // 如果是第一套规则  没有分组的概念
    val indexNums = if(1==ruleType) 1 else RULE_CONSTANT.RULE_INDEX_NUM

    for(index <- Range(1,indexNums+1)){
      // 生成组合 eg: USL_1
      val level_index = level + '_' + index.toString
      update_count_1_2_6_by_level_index(key1,key2,level_index ,isZero)
    }
  }


  /**
   * 同一个级别的 3组 计数都需要更新  合并相同级别的统计计数
   * @param ruleType
   * @param key1
   * @param key2
   * @param level
   * @param isZero
   */
  def update_count_1_2_6_merge_by_level(ruleType:Int,key1:String,key2:String,level:String,isZero:Boolean=false)= {

    // 如果是第一套规则  没有分组的概念
    val indexNums = if(1==ruleType) 1 else RULE_CONSTANT.RULE_INDEX_NUM

    for(index <- Range(1,indexNums+1)){
      // 生成组合 eg: USL_1
      val level_index = level + '_' + index.toString
      update_count_1_2_6_merge_by_level_index(key1,key2,level_index ,isZero)
    }
  }


  /**
   * 更新计数 或 清零
   * @param key1
   * @param key2
   * @param level_index
   */
  def update_count_1_2_6_by_level_index(key1:String,key2:String,level_index:String,isZero:Boolean=false) = {
    // todo 0 : 只有key1 key2 存在才能更新
    if (countMap_Rule_1_2_6.contains(key1)){
      val countMapKey1_Rule_1_2_6 = countMap_Rule_1_2_6(key1)

      if(countMapKey1_Rule_1_2_6.contains(key2)){

        val count_1_2_6: Count_1_2_6 = countMapKey1_Rule_1_2_6(key2)

        level_index match {
          //------------------------第一组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_USL_1 => {
            if(isZero){
              count_1_2_6.setUSL_1(0)
            }else {
              count_1_2_6.setUSL_1(count_1_2_6.getUSL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_UBL_1 => {
            if(isZero){
              count_1_2_6.setUBL_1(0)
            }else {
              count_1_2_6.setUBL_1(count_1_2_6.getUBL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_UCL_1 => {
            if(isZero){
              count_1_2_6.setUCL_1(0)
            }else {
              count_1_2_6.setUCL_1(count_1_2_6.getUCL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LCL_1 => {
            if(isZero){
              count_1_2_6.setLCL_1(0)
            }else {
              count_1_2_6.setLCL_1(count_1_2_6.getLCL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LBL_1 => {
            if(isZero){
              count_1_2_6.setLBL_1(0)
            }else {
              count_1_2_6.setLBL_1(count_1_2_6.getLBL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LSL_1 => {
            if(isZero){
              count_1_2_6.setLSL_1(0)
            }else {
              count_1_2_6.setLSL_1(count_1_2_6.getLSL_1 + 1)
            }
          }

          //------------------------第二组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_USL_2 => {
            if(isZero){
              count_1_2_6.setUSL_2(0)
            }else {
              count_1_2_6.setUSL_2(count_1_2_6.getUSL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_UBL_2 => {
            if(isZero){
              count_1_2_6.setUBL_2(0)
            }else {
              count_1_2_6.setUBL_2(count_1_2_6.getUBL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_UCL_2 => {
            if(isZero){
              count_1_2_6.setUCL_2(0)
            }else {
              count_1_2_6.setUCL_2(count_1_2_6.getUCL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LCL_2 => {
            if(isZero){
              count_1_2_6.setLCL_2(0)
            }else {
              count_1_2_6.setLCL_2(count_1_2_6.getLCL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LBL_2 => {
            if(isZero){
              count_1_2_6.setLBL_2(0)
            }else {
              count_1_2_6.setLBL_2(count_1_2_6.getLBL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LSL_2 => {
            if(isZero){
              count_1_2_6.setLSL_2(0)
            }else {
              count_1_2_6.setLSL_2(count_1_2_6.getLSL_2 + 1)
            }
          }

          //------------------------第三组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_USL_3 => {
            if(isZero){
              count_1_2_6.setUSL_3(0)
            }else {
              count_1_2_6.setUSL_3(count_1_2_6.getUSL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_UBL_3 => {
            if(isZero){
              count_1_2_6.setUBL_3(0)
            }else {
              count_1_2_6.setUBL_3(count_1_2_6.getUBL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_UCL_3 => {
            if(isZero){
              count_1_2_6.setUCL_3(0)
            }else {
              count_1_2_6.setUCL_3(count_1_2_6.getUCL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LCL_3 => {
            if(isZero){
              count_1_2_6.setLCL_3(0)
            }else {
              count_1_2_6.setLCL_3(count_1_2_6.getLCL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LBL_3 => {
            if(isZero){
              count_1_2_6.setLBL_3(0)
            }else {
              count_1_2_6.setLBL_3(count_1_2_6.getLBL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_LSL_3 => {
            if(isZero){
              count_1_2_6.setLSL_3(0)
            }else {
              count_1_2_6.setLSL_3(count_1_2_6.getLSL_3 + 1)
            }
          }
        }

        // 修改完后需要在存入缓存
        countMapKey1_Rule_1_2_6.put(key2,count_1_2_6)
        countMap_Rule_1_2_6.put(key1,countMapKey1_Rule_1_2_6)
      }
    }
  }

  /**
   * 更新计数 或 清零  合并相同级别的统计计数
   * @param key1
   * @param key2
   * @param level_index
   */
  def update_count_1_2_6_merge_by_level_index(key1:String,key2:String,level_index:String,isZero:Boolean=false) = {
    // todo 0 : 只有key1 key2 存在才能更新
    if (countMap_Rule_1_2_6_merge_by_level.contains(key1)){
      val countMapKey1_Rule_1_2_6 = countMap_Rule_1_2_6_merge_by_level(key1)

      if(countMapKey1_Rule_1_2_6.contains(key2)){

        val count_1_2_6: Count_1_2_6_merge_by_level = countMapKey1_Rule_1_2_6(key2)

        level_index match {
          //------------------------第一组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_1 => {
            if(isZero){
              count_1_2_6.setCL_1(0)
            }else {
              count_1_2_6.setCL_1(count_1_2_6.getCL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_1 => {
            if(isZero){
              count_1_2_6.setBL_1(0)
            }else {
              count_1_2_6.setBL_1(count_1_2_6.getBL_1 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_1 => {
            if(isZero){
              count_1_2_6.setSL_1(0)
            }else {
              count_1_2_6.setSL_1(count_1_2_6.getSL_1 + 1)
            }
          }

          //------------------------第二组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_2 => {
            if(isZero){
              count_1_2_6.setCL_2(0)
            }else {
              count_1_2_6.setCL_2(count_1_2_6.getCL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_2 => {
            if(isZero){
              count_1_2_6.setBL_2(0)
            }else {
              count_1_2_6.setBL_2(count_1_2_6.getBL_2 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_2 => {
            if(isZero){
              count_1_2_6.setSL_2(0)
            }else {
              count_1_2_6.setSL_2(count_1_2_6.getSL_2 + 1)
            }
          }

          //------------------------第三组--------------------

          case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_3 => {
            if(isZero){
              count_1_2_6.setCL_3(0)
            }else {
              count_1_2_6.setCL_3(count_1_2_6.getCL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_3 => {
            if(isZero){
              count_1_2_6.setBL_3(0)
            }else {
              count_1_2_6.setBL_3(count_1_2_6.getBL_3 + 1)
            }
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_3 => {
            if(isZero){
              count_1_2_6.setSL_3(0)
            }else {
              count_1_2_6.setSL_3(count_1_2_6.getSL_3 + 1)
            }
          }
        }

        // 修改完后需要在存入缓存
        countMapKey1_Rule_1_2_6.put(key2,count_1_2_6)
        countMap_Rule_1_2_6_merge_by_level.put(key1,countMapKey1_Rule_1_2_6)
      }
    }
  }


  /**
   * 根据 key1  key2  level  index 获取计数
   * @param key1
   * @param key2
   * @param level
   * @param index
   * @return
   */
  def getOneCount_1_2_6(key1:String,key2:String,level_index:String) = {

    if(countMap_Rule_1_2_6.contains(key1) && countMap_Rule_1_2_6(key1).contains(key2)){
      val count_1_2_6: Count_1_2_6 = countMap_Rule_1_2_6(key1)(key2)
      level_index match {
        //------------------------第一组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_USL_1 => Option(count_1_2_6.getUSL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_UBL_1 => Option(count_1_2_6.getUBL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_UCL_1 => Option(count_1_2_6.getUCL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LCL_1 => Option(count_1_2_6.getLCL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LBL_1 => Option(count_1_2_6.getLBL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LSL_1 => Option(count_1_2_6.getLSL_1)

        //------------------------第二组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_USL_2 => Option(count_1_2_6.getUSL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_UBL_2 => Option(count_1_2_6.getUBL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_UCL_2 => Option(count_1_2_6.getUCL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LCL_2 => Option(count_1_2_6.getLCL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LBL_2 => Option(count_1_2_6.getLBL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LSL_2 => Option(count_1_2_6.getLSL_2)

        //------------------------第三组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_USL_3 => Option(count_1_2_6.getUSL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_UBL_3 => Option(count_1_2_6.getUBL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_UCL_3 => Option(count_1_2_6.getUCL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LCL_3 => Option(count_1_2_6.getLCL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LBL_3 => Option(count_1_2_6.getLBL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_LSL_3 => Option(count_1_2_6.getLSL_3)

      }
    }else{
      None
    }

  }



  /**
   * 根据 key1  key2  level  index 获取计数   合并相同级别的统计计数
   * @param key1
   * @param key2
   * @param level
   * @param index
   * @return
   */
  def getOneCount_1_2_6_merge_by_level(key1:String,key2:String,level_index:String) = {

    if(countMap_Rule_1_2_6_merge_by_level.contains(key1) && countMap_Rule_1_2_6_merge_by_level(key1).contains(key2)){
      val count_1_2_6: Count_1_2_6_merge_by_level = countMap_Rule_1_2_6_merge_by_level(key1)(key2)
      level_index match {
        //------------------------第一组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_1 => Option(count_1_2_6.getCL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_1 => Option(count_1_2_6.getBL_1)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_1 => Option(count_1_2_6.getSL_1)

        //------------------------第二组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_2 => Option(count_1_2_6.getCL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_2 => Option(count_1_2_6.getBL_2)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_2 => Option(count_1_2_6.getSL_2)

        //------------------------第三组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_3 => Option(count_1_2_6.getCL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_3 => Option(count_1_2_6.getBL_3)
        case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_3 => Option(count_1_2_6.getSL_3)

      }
    }else{
      None
    }

  }

  /**
   * 清空缓存
   * @param key1
   */
  override def removeRuleData(key1: String): Unit = {
    this.countMap_Rule_1_2_6_merge_by_level.remove(key1)
    logger.warn(s"countMap_Rule_1_2_6_merge_by_level removeRuleData key = ${key1}")
  }




}
