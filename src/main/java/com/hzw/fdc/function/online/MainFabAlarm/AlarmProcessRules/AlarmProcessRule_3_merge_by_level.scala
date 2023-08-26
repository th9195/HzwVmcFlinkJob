package com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * AlarmProcessRule_3
 *
 * @desc:
 * @author tobytang
 * @date 2022/6/16 13:09
 * @since 1.0.0
 * @update 2022/6/16 13:09
 * */
class AlarmProcessRule_3_merge_by_level extends AlarmProcessRuleParent {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmProcessRule_3_merge_by_level])
  var Max_N: Int = ProjectConfig.ALARM_RULE_3_MAX_N.toInt


  override def processRule(
                            indicatorResult: IndicatorResult,
                            alarmRuleType: AlarmRuleType,
                            alarmRuleLimit: AlarmRuleLimit,
                            ruleConfigKey: String,
                            w2wType: String,
                            indicatorLimitResult: IndicatorLimitResult,
                            shouldByPass: Boolean): Option[List[Option[Rule]]] = {

//    logger.error(s"----------- AlarmProcessRule_3_merge_by_level start processRule -----------")

    val ruleType = alarmRuleType.ruleType

    val ruleKeyTuple = getRuleKey(w2wType, indicatorResult)
    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +ruleType
    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +ruleType)

    try {

      // todo 取出最大的 N
      getMax_N(alarmRuleType)
      if(Max_N > 0 && Max_N <= ProjectConfig.ALARM_RULE_3_MAX_N){

        //取出生效的indicatorValue 值
        val oocLevel = indicatorLimitResult.oocLevel

        // todo 更新 countMap_Rule_3 如果 key1 key2 不存在需要添加进去
        updateKeys_countMap_Rule_3_merge_by_level(ruleConfigKey,ruleKey)

        // todo 更新所有计数信息
        update_all_count_merge_by_level(oocLevel,ruleConfigKey,ruleKey)

        // todo 统计所有级别的告警
        val ruleList = calculateRule3AlarmAllLevel_merge_by_level(alarmRuleType,alarmRuleLimit,ruleConfigKey,
                                                  ruleKey,
                                                  shouldByPass,
                                                  indicatorResult,
                                                  oocLevel)


        if(ruleList.nonEmpty){
          Option(ruleList.toList)
        }else{
          None
        }
      }else{
        logger.error(s"error N == null, alarmRuleType == ${alarmRuleType.toJson}")
        None
      }

    } catch {
      case ex:Exception => logger.warn(s" processRule $ruleType  error:${ExceptionInfo.getExceptionInfo(ex)} indicatorResult:$indicatorResult key1: $ruleConfigKey key2: $ruleKey alarmRuleType :$alarmRuleType")
        None
    }
  }
  
  /**
   * 获取最大的 N
   * @param alarmRuleType
   * @return
   */
  def getMax_N(alarmRuleType: AlarmRuleType) ={

    val NList: List[Long] =
      alarmRuleType.USLorRule45.map(_.N.get) ++
        alarmRuleType.UBL.map(_.N.get) ++
        alarmRuleType.UCL.map(_.N.get) ++
        alarmRuleType.LCL.map(_.N.get) ++
        alarmRuleType.LBL.map(_.N.get) ++
        alarmRuleType.LSL.map(_.N.get)
    val config_max_N = ProjectConfig.ALARM_RULE_3_MAX_N
    Max_N = Math.max(NList.max,config_max_N).toInt


  }

  def updateKeys_countMap_Rule_3_merge_by_level(key1: String, key2: String) = {
    // todo 0 : 新增不存在的key1  key2
    if(!countMap_Rule_3_merge_by_level.contains(key1)){

      // todo 1 : key1 不存在
      // 新建计数样例类并存入Map
      val countMapKey1_Rule_3 = new TrieMap[String, Count_3_merge_by_level]()
      countMapKey1_Rule_3.put(key2,new Count_3_merge_by_level)
      countMap_Rule_3_merge_by_level.put(key1,countMapKey1_Rule_3)

    }else{
      // todo 2 : key1 存在
      val countMapKey1_Rule_3 = countMap_Rule_3_merge_by_level(key1)

      if (!countMapKey1_Rule_3.contains(key2)){

        // todo 3 : key2 不存在
        countMapKey1_Rule_3.put(key2,new Count_3_merge_by_level )
        countMap_Rule_3_merge_by_level.put(key1,countMapKey1_Rule_3)
      }
    }
  }

  /**
   * 根据level 更新统计计数
   * @param oocLevel
   * @param ruleConfigKey
   * @param ruleKey
   */
  def update_all_count_merge_by_level(oocLevel: Int, ruleConfigKey:String, ruleKey:String) = {

    if (oocLevel.equals(3) || oocLevel.equals(-3)) {
      // todo alarmLevel == 3 或者 -3
      // SL : true ;
      // BL : true ;
      // CL : true ;
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"SL",true)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"BL",true)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"CL",true)

    }else if (oocLevel.equals(2) || oocLevel.equals(-2)){
      // todo alarmLevel == 2 或者 -2
      // SL : false ;
      // BL : true ;
      // CL : true ;
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"SL",false)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"BL",true)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"CL",true)

    }else if (oocLevel.equals(1) || oocLevel.equals(-1)) {
      // todo alarmLevel == 1 或者 -1
      // SL : false ;
      // BL : false ;
      // CL : true ;
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"SL",false)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"BL",false)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"CL",true)

    }else {
      // todo alarmLevel == 0
      // SL : false ;
      // BL : false ;
      // CL : false ;
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"SL",false)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"BL",false)
      update_count_3_merge_by_level(ruleConfigKey,ruleKey,"CL",false)
      
    }
  }
  
  def update_count_3_merge_by_level(key1: String, key2: String, level: String, isAlarm: Boolean) = {
    // 如果是第一套规则  没有分组的概念
    val indexNums = RULE_CONSTANT.RULE_INDEX_NUM

    for(index <- Range(1,indexNums+1)){
      // 生成组合 eg: USL_1
      val level_index = level + '_' + index.toString
      update_count_3_merge_by_level_index(key1,key2,level_index ,isAlarm)
    }
  }

  def update_count_3_merge_by_level_index(key1: String, key2: String, level_index: String, isAlarm: Boolean) = {
    // todo 0 : 只有key1 key2 存在才能更新
    if (countMap_Rule_3_merge_by_level.contains(key1)){
      val countMapKey1_Rule_3 = countMap_Rule_3_merge_by_level(key1)

      if(countMapKey1_Rule_3.contains(key2)){

        val count_3_merge_by_level : Count_3_merge_by_level = countMapKey1_Rule_3(key2)

        level_index match {
          //------------------------第一组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_1 => {
            val currentList = count_3_merge_by_level.CL_1
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setCL_1(saveList)
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_1 => {
            val currentList = count_3_merge_by_level.BL_1
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setBL_1(saveList)
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_1 => {
            val currentList = count_3_merge_by_level.SL_1
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setSL_1(saveList)
          }

          //------------------------第二组--------------------
          case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_2 => {
            val currentList = count_3_merge_by_level.CL_2
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setCL_2(saveList)
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_2 => {
            val currentList = count_3_merge_by_level.BL_2
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setBL_2(saveList)
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_2 => {
            val currentList = count_3_merge_by_level.SL_2
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setSL_2(saveList)
          }

          //------------------------第三组--------------------

          case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_3 => {
            val currentList = count_3_merge_by_level.CL_3
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setCL_3(saveList)
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_3 => {
            val currentList = count_3_merge_by_level.BL_3
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setBL_3(saveList)
          }
          case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_3 => {
            val currentList = count_3_merge_by_level.SL_3
            val saveList = add_and_drop_count_3(currentList,isAlarm)
            count_3_merge_by_level.setSL_3(saveList)
          }
        }

        // 修改完后需要在存入缓存
        countMapKey1_Rule_3.put(key2,count_3_merge_by_level)
        countMap_Rule_3_merge_by_level.put(key1,countMapKey1_Rule_3)
      }
    }
  }

  def add_and_drop_count_3(currentList: ListBuffer[Boolean],isAlarm:Boolean) = {

    // todo 添加最新的数据
    currentList.append(isAlarm)

    // todo 如果缓存的数据 大于 Max_N 清除前面的数据
    val saveList = if (currentList.length > Max_N) {
      currentList.drop(currentList.length - Max_N.toInt)
    } else {
      currentList
    }

    saveList
  }

  /**
   * 统计所有级别的告警
   *
   * @param alarmRuleType
   * @param alarmRuleLimit
   * @param countQueue_3
   * @param shouldByPass
   * @param indicatorResult
   * @return
   */
  def calculateRule3AlarmAllLevel_merge_by_level(alarmRuleType: AlarmRuleType,
                                                 alarmRuleLimit: AlarmRuleLimit,
                                                 ruleConfigKey : String,
                                                 ruleKey : String,
                                                 shouldByPass : Boolean,
                                                 indicatorResult : IndicatorResult,
                                                 oocLevel:Int) = {
    // 收集所有的Rule
    val ruleList = ListBuffer[Option[Rule]]()

    // todo  遍历告警级别 [-3,-2,-1,1,2,3]
    var startIndex = -3
    var endIndex = 4
    if(oocLevel > 0 && oocLevel < 4){
      startIndex = 1
      endIndex = oocLevel + 1
    }else if (oocLevel < 0 && oocLevel > -4){
      startIndex = oocLevel
      endIndex = 0
    }else{
      startIndex = 0
      endIndex = 0
    }

    for(alarm_level <- Range(startIndex,endIndex) ) {
      val alarmRuleParamList_level: (List[AlarmRuleParam], String) = alarm_level match {
        case 3 => (alarmRuleType.USLorRule45,"SL")
        case 2 => (alarmRuleType.UBL,"BL")
        case 1 => (alarmRuleType.UCL,"CL")
        case -3 => (alarmRuleType.LSL,"SL")
        case -2 => (alarmRuleType.LBL,"BL")
        case -1 => (alarmRuleType.LCL,"CL")
      }

      val singleLevelAlarmRuleParamList: ListBuffer[AlarmRuleParam] =
        calculateRule3AlarmSingleLevel_merge_by_level(alarmRuleParamList_level,ruleConfigKey, ruleKey)

      if(singleLevelAlarmRuleParamList.nonEmpty){
        // todo 组装Rule 信息
        val rule: Option[Rule] = getRule(
          alarmRuleType = alarmRuleType,
          alarmRuleLimit = alarmRuleLimit,
          alarmRuleParamList = singleLevelAlarmRuleParamList.toList,
          alarmLevel = alarm_level,
          shouldByPass = shouldByPass,
          indicatorBypassCondition = indicatorResult.bypassCondition)

        // todo 收集所有正常的Rule : 必须当前级别的 Limit 有值
        if(rule.nonEmpty){
          ruleList.append(rule)
        }
      }
    }

    ruleList
  }


  /**
   * 统计队列中 相同级别统计计数
   * @param countQueue_3
   * @param alarmRuleParamList_UP
   * @param currentLevelLimitValue_UP
   * @param alarmRuleParamList_LOW
   * @param currentLevelLimitValue_LOW
   * @return
   */
  def calculateRule3AlarmSingleLevel_merge_by_level(alarmRuleParamList_level: (List[AlarmRuleParam], String),
                                                    key1:String,
                                                    key2:String): ListBuffer[AlarmRuleParam] = {

    val alarmRuleParamList = alarmRuleParamList_level._1
    val level = alarmRuleParamList_level._2

    val singleLevelAlarmRuleParamList = ListBuffer[AlarmRuleParam]()

    // todo 过滤UP
    val validAlarmRuleParamList = alarmRuleParamList.filter(alarmRuleParam => {
      alarmRuleParam.X.nonEmpty && // X 不能为空
        alarmRuleParam.N.nonEmpty && // N 不能为空
        alarmRuleParam.X.get <= alarmRuleParam.N.get // X <= N
    })

    // todo 一共有3组
    for(index <- Range(0,RULE_CONSTANT.RULE_INDEX_NUM)){

      // todo 当up 和 low 超出Limit 的个数的和 大于 X  时，记录告警
      if(validAlarmRuleParamList.length > index){
        val alarmRuleParam: AlarmRuleParam = validAlarmRuleParamList(index)
        val total_count = getCount_merge_by_level_index(key1,key2,alarmRuleParam.N.get.toInt,level,index + 1)
        if(total_count >= alarmRuleParam.X.get){
          singleLevelAlarmRuleParamList.append(alarmRuleParam)
        }
      }
    }
    singleLevelAlarmRuleParamList
  }


  /**
   * 获取N 个记录中有多少个 true
   * @param currentList
   * @param num_N
   * @return
   */
  def getCount(currentList: ListBuffer[Boolean],num_N:Int): Int = {

    if(null != currentList && currentList.nonEmpty){
      val calcList = if(currentList.length > num_N){
        currentList.takeRight(num_N)
      }else{
        currentList
      }
      calcList.filter(elem => {
        elem
      }).length
    }else{
      0
    }

  }


  /**
   *
   * @param key1
   * @param key2
   * @param num_N
   * @param level
   * @param index
   * @return
   */
  def getCount_merge_by_level_index(key1:String, key2:String, num_N:Int ,level:String, index:Int) = {
    val key1CountMap_Rule_3: TrieMap[String, Count_3_merge_by_level] = countMap_Rule_3_merge_by_level.getOrElse(key1, new TrieMap[String, Count_3_merge_by_level])
    val count_3_merge_by_level: Count_3_merge_by_level = key1CountMap_Rule_3.getOrElse(key2, null)
    if(null != count_3_merge_by_level){
      val level_indix: String = level + "_" + index.toString
      val count = level_indix match {
        //------------------------第一组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_1 => {
          val currentList = count_3_merge_by_level.CL_1
          getCount(currentList,num_N)
        }
        case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_1 => {
          val currentList = count_3_merge_by_level.BL_1
          getCount(currentList,num_N)
        }
        case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_1 => {
          val currentList = count_3_merge_by_level.SL_1
          getCount(currentList,num_N)
        }

        //------------------------第二组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_2 => {
          val currentList = count_3_merge_by_level.CL_2
          getCount(currentList,num_N)
        }
        case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_2 => {
          val currentList = count_3_merge_by_level.BL_2
          getCount(currentList,num_N)
        }
        case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_2 => {
          val currentList = count_3_merge_by_level.SL_2
          getCount(currentList,num_N)
        }

        //------------------------第三组--------------------
        case RULE_CONSTANT.RULE_LEVEL_INDEX_CL_3 => {
          val currentList = count_3_merge_by_level.CL_3
          getCount(currentList,num_N)
        }
        case RULE_CONSTANT.RULE_LEVEL_INDEX_BL_3 => {
          val currentList = count_3_merge_by_level.BL_3
          getCount(currentList,num_N)
        }
        case RULE_CONSTANT.RULE_LEVEL_INDEX_SL_3 => {
          val currentList = count_3_merge_by_level.SL_3
          getCount(currentList,num_N)
        }

        case _ => {
          logger.error(s"getCount_merge_by_level_index error : " +
            s"key1 = ${key1} ; " +
            s"key2 = ${key2} ; " +
            s"num_N = ${num_N} ; " +
            s"level_indix = ${level_indix} ; ")
          0
        }
      }
      count
    }else{
      0
    }
  }

  /**
   * 清空缓存
 *
   * @param key1
   */
  override def removeRuleData(key1: String): Unit = {
    this.countMap_Rule_3_merge_by_level.remove(key1)
    logger.warn(s"countMap_Rule_3_merge_by_level removeRuleData key = ${key1}")
  }


}
