package com.hzw.fdc.function.online.MainFabAlarm.AlarmProcessRules
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{AlarmRuleLimit, AlarmRuleParam, AlarmRuleType, ByPassCondition, IndicatorLimitResult, IndicatorResult, RULE_CONSTANT, Rule}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
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
class AlarmProcessRule_3 extends AlarmProcessRuleParent {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmProcessRule_3])

  override def processRule(
                            indicatorResult: IndicatorResult,
                            alarmRuleType: AlarmRuleType,
                            alarmRuleLimit: AlarmRuleLimit,
                            ruleConfigKey: String,
                            w2wType: String,
                            indicatorLimitResult: IndicatorLimitResult,
                            shouldByPass: Boolean): Option[List[Option[Rule]]] = {

//    logger.error(s"----------- start processRule -----------")

    val ruleType = alarmRuleType.ruleType

    val ruleKeyTuple = getRuleKey(w2wType, indicatorResult)
    val ruleKey = ruleKeyTuple._1 + "|" + indicatorResult.indicatorId.toString + "|" +ruleType
    val w2wKeyList = ruleKeyTuple._2.map(_ + "|" + indicatorResult.indicatorId.toString + "|" +ruleType)

    try {

      // todo 取出最大的 N
      val max_N: Option[Long] = getMax_N(alarmRuleType)
      if(max_N.nonEmpty  &&  max_N.get > 0){

        //取出生效的indicatorValue 值
        val oocLevel = indicatorLimitResult.oocLevel
        val indicatorValueList: Array[Double] = indicatorResult.indicatorValue.split("\\|").map(_.toDouble)

        val indicatorValue = if(oocLevel > 0){
          indicatorValueList.max
        }else{
          indicatorValueList.min
        }

        // todo 更新队列
        val countQueue_3: Option[mutable.Queue[Double]] = update_countQueue_Rule_3(ruleConfigKey, ruleKey, indicatorValue, max_N.get)

        // todo 统计所有级别的告警
//        val ruleList = calculateRule3AlarmAllLevel(alarmRuleType,
//                                                  alarmRuleLimit,
//                                                  countQueue_3,
//                                                  shouldByPass,indicatorResult)

        val ruleList = calculateRule3AlarmAllLevel_merge_by_level(alarmRuleType,
                                                  alarmRuleLimit,
                                                  countQueue_3,
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

    Option(Math.max(NList.max,config_max_N))
  }

  /**
   * 统计所有级别的告警
   * @param alarmRuleType
   * @param alarmRuleLimit
   * @param countQueue_3
   * @param shouldByPass
   * @param indicatorResult
   * @return
   */
  def calculateRule3AlarmAllLevel(alarmRuleType: AlarmRuleType,
                                  alarmRuleLimit: AlarmRuleLimit,
                                  countQueue_3: Option[mutable.Queue[Double]],
                                  shouldByPass : Boolean,
                                  indicatorResult:IndicatorResult ) = {
    // 收集所有的Rule
    val ruleList = ListBuffer[Option[Rule]]()

    // 疑问: 为什么这里需要遍历所有级别的告警？
    // 解答: 1- 告警后不会将队列中的数据清空;
    for(alarm_level <- Range(-3,4)  if alarm_level != 0) {
      val alarmRuleParamList_isMoreThan_Limit: (List[AlarmRuleParam], Boolean,Option[Double]) = alarm_level match {
        case 3 => (alarmRuleType.USLorRule45,true,alarmRuleLimit.USL)
        case 2 => (alarmRuleType.UBL, true,alarmRuleLimit.UBL)
        case 1 => (alarmRuleType.UCL, true,alarmRuleLimit.UCL)
        case -1 => (alarmRuleType.LCL, false,alarmRuleLimit.LCL)
        case -2 => (alarmRuleType.LBL, false,alarmRuleLimit.LBL)
        case -3 => (alarmRuleType.LSL, false,alarmRuleLimit.LSL)
      }

      val alarmRuleParamList: List[AlarmRuleParam] = alarmRuleParamList_isMoreThan_Limit._1
      val isMoreThan: Boolean = alarmRuleParamList_isMoreThan_Limit._2
      val currentLevelLimitValue: Option[Double] = alarmRuleParamList_isMoreThan_Limit._3

      // todo 记录 单个级别(最多3组)的 所有告警
      val singleLevelAlarmRuleParamList: ListBuffer[AlarmRuleParam] =
        calculateRule3AlarmSingleLevel(countQueue_3.get, alarmRuleParamList, isMoreThan, currentLevelLimitValue)

      if(singleLevelAlarmRuleParamList.nonEmpty){
        // todo 组装Rule 信息
        val rule: Option[Rule] = getRule(
          alarmRuleType = alarmRuleType,
          alarmRuleLimit = alarmRuleLimit,
          alarmRuleParamList = singleLevelAlarmRuleParamList.toList,
          alarmLevel = alarm_level,
          shouldByPass=shouldByPass,
          indicatorBypassCondition = indicatorResult.bypassCondition)

        // todo 收集所有正常的Rule : 必须当前级别的 Limit 有值
        if(rule.nonEmpty && currentLevelLimitValue.nonEmpty){
          ruleList.append(rule)
        }
      }
    }

    ruleList
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
                                                countQueue_3: Option[mutable.Queue[Double]],
                                                shouldByPass : Boolean,
                                                indicatorResult:IndicatorResult,
                                                oocLevel :Int) = {
    // 收集所有的Rule
    val ruleList = ListBuffer[Option[Rule]]()

    for(alarm_level <- Range(1,oocLevel.abs + 1)  if alarm_level != 0) {
      val alarmRuleParamList_isMoreThan_Limit = alarm_level match {
        case 3 => (alarmRuleType.USLorRule45,alarmRuleLimit.USL,alarmRuleType.LSL,alarmRuleLimit.LSL)
        case 2 => (alarmRuleType.UBL, alarmRuleLimit.UBL,alarmRuleType.LBL,alarmRuleLimit.LBL)
        case 1 => (alarmRuleType.UCL, alarmRuleLimit.UCL,alarmRuleType.LCL,alarmRuleLimit.LCL)
      }

      val alarmRuleParamList_UP: List[AlarmRuleParam] = alarmRuleParamList_isMoreThan_Limit._1
      val currentLevelLimitValue_UP: Option[Double] = alarmRuleParamList_isMoreThan_Limit._2
      val alarmRuleParamList_LOW = alarmRuleParamList_isMoreThan_Limit._3
      val currentLevelLimitValue_LOW = alarmRuleParamList_isMoreThan_Limit._4

      // todo 记录 单个级别(最多3组)的 所有告警
      val singleLevelAlarmRuleParamList: ListBuffer[AlarmRuleParam] =
        calculateRule3AlarmSingleLevel_merge_by_level(countQueue_3.get,
                                                      alarmRuleParamList_UP,
                                                      currentLevelLimitValue_UP,
                                                      alarmRuleParamList_LOW,
                                                      currentLevelLimitValue_LOW,
                                                      oocLevel)

      if(singleLevelAlarmRuleParamList.nonEmpty){
        // todo 组装Rule 信息
        val rule: Option[Rule] = getRule(
          alarmRuleType = alarmRuleType,
          alarmRuleLimit = alarmRuleLimit,
          alarmRuleParamList = singleLevelAlarmRuleParamList.toList,
          alarmLevel = oocLevel,
          shouldByPass = shouldByPass,
          indicatorBypassCondition = indicatorResult.bypassCondition)

        // todo 收集所有正常的Rule : 必须当前级别的 Limit 有值
        if(rule.nonEmpty && (currentLevelLimitValue_UP.nonEmpty || currentLevelLimitValue_LOW.nonEmpty)){
          ruleList.append(rule)
        }
      }
    }

    ruleList
  }





  /**
   * 计算 单个级别(最多3组)的 所有告警
   * @param countMapQueue_3
   * @param alarmRuleParamList
   * @param isMorethan
   * @param limit
   * @return
   */
  def calculateRule3AlarmSingleLevel(countQueue_3: mutable.Queue[Double],
                                     alarmRuleParamList:List[AlarmRuleParam],
                                     isMorethan: Boolean,
                                     currentLevelLimitValue:Option[Double]) = {

    val validAlarmRuleParamList = alarmRuleParamList.filter(alarmRuleParam => {
      alarmRuleParam.X.nonEmpty && // X 不能为空
        alarmRuleParam.N.nonEmpty && // N 不能为空
        alarmRuleParam.X.get <= alarmRuleParam.N.get // X <= N
    })

    val singleLevelAlarmRuleParamList = ListBuffer[AlarmRuleParam]()

    if (currentLevelLimitValue.nonEmpty && validAlarmRuleParamList.nonEmpty){
      for(alarmRuleParam <- validAlarmRuleParamList ){

        // 当前规则队列中应该保留的值
        var currentCountQueue = countQueue_3

        // todo 如果 队列中 的值大于 N , 需要取后面 N 个值
        val n = alarmRuleParam.N.get
        while (currentCountQueue.length > n ){
          currentCountQueue = currentCountQueue.tail
        }

        // 记录超过界线的个数
        var count = 0

        if(isMorethan && currentCountQueue.last > currentLevelLimitValue.get){
          // 如果是 UP ; 当最新 IndicatorValue > limit 的时候才会 统计是否告警
          currentCountQueue.foreach(value => {
            if(value > currentLevelLimitValue.get){
              count += 1
            }
          })
        }else if(!isMorethan && currentCountQueue.last < currentLevelLimitValue.get){
          // 如果是 LOW ; 当最新 IndicatorValue < limit 的时候才会 统计是否告警
          currentCountQueue.foreach(value =>{
            if(value < currentLevelLimitValue.get){
              count += 1
            }
          })
        }else{
          logger.warn(s"this point not alarm ; " +
            s"isMorthan = ${isMorethan} ; " +
            s"lastIndicatorValue = ${currentCountQueue.last} ; " +
            s"limitValue = ${currentLevelLimitValue.get}")
        }

        // todo 当 超出Limit 的个数 大于 X 时，记录告警
        if(count >= alarmRuleParam.X.get){
          singleLevelAlarmRuleParamList.append(alarmRuleParam)
        }

      }
    }else{
      logger.error(s"error currentLevelLimitValue == ${currentLevelLimitValue},validAlarmRuleParamList==${validAlarmRuleParamList.toJson}")
    }
    singleLevelAlarmRuleParamList
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
  def calculateRule3AlarmSingleLevel_merge_by_level(countQueue_3: mutable.Queue[Double],
                                                    alarmRuleParamList_UP: List[AlarmRuleParam],
                                                    currentLevelLimitValue_UP: Option[Double],
                                                    alarmRuleParamList_LOW: List[AlarmRuleParam],
                                                    currentLevelLimitValue_LOW: Option[Double],
                                                    oocLevel:Int): ListBuffer[AlarmRuleParam] = {


    val singleLevelAlarmRuleParamList = ListBuffer[AlarmRuleParam]()

    // todo 过滤UP
    val validAlarmRuleParamList_UP = alarmRuleParamList_UP.filter(alarmRuleParam => {
      alarmRuleParam.X.nonEmpty && // X 不能为空
        alarmRuleParam.N.nonEmpty && // N 不能为空
        alarmRuleParam.X.get <= alarmRuleParam.N.get // X <= N
    })

    // todo 过滤LOW
    val validAlarmRuleParamList_LOW = alarmRuleParamList_LOW.filter(alarmRuleParam => {
      alarmRuleParam.X.nonEmpty && // X 不能为空
        alarmRuleParam.N.nonEmpty && // N 不能为空
        alarmRuleParam.X.get <= alarmRuleParam.N.get // X <= N
    })

    // todo 一共有3组
    for(index <- Range(0,RULE_CONSTANT.RULE_INDEX_NUM)){

      val count_up: Int = if(currentLevelLimitValue_UP.nonEmpty &&
        validAlarmRuleParamList_UP.nonEmpty &&
        validAlarmRuleParamList_UP.length > index){
        // 求up 的 count
        val alarmRuleParam = validAlarmRuleParamList_UP(index)
        getCount_merge_by_level(countQueue_3,alarmRuleParam,currentLevelLimitValue_UP,true)
      }else{
        0
      }

      val count_low: Int = if(currentLevelLimitValue_LOW.nonEmpty &&
        validAlarmRuleParamList_LOW.nonEmpty &&
        validAlarmRuleParamList_LOW.length > index){
        // 求low 的 count
        val alarmRuleParam = validAlarmRuleParamList_LOW(index)
        getCount_merge_by_level(countQueue_3,alarmRuleParam,currentLevelLimitValue_LOW,false)
      }else{
        0
      }

      val total_count = count_low + count_up
      // todo 当up 和 low 超出Limit 的个数的和 大于 X  时，记录告警
      if(oocLevel > 0 && validAlarmRuleParamList_UP.length > index){
        val alarmRuleParam = validAlarmRuleParamList_UP(index)
        if(total_count >= alarmRuleParam.X.get){
          singleLevelAlarmRuleParamList.append(alarmRuleParam)
        }
      }

      if(oocLevel < 0 && validAlarmRuleParamList_LOW.length > index){
        val alarmRuleParam = validAlarmRuleParamList_LOW(index)
        if(total_count >= alarmRuleParam.X.get){
          singleLevelAlarmRuleParamList.append(alarmRuleParam)
        }
      }
    }
    singleLevelAlarmRuleParamList
  }


  /**
   * 统计队列中 单个组 超过 limit 的个数
   * @param countQueue_3
   * @param alarmRuleParam
   * @param currentLevelLimitValue
   * @param isMorethan
   * @return
   */
  def getCount_merge_by_level(countQueue_3: mutable.Queue[Double],
                              alarmRuleParam: AlarmRuleParam,
                              currentLevelLimitValue: Option[Double],
                              isMorethan:Boolean): Int = {

    // 当前规则队列中应该保留的值
    var currentCountQueue = countQueue_3

    // todo 如果 队列中 的值大于 N , 需要取后面 N 个值
    val n = alarmRuleParam.N.get
    while (currentCountQueue.length > n ){
      currentCountQueue = currentCountQueue.tail
    }

    // 记录超过界线的个数
    var count = 0

    if(isMorethan){
      // 如果是 UP ; 当最新 IndicatorValue > limit 的时候才会 统计是否告警
      currentCountQueue.foreach(value => {
        if(value > currentLevelLimitValue.get){
          count += 1
        }
      })
    }else if(!isMorethan){
      // 如果是 LOW ; 当最新 IndicatorValue < limit 的时候才会 统计是否告警
      currentCountQueue.foreach(value =>{
        if(value < currentLevelLimitValue.get){
          count += 1
        }
      })
    }

    count
  }



  /**
   * 初始化队列 Queue
   * @param indicatorValue
   * @return
   */
  def initCountQueue(indicatorValue:Double = 0.0) = {
    val countQueue = new mutable.Queue[Double]()
    countQueue.enqueue(indicatorValue)
    countQueue
  }


  /**
   * 只需要保存 最大的 N 个值
   * @param key1
   * @param key2
   * @param indicatorValue
   * @param max_N
   * @return
   */
  def update_countQueue_Rule_3(key1:String,key2:String,indicatorValue:Double,max_N: Long) = {

    // todo 新增不存在的key1  key2
    if(!countQueue_Rule_3.contains(key1)){

      // todo 1 : key1 不存在  初始化 countQueue
      // 新建计数样例类并存入Map
      val countQueueKey1_Rule_3 = new TrieMap[String, mutable.Queue[Double]]()
      val countQueue = initCountQueue(indicatorValue)

      countQueueKey1_Rule_3.put(key2,countQueue)
      countQueue_Rule_3.put(key1,countQueueKey1_Rule_3)

      Option(countQueue)
    }else{
      // todo 2 : key1 存在
      val countQueueKey1_Rule_3 = countQueue_Rule_3(key1)

      if (!countQueueKey1_Rule_3.contains(key2)){

        // todo 3 : key2 不存在
        val countQueue = initCountQueue(indicatorValue)
        countQueueKey1_Rule_3.put(key2,countQueue)
        countQueue_Rule_3.put(key1,countQueueKey1_Rule_3)

        Option(countQueue)
      }else{
        // todo 获取对应的缓存队列
        val countQueueKey1_Rule_3 = countQueue_Rule_3(key1)
        val countQueue = countQueueKey1_Rule_3(key2)

        //todo 加入队列
        countQueue.enqueue(indicatorValue)

        //todo 循环删除超时的IndicatorValue (最多存储 多N个值)
        while (countQueue.length > max_N ){
          countQueue.dequeue()
        }

        // todo 更新后再存入缓存
        countQueueKey1_Rule_3.put(key2,countQueue)
        countQueue_Rule_3.put(key1,countQueueKey1_Rule_3)

        // todo 返回 当前的队列
        Option(countQueue)
      }
    }
  }

  /**
   * 清空缓存
   * @param key1
   */
  override def removeRuleData(key1: String): Unit = {
    this.countQueue_Rule_3.remove(key1)
    logger.warn(s"countQueue_Rule_3 removeRuleData key = ${key1}")
  }


}
