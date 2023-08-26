package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{AlarmRuleResult, ConfigData, ContextConfigData, ErrorCode, FdcData, IndicatorConfig, IndicatorErrorConfig, IndicatorErrorReportData, RunData, indicatorTimeOutTimestamp}
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{ContextDataType, IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer
/**
 *    author：yuxiang
 * *  time： 2023.03.20
 * *  功能：indicator error报表实时统计有哪些run对应的indicator没有计算
 */
class IndicatorTimeOutKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,
  JsonNode, JsonNode, IndicatorErrorReportData] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[IndicatorTimeOutKeyedBroadcastProcessFunction])

  // =============== 配置 ==========================================================================
  // context配置 {contextId: [("tool|chamber|recipe|product|stage", IndicatorErrorConfig)]}
  val contextConfigMap = new concurrent.TrieMap[Long, concurrent.TrieMap[String, IndicatorErrorConfig]]()

  // indicator配置 {"tool|chamber|recipe|product|stage": {controlPlanId: {controlPlanVersion: ("indicatorId", IndicatorErrorConfig)}}}
  val indicatorConfigMap = new concurrent.TrieMap[String, concurrent.TrieMap[String, concurrent.TrieMap[String, mutable.Set[(String, IndicatorErrorConfig)]]]]()


  // ============== 缓存数据 ========================================================================
  //{"runId": ["controlPlanId|indicatorId"]}
  private var indicatorCacheMapState: MapState[String, mutable.Set[String]] = _


  /** The state that is maintained by this process function */
  private var indicatorTimeOutState: ValueState[indicatorTimeOutTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    // 初始化ContextConfig
//    val contextConfigList = InitFlinkFullConfigHbase.ContextConfigList
    val contextConfigList =  readHbaseAllConfig[List[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE, ContextDataType)
    contextConfigList.foreach(addContextMapToContextMap)

//    val initConfig: ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList
    val initConfig: ListBuffer[ConfigData[IndicatorConfig]] =  readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)
    logger.warn("indicatorConfig SIZE: " + initConfig.size)
    initConfig.foreach(addIndicatorConfigToTCSP)

    val eventStartStateDescription = new ValueStateDescriptor[indicatorTimeOutTimestamp](
      "indicatorPrintTimeOutValueState", TypeInformation.of(classOf[indicatorTimeOutTimestamp]))
    eventStartStateDescription.enableTimeToLive(ttlConfig)
    indicatorTimeOutState = getRuntimeContext.getState(eventStartStateDescription)

    val indicatorCacheMapStateDescription: MapStateDescriptor[String,mutable.Set[String]] = new
        MapStateDescriptor[String,mutable.Set[String]]("indicatorCacheMapStateDescription",
          TypeInformation.of(classOf[String]),TypeInformation.of(classOf[mutable.Set[String]]))
    indicatorCacheMapStateDescription.enableTimeToLive(ttlConfig)
    indicatorCacheMapState = getRuntimeContext.getMapState(indicatorCacheMapStateDescription)
  }

  /**
   *  数据流超时
   */
  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode,
    IndicatorErrorReportData]#OnTimerContext, out: Collector[IndicatorErrorReportData]): Unit = {
    super.onTimer(timestamp, ctx, out)
    try {

      indicatorTimeOutState.value match {
        case indicatorTimeOutTimestamp(runId, runEndTime, key, lastModified)
          if (System.currentTimeMillis() >= lastModified + 900000L) =>

          logger.warn(s"onTimer_tep1: $runId  $key")

          // 获取计算完成的indicator
          val indicatorDataSet = indicatorCacheMapState.get(runId)

          val keyList = key.split("\\|", -1)
          val toolName = keyList(0)
          val chamberName = keyList(1)
          val recipeName = if(keyList.length >= 3) keyList(2) else ""
          val productTmp = if(keyList.length >= 4) keyList(3) else ""
          val stageTMp = if(keyList.length >= 5) keyList(4) else ""
          for(product <- productTmp.split(",")){
            for(stage <- stageTMp.split(",")){
              val productStageKey = s"${toolName}|${chamberName}|${recipeName}|${product}|${stage}"

              val stageKey = s"${toolName}|${chamberName}|${recipeName}||${stage}"

              val productKey = s"${toolName}|${chamberName}|${recipeName}|${product}|"

              val recipeKey = s"${toolName}|${chamberName}|${recipeName}||"

              val keyList = List(productStageKey, stageKey, productKey, recipeKey)

              for(elem <- keyList){
                if(indicatorConfigMap.contains(elem)){
                  val controlPlanMap = indicatorConfigMap(elem)
                  val configControlPlanId = controlPlanMap.keys.toList.max
                  val versionMap = controlPlanMap(configControlPlanId)
                  val configControlPlanVersion = versionMap.keys.map(_.toInt).max

                  val indicatorConfigSet = versionMap(configControlPlanVersion.toString)
//                  logger.warn(s"elem: ${elem} indicatorDataSet: ${indicatorDataSet} indicatorConfigSet: " +
//                    s"${indicatorConfigSet} configControlPlanVersion:${configControlPlanVersion}")

                  for(elem <- indicatorConfigSet){
                    val indicatorId = elem._1
                    val indicatorKey = s"$configControlPlanId|$indicatorId"
                    val indicatorErrorConfig = elem._2

                    // indicator在线没有计算的
                    if(!(indicatorDataSet.contains(indicatorKey))){
                      logger.error(s"002009d001C indicator在线没有计算 indicatorKey: $indicatorKey configControlPlanId:" +
                        s" $configControlPlanId version: $configControlPlanVersion $runId runEndTime: $runEndTime")

                      val indicatorErrorReportData = IndicatorErrorReportData(
                        runEndTime = runEndTime,
                        windowDataCreateTime = 0L,
                        runId = runId,
                        toolGroupName = indicatorErrorConfig.toolGroupName,
                        chamberGroupName = indicatorErrorConfig.chamberGroupName,
                        recipeGroupName = indicatorErrorConfig.recipeGroupName,
                        toolName = indicatorErrorConfig.toolName,
                        chamberName = indicatorErrorConfig.chamberName,
                        recipeName = indicatorErrorConfig.recipeName,
                        controlPlanName = indicatorErrorConfig.controlPlanName,
                        indicatorName = indicatorErrorConfig.indicatorName,
                        indicatorValue = ""
                      )

                      out.collect(indicatorErrorReportData)
                    }
                  }
                  indicatorCacheMapState.remove(runId)

                  close()
                  return
                }
              }
            }
          }
          indicatorCacheMapState.remove(runId)

          close()
        case _ =>
      }
    }catch {
      case e: Exception => logger.warn(s"indicatorPrintTimeOut onTimer " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }

  /**
   *  数据流
   */
  override def processElement(in1: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode,
    JsonNode, IndicatorErrorReportData]#ReadOnlyContext, out: Collector[IndicatorErrorReportData]): Unit = {
    try {
      val option = in1.path(MainFabConstants.dataType).asText()
      // indicator处理
      if (option == "AlarmLevelRule") {

        val a = toBean[FdcData[AlarmRuleResult]](in1)
        val indicatorData = a.datas

        /**
         *  数据流输出
         */
        val indicatorErrorReportData = IndicatorErrorReportData(
          runEndTime = indicatorData.runEndTime,
          windowDataCreateTime = indicatorData.windowDataCreateTime,
          runId = indicatorData.runId,
          toolGroupName = indicatorData.toolGroupName,
          chamberGroupName = indicatorData.chamberGroupName,
          recipeGroupName = indicatorData.recipeGroupName,
          toolName = indicatorData.toolName,
          chamberName = indicatorData.chamberName,
          recipeName = indicatorData.recipeName,
          controlPlanName = indicatorData.controlPlanName,
          indicatorName = indicatorData.indicatorName,
          indicatorValue = indicatorData.indicatorValue
        )

        out.collect(indicatorErrorReportData)


        /**
         *   缓存indicator计算完成的记录, 用来判断那些indicator没有计算
         */
        val controlPlanId = indicatorData.controlPlnId
        val value = s"${controlPlanId}|${indicatorData.indicatorId}"

        val runId = a.datas.runId

        if(indicatorCacheMapState.contains(runId)){
          val indicatorIdSet = indicatorCacheMapState.get(runId)
          indicatorIdSet.add(value)
          indicatorCacheMapState.put(runId, indicatorIdSet)
        }else{
          indicatorCacheMapState.put(runId, mutable.Set(value))
        }
      }else if(option == "rundata"){
        val rundata = toBean[FdcData[RunData]](in1)
        if(rundata.datas.runEndTime.nonEmpty){
          if(rundata.datas.runEndTime.get != -1L) {
            val runId = rundata.datas.runId

            val productList = rundata.datas.lotMESInfo.map(x => if (x.nonEmpty) x.get.product.getOrElse("")).distinct.mkString(",")
            val StageList = rundata.datas.lotMESInfo.map(x => if (x.nonEmpty) x.get.stage.getOrElse("")).distinct.mkString(",")
            val key = s"${rundata.datas.toolName}|${rundata.datas.chamberName}|${rundata.datas.recipe}|${productList}|${StageList}"

            // write the state back
            indicatorTimeOutState.update(indicatorTimeOutTimestamp(runId, rundata.datas.runEndTime.get, key, System.currentTimeMillis()))

            // schedule the next timer 600 seconds from the current event time
            ctx.timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 900000L)
          }
        }
      }

    }catch {
      case e: Exception => logger.warn(s"indicatorPrintTimeOut processBroadcastElement error data:$in1  " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }

  /**
   *  广播流
   */
  override def processBroadcastElement(in2: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode,
    JsonNode, IndicatorErrorReportData]#Context, collector: Collector[IndicatorErrorReportData]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()

      if(dataType == "indicatorconfig"){
        logger.warn(s"indicatorconfig: ${in2}")
        val indicatorConfig = toBean[ConfigData[IndicatorConfig]](in2)
        addIndicatorConfigToTCSP(indicatorConfig)
      } else if (dataType == MainFabConstants.context) {
        try {
          val contextConfig = toBean[ConfigData[List[ContextConfigData]]](in2)
          addContextMapToContextMap(contextConfig)

        } catch {
          case e: Exception => logger.warn(s"contextConfig json error data:$in2  " +
            s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      }
    } catch {
      case e: Exception => logger.warn(s"indicatorPrintTimeOut processBroadcastElement error data:$in2  " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * 添加context config
   *
   */
  def addContextMapToContextMap(configList: ConfigData[List[ContextConfigData]]): Unit = {
    try {
      for (elem <- configList.datas) {
        val product = if (elem.productName == null) "" else elem.productName
        val stage = if (elem.stage == null) "" else elem.stage
        val toolKey = s"${elem.toolName}|${elem.chamberName}|${elem.recipeName}|${product}|${stage}"
        val indicatorErrorConfig = IndicatorErrorConfig(elem.toolGroupName, elem.chamberGroupName,
          elem.recipeGroupName, elem.toolName, elem.chamberName, elem.recipeName, "", "")

        val key = elem.contextId

        if (configList.status) {
          // 修改
          if(contextConfigMap.contains(key)){
            val toolMap = contextConfigMap(key)
            toolMap.put(toolKey, indicatorErrorConfig)

            contextConfigMap.put(key, toolMap)
          }else{
            val toolScala = concurrent.TrieMap[String, IndicatorErrorConfig](toolKey -> indicatorErrorConfig)

            contextConfigMap.put(key, toolScala)
          }
        }else{
          if(contextConfigMap.contains(key)){
            val contextKeys = contextConfigMap(key)
            for(contextKey <- contextKeys){
              // 删除
              if (this.indicatorConfigMap.contains(contextKey._1)) {
                indicatorConfigMap.remove(contextKey._1)
                logger.warn(s"indicatorConfigMap删除: ${contextKey._1}")
              }
            }
          }
          logger.warn(s"contextConfigMap删除: ${key}")
          // 删除
          contextConfigMap.remove(key)
        }
      }
    }catch {
      case ex: Exception => logger.warn(s"addContextMapToContextMap_error: $ex")
    }
  }

  /**
   *  添加indicator配置
   */
  def addIndicatorConfigToTCSP(kafkaConfigData: ConfigData[IndicatorConfig]): Unit = {
    try {
      val indicatorConfig = kafkaConfigData.datas
      val contextId = indicatorConfig.contextId
      val controlPlanId = indicatorConfig.controlPlanId.toString
      val controlPlanVersion = indicatorConfig.controlPlanVersion.toString
      val indicatorId = indicatorConfig.indicatorId.toString

      if(contextConfigMap.contains(contextId)){
        val keyList = contextConfigMap(contextId)

        keyList.foreach(keyTupe => {
          val key = keyTupe._1
          val indicatorErrorConfig = keyTupe._2
          // 增加
          if (kafkaConfigData.status) {

            val ControlPlanKey = s"${indicatorId}"
            val indicatorErrorConfigNew = indicatorErrorConfig.copy(controlPlanName=indicatorConfig.controlPlanName,
              indicatorName = indicatorConfig.indicatorName)

            if (this.indicatorConfigMap.contains(key)) {
              val ControlPlanMap = this.indicatorConfigMap(key)
              // 包含controlPlanId
              if (ControlPlanMap.contains(controlPlanId)) {
                val versionMap = ControlPlanMap(controlPlanId)
                if (versionMap.contains(controlPlanVersion)) {
                  val indicatorSet = versionMap(controlPlanVersion)
                  indicatorSet.add((ControlPlanKey, indicatorErrorConfigNew))
                  versionMap.put(controlPlanVersion, indicatorSet)
                } else {
                  versionMap.put(controlPlanVersion, mutable.Set((ControlPlanKey, indicatorErrorConfigNew)))
                }
                ControlPlanMap.put(controlPlanId, versionMap)
              } else {
                val versionScala = concurrent.TrieMap[String, mutable.Set[(String, IndicatorErrorConfig)]](controlPlanVersion -> mutable.Set((ControlPlanKey, indicatorErrorConfigNew)))
                ControlPlanMap.put(controlPlanId, versionScala)
              }
              indicatorConfigMap.put(key, ControlPlanMap)
            }else {
              val versionNewcala = concurrent.TrieMap[String, mutable.Set[(String, IndicatorErrorConfig)]](controlPlanVersion -> mutable.Set((ControlPlanKey, indicatorErrorConfigNew)))
              val controlPlanScala = concurrent.TrieMap[String, concurrent.TrieMap[String, mutable.Set[(String, IndicatorErrorConfig)]]](controlPlanId -> versionNewcala)
              indicatorConfigMap.put(key, controlPlanScala)
            }

            if(indicatorConfigMap.contains(key)){
              val controlPlanMap = indicatorConfigMap(key)
              if(controlPlanMap.contains(controlPlanId)){
                val versionMap = controlPlanMap(controlPlanId)
                // 更新版本
                val k = versionMap.keys.map(_.toLong)
                if (k.size > 2) {
                  val minVersion = k.toList.min
                  versionMap.remove(minVersion.toString)
                  controlPlanMap.put(controlPlanId, versionMap)
                  indicatorConfigMap.put(key, controlPlanMap)
                  logger.warn(s"addToolWindowConfig_step4: " + minVersion + "\tindicatorConfig: " + indicatorConfig)
                }
              }
            }

          }else{
            // 删除
            if (this.indicatorConfigMap.contains(key)) {
              indicatorConfigMap.remove(key)
            }
          }
        })
      }
    } catch {
      case ex: Exception => logger.warn(s"addIndicatorConfigToTCSP_error: $ex")
    }
  }


}
