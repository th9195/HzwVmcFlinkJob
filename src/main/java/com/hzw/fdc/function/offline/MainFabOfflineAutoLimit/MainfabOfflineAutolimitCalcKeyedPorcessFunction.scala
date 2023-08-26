package com.hzw.fdc.function.offline.MainFabOfflineAutoLimit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.PublicFunction.limitCondition.{ MatchingLimitConditionFunctions}
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.scalabean.{AutoLimitIndicatorResult, OfflineAutoLimitIndicatorConfig, OfflineAutoLimitResult}
import com.hzw.fdc.util.ProjectConfig
import com.hzw.fdc.util.hbaseUtils.HbaseUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 * MainfabOfflineAutolimitCalcKeyedPorcessFunction
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/23 14:33
 * @since 1.0.0
 * @update 2022/8/23 14:33
 * */
class MainfabOfflineAutolimitCalcKeyedPorcessFunction extends KeyedProcessFunction[String,JsonNode,JsonNode]{

  /**
   * // todo 1- 解析 所有的 ConditionEntity : specId -> List[ConditionEntity]
   * // todo 2- 解析 所有的 OffLineAutoLimitOneConfig (isCalcLimit == true) : specId -> OffLineAutoLimitOneConfig
   * // todo 3- 查询Indicator数据并根据every 采样
   * // todo 4- 匹配condition
   *    // todo 4-1 获取当前indicator 的conditionEntityList
   *    // todo 4-2 计算出匹配每个 spec 的得分 : 配置的 ConditionEntityList 与 每个Indicator 的ConditionEntityList
   *    // todo 4-3 收集每个SpecId 的 OfflineAutoLimitIndicatorResult
   * // todo 5- 开始每个计算SpecId的AutoLimit
   */

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainfabOfflineAutolimitCalcKeyedPorcessFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    HbaseUtil.initProjectConfig()
  }

  override def processElement(inRow: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{

      var startCalcTimestamp = 0l

      val offlineAutoLimitIndicatorConfig = toBean[OfflineAutoLimitIndicatorConfig](inRow)

      // todo 1- 解析 所有的 ConditionEntity : specId -> List[ConditionEntity]
      val specConfigConditionEntityMap = OfflineAutoLimitUtil.parseConfigConditionEntity(offlineAutoLimitIndicatorConfig)

      // todo 2-解析 所有的 OffLineAutoLimitOneConfig (isCalcLimit == true) : specId -> OffLineAutoLimitOneConfig
      val  offlineAutoLimitOneConfigMap = OfflineAutoLimitUtil.paseOffLineAutoLimitOneConfig(offlineAutoLimitIndicatorConfig)

      // todo 3- 查询Indicator数据并根据 every 采样
      val startQueryIndicatorTimestamp = System.currentTimeMillis()
      logger.warn(s"---------------------start getIndicatorData : ${offlineAutoLimitIndicatorConfig.controlPlanId}-${offlineAutoLimitIndicatorConfig.version}-${offlineAutoLimitIndicatorConfig.indicatorId}:${startQueryIndicatorTimestamp}")
      val sampleAutoLimitIndicatorResultList = OfflineAutoLimitUtil.getAutoLimitIndicatorResultData(offlineAutoLimitIndicatorConfig)
      val endQueryIndicatorTimestamp = System.currentTimeMillis()
      logger.warn(s"---------------------end getIndicatorData : ${offlineAutoLimitIndicatorConfig.controlPlanId}-${offlineAutoLimitIndicatorConfig.version}-${offlineAutoLimitIndicatorConfig.indicatorId}:${endQueryIndicatorTimestamp} ; cost Time == ${endQueryIndicatorTimestamp - startQueryIndicatorTimestamp}")

      // todo 4- 匹配condition
      if(sampleAutoLimitIndicatorResultList.nonEmpty){

        // 缓存specId -> List[AutoLimitIndicatorResult]
        val specAutoLimitIndicatorResultMap = TrieMap[String,ListBuffer[AutoLimitIndicatorResult]]()

        val isCalcSpecList = offlineAutoLimitOneConfigMap.keys.toList

        sampleAutoLimitIndicatorResultList.foreach(autoLimitIndicatorResult => {

          // todo 4-1 获取当前indicator 的conditionEntityList
          val indicatorConditionEntityList = OfflineAutoLimitUtil.getIndicatorConditionEntity(autoLimitIndicatorResult)

          // todo 4-2 计算出匹配每个spec 的得分
          var resultSpecId =  ""
          var resultMaxScore = -1
          specConfigConditionEntityMap.foreach(configConditionEntityTuple => {
            val currentSpecId = configConditionEntityTuple._1
            val configConditionEntity = configConditionEntityTuple._2
            indicatorConditionEntityList.foreach(indicatorConditionEntity => {

              val currentScore: Int = MatchingLimitConditionFunctions.satisfied(configConditionEntity, indicatorConditionEntity)
              if (currentScore>resultMaxScore){
                resultSpecId = currentSpecId
                resultMaxScore = currentScore
              }

              logger.error(s"indicatorConditionEntity = ${indicatorConditionEntity}")
              logger.error(s"configConditionEntity = ${configConditionEntity}")
              logger.error(s"resultSpecId = ${resultSpecId}")
              logger.error(s"currentScore = ${currentScore}")
              logger.error(s"resultMaxScore = ${resultMaxScore}")

            })
          })


          // todo 4-3 收集匹配度最高的specID 存入该sepcID的集合中
          if(isCalcSpecList.contains(resultSpecId) && resultMaxScore >= 0){

            if(specAutoLimitIndicatorResultMap.contains(resultSpecId)){
              val autoLimitIndicatorResultList = specAutoLimitIndicatorResultMap.get(resultSpecId).get
              autoLimitIndicatorResultList.append(autoLimitIndicatorResult)
              specAutoLimitIndicatorResultMap.put(resultSpecId,autoLimitIndicatorResultList)
            }else{
              val autoLimitIndicatorResultList = ListBuffer[AutoLimitIndicatorResult](autoLimitIndicatorResult)
              specAutoLimitIndicatorResultMap.put(resultSpecId,autoLimitIndicatorResultList)
            }
          }
        })

        // todo 5- 开始每个计算SpecId的AutoLimit
        startCalcTimestamp = System.currentTimeMillis()
        logger.warn(s"---------------------start CalcAutoLimit : ${offlineAutoLimitIndicatorConfig.controlPlanId}-${offlineAutoLimitIndicatorConfig.version}-${offlineAutoLimitIndicatorConfig.indicatorId}:${startCalcTimestamp}")

        offlineAutoLimitOneConfigMap.foreach( specIdOfflineAutoLimitOneConfig => {
          val specId = specIdOfflineAutoLimitOneConfig._1
          val offlineAutoLimitOneConfig = specIdOfflineAutoLimitOneConfig._2

          if(specAutoLimitIndicatorResultMap.contains(specId)){
            val autoLimitIndicatorResultList = specAutoLimitIndicatorResultMap.get(specId).get
            if(autoLimitIndicatorResultList.nonEmpty){
              logger.warn(s"1--normal calc--indicatorId == ${offlineAutoLimitOneConfig.indicatorId};specId == ${specId};indicator length == ${autoLimitIndicatorResultList.length}")
              // todo 正常值计算 并返回结果
              val offlineAutoLimitResult = OfflineAutoLimitUtil.calcAutoLimit(offlineAutoLimitOneConfig, autoLimitIndicatorResultList)
              logger.warn(s"offlineAutoLimitResult == ${offlineAutoLimitResult}")
              val jsonNode = beanToJsonNode[OfflineAutoLimitResult](offlineAutoLimitResult)
              collector.collect(jsonNode)
            }else{
              logger.warn(s"2--Exception1--indicatorId == ${offlineAutoLimitOneConfig.indicatorId};specId == ${specId};indicator length == ${autoLimitIndicatorResultList.length}")

              // todo 返回空结果
              val emptyOfflineAutoLimitResult = OfflineAutoLimitUtil.getEmptyOfflineAutoLimitResult(offlineAutoLimitOneConfig)
              val jsonNode = beanToJsonNode[OfflineAutoLimitResult](emptyOfflineAutoLimitResult)
              collector.collect(jsonNode)
            }

          }else {

            logger.warn(s"3--Exception2--indicatorId == ${offlineAutoLimitOneConfig.indicatorId};specId == ${specId};indicator length == 0")

            // todo 返回空结果
            val emptyOfflineAutoLimitResult = OfflineAutoLimitUtil.getEmptyOfflineAutoLimitResult(offlineAutoLimitOneConfig)

            val jsonNode = beanToJsonNode[OfflineAutoLimitResult](emptyOfflineAutoLimitResult)
            collector.collect(jsonNode)
          }

        })
      }else{
        // todo 返回空的结果
        offlineAutoLimitOneConfigMap.foreach(offlineAutoLimitOneConfigTuple => {

          val offlineAutoLimitOneConfig = offlineAutoLimitOneConfigTuple._2
          logger.warn(s"4--Exception3--indicatorId == ${offlineAutoLimitOneConfig.indicatorId};specId == ${offlineAutoLimitOneConfig.specId};indicator length == 0")
          val emptyOfflineAutoLimitResult = OfflineAutoLimitUtil.getEmptyOfflineAutoLimitResult(offlineAutoLimitOneConfig)
          val jsonNode = beanToJsonNode[OfflineAutoLimitResult](emptyOfflineAutoLimitResult)
          collector.collect(jsonNode)
        })
      }
      val endCalcTimestamp = System.currentTimeMillis()
      logger.warn(s"---------------------end CalcAutoLimit : ${offlineAutoLimitIndicatorConfig.controlPlanId}-${offlineAutoLimitIndicatorConfig.version}-${offlineAutoLimitIndicatorConfig.indicatorId}:${endCalcTimestamp} ; cost Time == ${endCalcTimestamp - startCalcTimestamp}")

    }catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"OfflineAutoLimit calc error")
      }
    }
  }

}
