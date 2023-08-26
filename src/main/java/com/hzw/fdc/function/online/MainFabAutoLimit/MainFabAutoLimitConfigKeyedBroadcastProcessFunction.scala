package com.hzw.fdc.function.online.MainFabAutoLimit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.PublicFunction.limitCondition.ConditionEntity
import com.hzw.fdc.function.PublicFunction.limitCondition.MatchingLimitConditionFunctions.{produce, satisfied}
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：Created in 2021/6/11 15:59
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitConfigKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,JsonNode,JsonNode,(AutoLimitOneConfig,IndicatorResult)]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitConfigKeyedBroadcastProcessFunction])

  lazy val outputTagByCount = new OutputTag[(AutoLimitOneConfig, IndicatorResult)]("ByCount")

  //最外层key改为 indicatorId + controlPlanVersion，里层key为 specId
  private val outerMap = new TrieMap[String, TrieMap[String,(ConditionEntity,AutoLimitOneConfig)]]()

  override def open(parameters: Configuration): Unit = {
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    /**
     * 需求-FAB7-55 auto limit 开启，controlplan激活后只计算一次
     */
//    val alarmConfigs = InitFlinkFullConfigHbase.AlarmConfigList
//    alarmConfigs.foreach(addLimitConditionConfig(_))
//
//    val configs: ListBuffer[ConfigData[AutoLimitConfig]] = InitFlinkFullConfigHbase.AutoLimitConfigList
//    configs.foreach(x => {addAutoLimitConfig(x.datas)})
  }


  override def processElement(in1: JsonNode,
                              readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (AutoLimitOneConfig, IndicatorResult)]#ReadOnlyContext,
                              collector: Collector[(AutoLimitOneConfig, IndicatorResult)]): Unit = {


    val indicatorResult = toBean[FdcData[IndicatorResult]](in1).datas
    val outerKey = geneAlarmKey(indicatorResult.indicatorId.toString , indicatorResult.controlPlanVersion.toString)
    val option = outerMap.get(outerKey)
    val innerMap = if (option.isEmpty){
      return
    }else{
      option.get
    }
    val tp = innerMap.values

    val entities = produce(indicatorResult)

    if (tp.size>0) {
      if (entities.size>1) logger.error(s"indicator:${indicatorResult.indicatorId}的product或stage不唯一${indicatorResult.toString}")
      val suitedTuples = tp.flatMap(t2 =>
          entities.map(entity =>
            (t2, satisfied(t2._1, entity))
          )
        ).filter(_._2 >= 0)
      val configOne = if (suitedTuples.isEmpty) null else suitedTuples.maxBy(_._2)._1._2

      if (configOne!=null) {
        if (configOne.triggerMethodName == MainFabConstants.triggerMethodNameByCount) {
          readOnlyContext.output(outputTagByCount, (configOne, indicatorResult))
        } else if (configOne.triggerMethodName == MainFabConstants.triggerMethodNameByTime) {
          //byTime
          collector.collect((configOne, indicatorResult))
        } else {
          logger.warn(s"AutoLimitConfig error triggerMethodName: ${configOne.triggerMethodName}")
        }
      }
    }
  }


  override def processBroadcastElement(in2: JsonNode,
                                       context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, (AutoLimitOneConfig, IndicatorResult)]#Context,
                                       collector: Collector[(AutoLimitOneConfig, IndicatorResult)]): Unit = {

    try {
      in2.get("dataType").asText() match {
        case "autoLimitSettings" =>
          val autoLimitSetting: autoLimitSettings = toBean[autoLimitSettings](in2)
          if (autoLimitSetting.status) {
            addAutoLimitConfig(autoLimitSetting.datas)
          } else {
            for (elem: DeploymentInfo <- autoLimitSetting.datas.deploymentInfo) {
              val outerKey =geneAlarmKey(elem.indicatorId.toString,autoLimitSetting.datas.version.toString)
              val innerKey = elem.specId.toString
              val innerMap = outerMap.getOrElse(outerKey, new TrieMap())
              if (innerMap.contains(innerKey)){
                val entity = innerMap.get(innerKey).get._1
                innerMap.put(innerKey,(entity,null))
              }
              outerMap.put(outerKey,innerMap)
            }
          }
        case "alarmconfig" =>
          val config = toBean[ConfigData[AlarmRuleConfig]](in2)
          addLimitConditionConfig(config)
        case _ =>
//          logger.warn(s"不能识别的配置数据:${in2.toString}")
      }
    } catch {
      case e:Exception =>logger.error(s"MainFabLog: addAutoLimitConfig error: $e config :$in2")
    }

  }

  /**
   * 添加limit condition配置
   * @param config
   */
  def addLimitConditionConfig(config:ConfigData[AlarmRuleConfig]): Unit ={
    val alarmRuleConfig = config.datas
    val entity: ConditionEntity = produce(alarmRuleConfig)
    val outerKey =geneAlarmKey(alarmRuleConfig.indicatorId , alarmRuleConfig.controlPlanVersion.toString)
    val key = alarmRuleConfig.specDefId
    val innerMap=outerMap.getOrElse(outerKey,new TrieMap())
    if (config.status) {
      val (oldEntity, autoLimitOneConfig) = innerMap.getOrElse(key,(null,null))
      if (oldEntity==null){
        innerMap.put(key,(entity,autoLimitOneConfig))
      }else{
        innerMap.put(key,(oldEntity.join(entity),autoLimitOneConfig))
      }
      outerMap.put(outerKey,innerMap)
      cleanOldVersion(alarmRuleConfig)
    }else if (innerMap.contains(key)){
      innerMap.remove(key)
    }
  }

  /**
   * 添加auto limit配置
   * @param autoLimitOneConfig
   */
  def addAutoLimitConfig(autoLimitOneConfig:AutoLimitConfig){
    for (elem <- autoLimitOneConfig.deploymentInfo) {
      if(!elem.equals("selfDefined")){

        var triggerMethodValue = autoLimitOneConfig.triggerMethodValue
        if(autoLimitOneConfig.triggerMethodName == MainFabConstants.triggerMethodNameByTime &&
          ProjectConfig.AUTO_LIMIT_BY_TIME_NUM != null){
          triggerMethodValue = ProjectConfig.AUTO_LIMIT_BY_TIME_NUM
        }

        val one=AutoLimitOneConfig(
          controlPlanId = autoLimitOneConfig.controlPlanId,
          version = autoLimitOneConfig.version,
          removeOutlier = autoLimitOneConfig.removeOutlier,
          maxRunNumber = autoLimitOneConfig.maxRunNumber,
          every = autoLimitOneConfig.every,
          triggerMethodName = autoLimitOneConfig.triggerMethodName,
          triggerMethodValue = triggerMethodValue,
          active = autoLimitOneConfig.active,
          indicatorId = elem.indicatorId,
          specId = elem.specId,
          condition = elem.condition,
          limitMethod = elem.limitMethod,
          limitValue = elem.limitValue,
          cpk = elem.cpk,
          target = elem.target
        )

        val outerKey=geneAlarmKey(elem.indicatorId.toString , autoLimitOneConfig.version.toString)
        val innerMap = outerMap.getOrElse(outerKey, new TrieMap())
        val key = one.specId.toString
        val entity = innerMap.getOrElse(key,(null,null))._1

        innerMap.put(key,(entity,one))

        outerMap.put(outerKey,innerMap)

        logger.warn(s"MainFabLog: addAutoLimit config :$one ")
      }else{
        logger.warn(s"MainFabLog: addAutoLimit error config :$elem ")
      }
    }
  }

  /**
   * 清除旧版本数据indicator,目前只保留两个版本的indicator
   * @param config
   */
  def cleanOldVersion(config:AlarmRuleConfig): Unit ={
    outerMap.keySet
      .filter(key=>{
        val strings = key.split(",")
        strings(0)==config.indicatorId && strings(1).toLong<=config.controlPlanVersion-2
      })
      .foreach(outerMap.remove(_))
  }

  /**
   * 获取key
   * @return
   */
  def geneAlarmKey(ags: String*): String = {
    ags.reduceLeft((a, b) => s"$a,$b")
  }

}
