package com.hzw.fdc.function.online.MainFabIndicator


import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


/**
 *  对某个Indicator计算当前值与某些个历史值的移动加权平均数
 *  Formula：EWMA(n) = λInd(n) +（1-λ）*EWMA(n-1)，其中Ind(n)表示某个Indictaor的当前值，
 *  λ为系数，EWMA(n-1)表示该Indicator对应的上一个点的EWMA值
 */
class FdcEWMAProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
  ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcEWMAProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": { "version": { "λ" : "IndicatorConfig"}}}
  var EWMAIndicatorByAll: TrieMap[String, TrieMap[String, TrieMap[String, TrieMap[String, IndicatorConfig]]]] = IndicatorByAll

  //数据缓存: indicatorid ->（tool -> EWMAValueList)
  var EWMAValue = new TrieMap[String, TrieMap[String, List[Double]]]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

//    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList

    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

    initConfig.foreach(kafkaDatas=> {
      kafkaDatas.`dataType` match {
        case "indicatorconfig" =>
          addIndicatorConfigByAll(kafkaDatas, "EWMA")

        case _ => logger.warn(s"EWMAIndicator open no mach type: " + kafkaDatas.`dataType`)
      }
    })
//    logger.warn("EWMAIndicatorByAll" + EWMAIndicatorByAll)
  }

  override def processElement(record: FdcData[IndicatorResult], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#ReadOnlyContext,
                              out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "indicator" =>
          val myDatas = record.datas

          val indicatorResult = myDatas.toJson.fromJson[IndicatorResult]
          val version = indicatorResult.controlPlanVersion.toString
          val indicatorKey = indicatorResult.indicatorId.toString

          if (!this.EWMAIndicatorByAll.contains(indicatorKey)) {
            return
          }

          //          val versionMap = EWMAIndicatorByAll(indicatorKey)

          //          val EWMAIndicator = versionMap(version)
          val controlPlanIdMap = EWMAIndicatorByAll(indicatorKey)
          for(versionMap <- controlPlanIdMap.values) {

            breakable {
              if (!versionMap.contains(version)) {
                logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                  -> versionMap.keys, "match version" -> version, "type" -> "EWMA"), "indicator_contains version no exist").toJson)
                break
              }
            }

            //            val controlPlanVersion = versionMap.keys.map(_.toLong).max
            val EWMAIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version.toString)


            val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()
            for ((λ, indicatorConfigScala) <- EWMAIndicator) {
              try {
                IndicatorList.append(EWMAFunction(λ.toDouble, indicatorResult, indicatorConfigScala))
              } catch {
                case ex: Exception => logger.warn(s"EWMAFunction error $ex  config:$indicatorConfigScala  indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          }
        case _ => logger.warn(s"EWMAIndicator processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "EWMA"), s"indicator job error $ex").toJson)
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" => addIndicatorConfigByAll(value, "EWMA")

      case "offlineConfig" => parseOfflineConfig(value)

      case _ => logger.warn(s"EWMAIndicator job processBroadcastElement no mach type: " + value.`dataType`)
    }
  }

  /**
   *   解析离线配置, 清除缓存
   */
  def parseOfflineConfig(value: ConfigData[IndicatorConfig]): Unit = {
    val offlineConfigScala = value.datas
    val indicatorId = offlineConfigScala.indicatorId
    this.EWMAValue.remove(indicatorId.toString)
  }

  /**
   * 获取缓存的历史数据，用于计算
   */
  def getCacheHistoryData(indicatorResult: IndicatorResult, config: IndicatorConfig, indicatorValueDouble: Double, λ: Double): Double = {
    val toolMap = new TrieMap[String, List[Double]]()

    val toolName = indicatorResult.toolName
    val chamberName = indicatorResult.chamberName
    val recipeName = indicatorResult.recipeName

    val byChamber = toolName + "|" + chamberName
    val byRecipe = toolName + "|" + chamberName + "|" + recipeName

    val indicatorKey = config.indicatorId.toString
    var NowEWMAVale = λ * indicatorValueDouble

    // 判断indicatorId 之前是否存在
    if (!this.EWMAValue.contains(indicatorKey)) {
      toolMap += byChamber -> List(NowEWMAVale)
      toolMap += byRecipe -> List(NowEWMAVale)

      this.EWMAValue += indicatorKey -> toolMap
    } else {
      val toolMap = this.EWMAValue(indicatorKey)

      // 判断toolName + chamberName 之前是否存在
      val chamberData = byFilter(byChamber, toolMap, NowEWMAVale, indicatorKey, λ)

      // 判断toolName + chamberName +recipe 之前是否存在
      val recipeData = byFilter(byRecipe, toolMap, NowEWMAVale, indicatorKey, λ)

      NowEWMAVale = if(config.w2wType == "By Tool-Chamber-Recipe") recipeData else chamberData
    }
    NowEWMAVale
  }

  def byFilter(Name: String, toolMap: TrieMap[String, List[Double]], NowEWMAVale: Double, indicatorKey: String, λ: Double): Double= {
    var res = NowEWMAVale
    // 判断Name之前是否存在
    if (!toolMap.contains(Name)) {
      toolMap += Name -> List(NowEWMAVale)
      this.EWMAValue += indicatorKey -> toolMap

    } else {
      val EWMAList: List[Double] = toolMap(Name)
      res = NowEWMAVale + (1 - λ) * EWMAList.last

      val newEWMAList = EWMAList :+ res
      val dropEWMAList = if (newEWMAList.size > 4) {
        newEWMAList.drop(1)
      } else {
        newEWMAList
      }

      toolMap += Name -> dropEWMAList
      this.EWMAValue += indicatorKey -> toolMap
    }
    res
  }


  /**
   *  计算EWMA值
   */
  def EWMAFunction(λ: Double, indicatorResult: IndicatorResult, config: IndicatorConfig): (IndicatorResult, Boolean, Boolean, Boolean) = {

    val indicatorValueDouble = indicatorResult.indicatorValue

    val NowEWMAVale = getCacheHistoryData(indicatorResult, config, indicatorValueDouble.toDouble, λ)

    val EWMAIndicator = IndicatorResult(
      controlPlanId =   indicatorResult.controlPlanId,
      controlPlanName =   indicatorResult.controlPlanName,
      controlPlanVersion =   indicatorResult.controlPlanVersion,
      locationId =   indicatorResult.locationId,
      locationName =   indicatorResult.locationName,
      moduleId =   indicatorResult.moduleId,
      moduleName =   indicatorResult.moduleName,
      toolGroupId =   indicatorResult.toolGroupId,
      toolGroupName =   indicatorResult.toolGroupName,
      chamberGroupId =   indicatorResult.chamberGroupId,
      chamberGroupName =   indicatorResult.chamberGroupName,
      recipeGroupName =   indicatorResult.recipeGroupName,
      runId =   indicatorResult.runId,
      toolName =   indicatorResult.toolName,
      toolId = indicatorResult.toolId,
      chamberName =   indicatorResult.chamberName,
      chamberId = indicatorResult.chamberId,
      indicatorValue =   NowEWMAVale.toString,
      indicatorId =   config.indicatorId,
      indicatorName =   config.indicatorName,
      algoClass =   config.algoClass,
      indicatorCreateTime =   indicatorResult.indicatorCreateTime,
      missingRatio =   indicatorResult.missingRatio,
      configMissingRatio =   indicatorResult.configMissingRatio,
      runStartTime =   indicatorResult.runStartTime,
      runEndTime =   indicatorResult.runEndTime,
      windowStartTime =   indicatorResult.windowStartTime,
      windowEndTime =   indicatorResult.windowEndTime,
      windowDataCreateTime =   indicatorResult.windowDataCreateTime,
      limitStatus =   indicatorResult.limitStatus,
      materialName =   indicatorResult.materialName,
      recipeName =   indicatorResult.recipeName,
      recipeId = indicatorResult.recipeId,
      product =   indicatorResult.product,
      stage =   indicatorResult.stage,
      bypassCondition =   config.bypassCondition,
      pmStatus =   indicatorResult.pmStatus,
      pmTimestamp =   indicatorResult.pmTimestamp,
      area = indicatorResult.area,
      section = indicatorResult.section,
      mesChamberName = indicatorResult.mesChamberName,
      lotMESInfo = indicatorResult.lotMESInfo,
      dataVersion =indicatorResult.dataVersion,
      cycleIndex = indicatorResult.cycleIndex,
      unit =  ""
    )

    (EWMAIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}


