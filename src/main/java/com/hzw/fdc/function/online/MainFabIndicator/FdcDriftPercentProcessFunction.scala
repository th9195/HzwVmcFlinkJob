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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
import scala.util.control.Breaks._


/**
 * Drift(Percent） = [Ind(N)-Ind(N-M)]/Ind(N). (以示区别，名字更改为Drift(Percent))
 * Formula：[Ind(n) – Ind(n-X)]/Ind(n)，即n是当前值，n-X是前面X个值
 */
class FdcDriftPercentProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
  ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcDriftPercentProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": { "version": { "driftX" : "IndicatorConfig"}}}}
  var driftPercentIndicatorByAll: TrieMap[String, TrieMap[String, TrieMap[String, TrieMap[String, IndicatorConfig]]]] = IndicatorByAll

  //drift算法, 数据缓存: indicatorid ->（tool -> driftValueList)
  var driftPercentValue = new mutable.HashMap[String, mutable.HashMap[String, List[Double]]]()

  val calcType = "driftPercent"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

//    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList

    initConfig.foreach(kafkaDatas=> {
      kafkaDatas.`dataType` match {
        case "indicatorconfig" =>
          addIndicatorConfigByAll(kafkaDatas, "driftPercent")

        case _ => logger.warn(s"DriftIndicator job open no mach type: " + kafkaDatas.`dataType`)
      }
    })

    if(ProjectConfig.INIT_ADVANCED_INDICATOR_FROM_REDIS){
      initAdvancedIndicator(calcType,this.driftPercentValue)
//      logger.warn(s"driftPercentValue == ${driftPercentValue.toJson}")
    }
//    logger.warn("driftPercentIndicatorByAll" + driftPercentIndicatorByAll)
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

          val indicatorValueDouble = indicatorResult.indicatorValue
          if (!this.driftPercentIndicatorByAll.contains(indicatorKey)) {
            return
          }

          //          val versionMap = driftPercentIndicatorByAll(indicatorKey)

          //          val driftIndicator = versionMap(version)

          val controlPlanIdMap = driftPercentIndicatorByAll(indicatorKey)
          for(versionMap <- controlPlanIdMap.values) {

            breakable {
              if (!versionMap.contains(version)) {
                logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                  -> versionMap.keys, "match version" -> version, "type"-> "driftPercent"), "indicator_contains version no exist").toJson)
                break
              }
            }

            //            val controlPlanVersion = versionMap.keys.map(_.toLong).max
            val driftIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version.toString)


            if (driftIndicator.isEmpty) {
              logger.warn(s"indicatorMap isEmpty: " + indicatorKey)
              return
            }

            //取出最大的dritfx
            val max = driftIndicator.keys.map(_.toInt).max
            val w2wType = driftIndicator(max.toString).w2wType

//            CacheHistoryData(w2wType, indicatorResult, indicatorKey, indicatorValueDouble.toDouble, max, this.driftPercentValue)

            cacheHistoryDataAndRedis(w2wType,
              indicatorResult,
              indicatorKey,
              indicatorValueDouble.toDouble,
              max,
              this.driftPercentValue,
              calcType,
              ctx)

            val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()
            for ((k, v) <- driftIndicator) {
              try {
                val res = driftFunction(k, indicatorResult, v)
                if (res._1 != null) {
                  IndicatorList.append(res)
                }
              } catch {
                case ex: Exception => logger.warn(s"driftFunction error $ex  config:$v  indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          }
        case _ => logger.warn(s"DriftIndicator job DriftPercentProcessElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "indicatorAvg"), s"indicator job Mach error $ex").toJson)
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                                       out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" =>
        addIndicatorConfigByAll(value, "driftPercent")

      case _ => logger.warn(s"DriftIndicator job processBroadcastElement no mach type: " + value.`dataType`)
    }
  }

  /**
   * Drift（X）：对某个Indicator计算当前值与历史值的差
   * Formula：Ind_n – Ind_(n-X)/Ind_n，其中Ind_n表示某个Indictaor的当前值， Ind_(n-X)表示当前值前的第X个历史值
   */
  def driftFunction(driftX: String, indicatorResult: IndicatorResult, config: IndicatorConfig): (IndicatorResult, Boolean, Boolean, Boolean) = {

    val DriftValueList: List[Double] = getCacheValue(indicatorResult, config, this.driftPercentValue)

    var driftResult: Double = 0.00
    val driftXInt = driftX.toInt
    val size = DriftValueList.size
    val Index = size - 1 - driftXInt

    if (Index >= 0) {
      driftResult = (DriftValueList.last - DriftValueList(Index)) / DriftValueList.last
    }else{
      logger.warn(s"driftFunction Index >= 0 config:$config  indicatorResult:$indicatorResult")
      return (null, config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }

    val  DriftIndicator = IndicatorResult(
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
      indicatorValue =   driftResult.toString,
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

    (DriftIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}

