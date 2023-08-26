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
 * @author gdj
 * @create 2020-09-01-18:01
 *
 */
class FdcDriftConfigBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
  ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcDriftConfigBroadcastProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": {"version": { "driftX" : "IndicatorConfig"}}}}
  var driftIndicatorByAll: TrieMap[String, TrieMap[String, TrieMap[String, TrieMap[String, IndicatorConfig]]]] = IndicatorByAll

  //drift算法, 数据缓存: indicatorid ->((tool + chamber) or (tool+chamber+recipe) -> driftValueList)
  var driftValue = new mutable.HashMap[String, mutable.HashMap[String, List[Double]]]()

  val calcType = "drift"

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
          addIndicatorConfigByAll(kafkaDatas, "drift")

        case _ => logger.warn(s"DriftIndicator job open no mach type: " + kafkaDatas.`dataType`)
      }
    })

//    logger.warn("driftIndicatorByAll: " + driftIndicatorByAll)
    // 通过Redis初始化上次结果数据
    if(ProjectConfig.INIT_ADVANCED_INDICATOR_FROM_REDIS){
      initAdvancedIndicator(calcType,this.driftValue)
//      logger.warn(s"driftValue == ${driftValue.toJson}")
    }

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

          val indicatorValueDouble = indicatorResult.indicatorValue.toDouble
          if (!this.driftIndicatorByAll.contains(indicatorKey)) {
            logger.warn(s"indicatorMap no exit: " + indicatorResult.controlPlanName + "\t" + indicatorKey)
            return
          }

          val controlPlanIdMap = driftIndicatorByAll(indicatorKey)
          for(versionMap <- controlPlanIdMap.values) {
            breakable {
              if (!versionMap.contains(version)) {
                logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                  -> versionMap.keys, "match version" -> version, "type" -> "drift"), "indicator_contains version no exist").toJson)
                break
              }
            }
            //            val controlPlanVersion = versionMap.keys.map(_.toLong).max
            val driftIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version.toString)

            //            //取出最大的dritfx
            //            val max = driftIndicator.keys.map(_.toInt).max
            //            CacheHistoryData(indicatorResult, indicatorKey, indicatorValueDouble, max, this.driftValue)
            //取出最大的dritfx
            val max = driftIndicator.keys.map(_.toInt).max
            val w2wType = driftIndicator(max.toString).w2wType

//            CacheHistoryData(w2wType, indicatorResult, indicatorKey, indicatorValueDouble, max, this.driftValue)
            cacheHistoryDataAndRedis(w2wType,
              indicatorResult,
              indicatorKey,
              indicatorValueDouble,
              max,
              this.driftValue,
              calcType,
              ctx)

            val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()
            for ((k, v) <- driftIndicator) {
              try {
                val res = driftFunction(k, indicatorResult, v)
                if (res._1 != null) {
                  IndicatorList.append(res)
                }else{
                  logger.warn(s"FdcDrift job null: " + this.driftValue(indicatorResult.indicatorId.toString) + "\t" + record)
                }
              } catch {
                case ex: Exception => logger.warn(s"driftFunction error $ex  config:$v  indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          }
        case _ => logger.warn(s"DriftIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "drift"), s"indicator job error $ex").toJson)
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                                       out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" => addIndicatorConfigByAll(value, "drift")

      case _ => logger.warn(s"DriftIndicator job processBroadcastElement no mach type: " + value.`dataType`)
    }
  }


  /**
   * Drift（X）：对某个Indicator计算当前值与历史值的差
   * Formula：Ind_n – Ind_(n-X)，其中Ind_n表示某个Indictaor的当前值， Ind_(n-X)表示当前值前的第X个历史值
   */
  def driftFunction(driftX: String, indicatorResult: IndicatorResult, config: IndicatorConfig):
  (IndicatorResult, Boolean, Boolean, Boolean) = {

    val DriftValueList = getCacheValue(indicatorResult, config, this.driftValue)

    val driftXInt = driftX.toInt
    val size = DriftValueList.size
    val Index = size - 1 - driftXInt

    val DriftIndicator = if (Index >= 0) {
      val driftResult = DriftValueList.last - DriftValueList(Index)
      IndicatorResult(
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
        unit = ""
      )
    } else {
      null
    }
    (DriftIndicator, config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}

