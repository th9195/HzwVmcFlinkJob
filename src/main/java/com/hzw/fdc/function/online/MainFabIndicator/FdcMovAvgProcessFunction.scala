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
 *  对某个Indicator计算当前值与某些个历史值得平均数
 *  Formula：[Ind(n) + Ind(n-1) + Ind(n-2)+…+ Ind(n-X)]/(X+1)，其中Ind(n)表示某个Indictaor的当前值，Ind(n-1) ，
 *  Ind(n-2)，… ，Ind(n-x)表示当前值前的X个历史值
 */
class FdcMovAvgProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
  ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcMovAvgProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": { "version": { "MovAvgX" : "IndicatorConfig"}}}}
  var MovAvgIndicatorByAll: TrieMap[String, TrieMap[String, TrieMap[String, TrieMap[String, IndicatorConfig]]]] = IndicatorByAll

  //数据缓存: indicatorid ->（tool -> MovAvgValueList)
  var MovAvgValue = new mutable.HashMap[String, mutable.HashMap[String, List[Double]]]()

  val calcType = "movAvg"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

//    val initIndicatorHbase = new InitIndicatorOracle()
val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)
//    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList

    initConfig.foreach(kafkaDatas=> {
      kafkaDatas.`dataType` match {
        case "indicatorconfig" =>
          addIndicatorConfigByAll(kafkaDatas, "MA")

        case _ => logger.warn(s"DriftIndicator job open no mach type: " + kafkaDatas.`dataType`)
      }
    })

    if(ProjectConfig.INIT_ADVANCED_INDICATOR_FROM_REDIS){
      initAdvancedIndicator(calcType,this.MovAvgValue)
//      logger.warn(s"MovAvgValue == ${MovAvgValue.toJson}")
    }

//    logger.warn("MovAvgIndicatorByAll" + MovAvgIndicatorByAll)
  }

  override def processElement(record: FdcData[IndicatorResult], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#ReadOnlyContext, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "indicator" =>
          val myDatas = record.datas

          val indicatorResult = myDatas.toJson.fromJson[IndicatorResult]
          val version = indicatorResult.controlPlanVersion.toString
          val indicatorKey = indicatorResult.indicatorId.toString

          if (!this.MovAvgIndicatorByAll.contains(indicatorKey)) {
            return
          }
          val controlPlanIdMap = MovAvgIndicatorByAll(indicatorKey)
          for(versionMap <- controlPlanIdMap.values) {
            //            val versionMap = MovAvgIndicatorByAll(indicatorKey)
            breakable {
              if (!versionMap.contains(version)) {
                logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                  -> versionMap.keys, "match version" -> version, "type" -> "MA"), "indicator_contains version no exist").toJson)
                break
              }
            }
            //            val MovAvgIndicator = versionMap(version)

            //            val controlPlanVersion = versionMap.keys.map(_.toLong).max
            val MovAvgIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version.toString)


            val MaxMovAvgX = MovAvgIndicator.keys.map(_.toInt).max
            val w2wType = MovAvgIndicator(MaxMovAvgX.toString).w2wType

            val indicatorValueDouble = indicatorResult.indicatorValue.toDouble
//            CacheHistoryData(w2wType, indicatorResult, indicatorKey, indicatorValueDouble, MaxMovAvgX, this.MovAvgValue)

            cacheHistoryDataAndRedis(w2wType,
              indicatorResult,
              indicatorKey,
              indicatorValueDouble,
              MaxMovAvgX,
              this.MovAvgValue,
              calcType,
              ctx)

            val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()
            for ((movAvgX, indicatorConfigScala) <- MovAvgIndicator) {
              try {
                val res = MovAvgFunction(movAvgX, indicatorResult, indicatorConfigScala)
                if (res._1 != null) {
                  IndicatorList.append(res)
                }
              } catch {
                case ex: Exception => logger.warn(s"MovAvgFunction error $ex  config:$indicatorConfigScala  indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          }
        case _ => logger.warn(s"MovAvgIndicator job processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "MovAvg"), s"indicator job error $ex").toJson)
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                                       out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" =>
        addIndicatorConfigByAll(value, "MA")

      case _ => logger.warn(s"MovAvgIndicator job processBroadcastElement no mach type: " + value.`dataType`)
    }
  }



  /**
   *  计算MovAvg值
   */
  def MovAvgFunction(movAvgX: String, indicatorResult: IndicatorResult, config: IndicatorConfig
                    ): (IndicatorResult, Boolean, Boolean, Boolean) = {

    val nowMovAvgList: List[Double] = getCacheValue(indicatorResult, config, this.MovAvgValue)

    val max = movAvgX.toInt + 1
    if(max > nowMovAvgList.size){
      logger.warn(s"MovAvgFunction movAvgXInt > nowMovAvgList.size config:$config  indicatorResult:$indicatorResult")
      return (null,config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }

    //提取列表的后n个元素
    val mathList: List[Double] = nowMovAvgList.takeRight(max)
    val Result: Double = mathList.sum / max

    val movAvgIndicator = IndicatorResult(
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
      indicatorValue =   Result.toString,
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



    (movAvgIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }

}

