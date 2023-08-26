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
 *
 * IndicatorAvg = [Ind(n)+Ind(n-X)]/2
 * Formula：[Ind-n + Ind-(n-X)]/2，即n是当前值，n-X是前面X个值，选择着算法之后，需要弹出对话框：
 * Parameter               Type            Value
 * InputIndicator          string      Ind (注：该地方点击之后列出所有已建立的Indicator，并选择其中之一)
 * X                                 Int            10
 */
class FdcAvgProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
  ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcAvgProcessFunction])

  // 配置 {"indicatorid": {"controlPlanId": {  "version": { "AvgX" : "IndicatorConfig"}}}}
  var AvgIndicatorByAll: TrieMap[String, TrieMap[String, TrieMap[String, TrieMap[String, IndicatorConfig]]]] = IndicatorByAll


  //数据缓存: indicatorid ->（tool -> AvgValueList)
  var AvgValue = new mutable.HashMap[String, mutable.HashMap[String, List[Double]]]()

  val calcType = "avg"

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
          addIndicatorConfigByAll(kafkaDatas, "indicatorAvg")

        case _ => logger.warn(s"AvgIndicator open no mach type: " + kafkaDatas.`dataType`)
      }
    })

    if(ProjectConfig.INIT_ADVANCED_INDICATOR_FROM_REDIS){
      initAdvancedIndicator(calcType,this.AvgValue)
//      logger.warn(s"AvgValue == ${AvgValue.toJson}")
    }

//    logger.warn("AvgIndicatorByAll" + AvgIndicatorByAll)
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

          if (!this.AvgIndicatorByAll.contains(indicatorKey)) {
            return
          }

          //          val versionMap = AvgIndicatorByAll(indicatorKey)

          //          val AvgIndicator = versionMap(version)

          val controlPlanIdMap = AvgIndicatorByAll(indicatorKey)
          for(versionMap <- controlPlanIdMap.values) {

            breakable {
              if (!versionMap.contains(version)) {
                logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                  -> versionMap.keys, "match version" -> version, "type" -> "indicatorAvg"), "indicator_contains version no exist").toJson)
                break
              }
            }

            //            val controlPlanVersion = versionMap.keys.map(_.toLong).max
            val AvgIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version.toString)


            val AvgX = AvgIndicator.keys.map(_.toInt).max
            val w2wType = AvgIndicator(AvgX.toString).w2wType

            val indicatorValueDouble = indicatorResult.indicatorValue.toDouble
//            CacheHistoryData(w2wType, indicatorResult, indicatorKey, indicatorValueDouble.toDouble, AvgX, this.AvgValue)

            cacheHistoryDataAndRedis(w2wType,
              indicatorResult,
              indicatorKey,
              indicatorValueDouble,
              AvgX,
              this.AvgValue,
              calcType,
              ctx)

            val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()
            for ((avgX, indicatorConfigScala) <- AvgIndicator) {
              try {
                val res = AvgFunction(avgX, indicatorResult, indicatorConfigScala)
                if (res._1 != null) {
                  IndicatorList.append(res)
                }else{
                  logger.warn(s"FdcAvgProcessFunction job processElement no mach type" + this.AvgValue + "\t" + record)
                }
              } catch {
                case ex: Exception => logger.warn(s"AvgFunction error $ex  config:$indicatorConfigScala  indicatorResult:$indicatorResult ")
              }
            }
            out.collect(IndicatorList)
          }

        case _ => logger.warn(s"AvgIndicator processElement no mach type")
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(),
        Map("record" -> record, "type" -> "indicatorAvg"), s"indicator job Mach error $ex").toJson)
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], ctx: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context, out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" =>
        addIndicatorConfigByAll(value, "indicatorAvg")


      case _ => logger.warn(s"AvgIndicator job processBroadcastElement no mach type: " + value.`dataType`)
    }
  }

  /**
   *  计算Avg值
   */
  def AvgFunction(avgX: String, indicatorResult: IndicatorResult, config: IndicatorConfig): (IndicatorResult, Boolean, Boolean, Boolean) = {

    val nowAvgList: List[Double] = getCacheValue(indicatorResult, config, this.AvgValue)

    val max = avgX.toInt + 1
    if(max > nowAvgList.size){
      logger.warn(s"AvgFunction AvgXInt > nowAvgList.size config:$config  indicatorResult:$indicatorResult")
      return (null,config.driftStatus, config.calculatedStatus, config.logisticStatus)
    }

    //提取列表的后n个元素
    val mathList: List[Double] = nowAvgList.takeRight(max)
    val Result: Double = (mathList.head + mathList.last) / 2

    val avgIndicator = IndicatorResult(
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

    (avgIndicator,config.driftStatus, config.calculatedStatus, config.logisticStatus)
  }
}

