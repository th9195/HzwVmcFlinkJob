package com.hzw.fdc.function.online.MainFabIndicator

import com.hzw.fdc.json.MarshallableImplicits.{Marshallable, Unmarshallable}
import com.hzw.fdc.scalabean.{ConfigData, ErrorCode, FdcData, IndicatorConfig, IndicatorResult}
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class FdcAbsProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig],
  ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]] with IndicatorCommFunction {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcAbsProcessFunction])

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
          addIndicatorConfigByAll(kafkaDatas, "abs")

        case _ => logger.warn(s"AbsIndicator open no mach type: " + kafkaDatas.`dataType`)

      }
    })
  }

  override def processElement(record: FdcData[IndicatorResult], readOnlyContext: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#ReadOnlyContext,
                              out: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    try {
      record.`dataType` match {
        case "indicator" =>
          val myDatas = record.datas

          val indicatorResult = myDatas.toJson.fromJson[IndicatorResult]
          val version = indicatorResult.controlPlanVersion.toString
          val indicatorKey = indicatorResult.indicatorId.toString
          if (IndicatorByAll.contains(indicatorKey)){
            val controlPlanIdMap: mutable.Map[String, TrieMap[String, TrieMap[String, IndicatorConfig]]] = IndicatorByAll(indicatorKey)
            for(versionMap <- controlPlanIdMap.values) {

              breakable {
                if (!versionMap.contains(version)) {
                  logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "exit version"
                    -> versionMap.keys, "match version" -> version, "type" -> "indicatorAbs"), "indicator_contains version no exist").toJson)
                  break
                }
              }

              val AbsIndicator: concurrent.TrieMap[String, IndicatorConfig] = versionMap(version)

              val indicatorValue = Math.abs(indicatorResult.indicatorValue.toDouble).toString



              val IndicatorList = ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]()

              for ((k,indicatorConfig) <- AbsIndicator) {
                val res=indicatorResult.copy(
                  indicatorValue = indicatorValue,
                  unit =  "",
                  bypassCondition =   indicatorConfig.bypassCondition,
                  indicatorName =   indicatorConfig.indicatorName,
                  algoClass =   indicatorConfig.algoClass,
                  indicatorId =   indicatorConfig.indicatorId
                )
                IndicatorList.append((res, indicatorConfig.driftStatus, indicatorConfig.calculatedStatus, indicatorConfig.logisticStatus))
              }

              out.collect(IndicatorList)
            }
          }

        case _ => logger.warn(s"AbsIndicator processElement no mach type")
      }
    } catch {
      case ex: Exception => {
        logger.warn(ErrorCode("005008d001C", System.currentTimeMillis(), Map("record" -> record, "type" -> "indicatorAbs"),
          s"indicator job Mach error $ex").toJson)
        logger.warn(ExceptionInfo.getExceptionInfo(ex))
      }
    }
  }

  override def processBroadcastElement(value: ConfigData[IndicatorConfig], context: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult],
    ConfigData[IndicatorConfig], ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]#Context,
                                       collector: Collector[ListBuffer[(IndicatorResult, Boolean, Boolean, Boolean)]]): Unit = {
    value.`dataType` match {
      case "indicatorconfig" =>
        addIndicatorConfigByAll(value, "abs")

      case _ => logger.warn(s"AbsIndicator job processBroadcastElement no mach type: " + value.`dataType`)

    }
  }
}
