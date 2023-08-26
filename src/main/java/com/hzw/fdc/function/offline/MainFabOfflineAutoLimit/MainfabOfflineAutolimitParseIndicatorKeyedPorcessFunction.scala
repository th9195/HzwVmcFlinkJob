package com.hzw.fdc.function.offline.MainFabOfflineAutoLimit

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.scalabean.{OfflineAutoLimitConfig, OfflineAutoLimitIndicatorConfig, OfflineAutoLimitSettings, OfflineDeploymentInfo, QueryRunDataConfig}
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
 * MainfabOfflineAutolimitParseIndicatorKeyedPorcessFunction
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/23 11:18
 * @since 1.0.0
 * @update 2022/8/23 11:18
 * */
class MainfabOfflineAutolimitParseIndicatorKeyedPorcessFunction extends KeyedProcessFunction[String,JsonNode,JsonNode]{

  /**
   * // todo 1- 解析配置参数 封装捞RunData的参数信息
   * // todo 2- 根据RunDataConfig 配置信息捞RunData 的起始时间
   * // todo 3- 解析出以 IndicatorId 为单位的配置信息
   * // todo 4- 收集 以 IndicatorId 为单位的配置信息
   */
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainfabOfflineAutolimitParseIndicatorKeyedPorcessFunction])

  val indicatorTableName = "mainfab_indicatordata_table"
  val runDataTableName = "mainfab_rundata_table"

  val runDataColumns = List[String](
    "RUN_START_TIME")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    HbaseUtil.initProjectConfig()
  }

  override def processElement(inRow: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType = inRow.get("dataType").asText()

      if(dataType != "offlineAutoLimitSettings"){
        logger.warn(s"process AutoLimit dataType == ${dataType}  inRow == ${inRow}")
        return
      }

      val autoLimitSetting: OfflineAutoLimitSettings = toBean[OfflineAutoLimitSettings](inRow)
      if(!autoLimitSetting.status) {
        logger.warn(s"process AutoLimit status == ${autoLimitSetting.status}  inRow == ${inRow}")
        return
      }

      val offlineAutoLimitConfig: OfflineAutoLimitConfig = autoLimitSetting.datas

      // todo 1- 解析配置参数 封装捞RunData的参数信息
      val queryRunDataConfig: QueryRunDataConfig = OfflineAutoLimitUtil.parseRunDataConfig(offlineAutoLimitConfig)
      if(null == queryRunDataConfig){
        logger.error(s"there isn't queryRunDataConfig ")
        return
      }

      // todo 2- 根据 RunDataConfig 配置信息捞 RunData 的起始时间
      val getRunStartTimestamp = System.currentTimeMillis()
      logger.warn(s"---------------------start getRunData : ${queryRunDataConfig.controlPlanId}-${queryRunDataConfig.version}:${getRunStartTimestamp}")
      val lastRunData = OfflineAutoLimitUtil.getRunTimeRange(queryRunDataConfig)

      val getRunEndTimestamp = System.currentTimeMillis()
      logger.warn(s"---------------------end getRunData : ${queryRunDataConfig.controlPlanId}-${queryRunDataConfig.version}:${getRunEndTimestamp} ; cost Time == ${getRunEndTimestamp - getRunStartTimestamp}")

      // todo 3- 解析出以 IndicatorId 为单位的配置信息
      val offlineAutoLimitIndicatorConfigMap = OfflineAutoLimitUtil.parseOfflineAutoLimitIndicatorConfig(offlineAutoLimitConfig,lastRunData)

      // 等待 有RunData数据的 indicator 数据写入Hbase
      Thread.sleep(2 * 60 * 1000)

      // todo 4- 收集 以 IndicatorId 为单位的配置信息
      offlineAutoLimitIndicatorConfigMap.values.foreach( offlineAutoLimitIndicatorConfig => {
        val jsonNode = beanToJsonNode[OfflineAutoLimitIndicatorConfig](offlineAutoLimitIndicatorConfig)
        collector.collect(jsonNode)
      })

    }catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"parse OfflineAutoLimitIndicatorConfig error ; inRow == ${inRow}")
      }
    }

  }

}
