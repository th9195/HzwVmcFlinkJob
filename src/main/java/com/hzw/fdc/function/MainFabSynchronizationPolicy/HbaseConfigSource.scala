package com.hzw.fdc.function.MainFabSynchronizationPolicy

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.online.MainFabAlarm.AlarmSwitchEventConfig
import com.hzw.fdc.json.JsonUtil.beanToJsonNode
import com.hzw.fdc.scalabean.{AlarmRuleConfig, AutoLimitConfig, ConfigData, ContextConfigData, IndicatorConfig,
  MicConfig, VirtualSensorConfigData, WindowConfigData}
import com.hzw.fdc.util.{InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 *  自定义数据源 处理 hbase配置数据
 */
class HbaseConfigSource() extends RichSourceFunction[JsonNode]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[HbaseConfigSource])

  // 定义一个flag：表示数据源是否还在正常运行
  var running:Boolean = true

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

  }

  override def run(ctx: SourceFunction.SourceContext[JsonNode]): Unit = {
    // 无限循环生成流数据，除非被cancel
    while (running) {

      logger.warn("HbaseConfigSource start!!!!!")


      val IndicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList
      IndicatorConfigList.foreach(x => {
        try{
          val elem = beanToJsonNode[ConfigData[IndicatorConfig]](x)
          ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val MicConfigList: ListBuffer[ConfigData[MicConfig]] = InitFlinkFullConfigHbase.MicConfigList
      MicConfigList.foreach(x => {
        try{
           val elem = beanToJsonNode[ConfigData[MicConfig]](x)
           ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val AlarmSwitchConfigList: ListBuffer[ConfigData[AlarmSwitchEventConfig]] = InitFlinkFullConfigHbase.AlarmSwitchConfigList
      AlarmSwitchConfigList.foreach(x => {
        try{
           val elem = beanToJsonNode[ConfigData[AlarmSwitchEventConfig]](x)
           ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val VirtualSensorConfigList: ListBuffer[ConfigData[VirtualSensorConfigData]] = InitFlinkFullConfigHbase.VirtualSensorConfigList
      VirtualSensorConfigList.foreach(x => {
        try{
           val elem = beanToJsonNode[ConfigData[VirtualSensorConfigData]](x)
           ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val ContextConfigList: ListBuffer[ConfigData[List[ContextConfigData]]] = InitFlinkFullConfigHbase.ContextConfigList
      ContextConfigList.foreach(x => {
        try {
          val elem = beanToJsonNode[ConfigData[List[ContextConfigData]]](x)
          ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val AlarmConfigList: ListBuffer[ConfigData[AlarmRuleConfig]] = InitFlinkFullConfigHbase.AlarmConfigList
      AlarmConfigList.foreach(x => {
        try {
          val elem = beanToJsonNode[ConfigData[AlarmRuleConfig]](x)
          ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val AutoLimitConfigList: ListBuffer[ConfigData[AutoLimitConfig]] = InitFlinkFullConfigHbase.AutoLimitConfigList
      AutoLimitConfigList.foreach(x => {
        try {
          val elem = beanToJsonNode[ConfigData[AutoLimitConfig]](x)
          ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      val RunWindowConfigList: ListBuffer[ConfigData[List[WindowConfigData]]] = InitFlinkFullConfigHbase.RunWindowConfigList
      RunWindowConfigList.foreach(x => {
        try {
          val elem = beanToJsonNode[ConfigData[List[WindowConfigData]]](x)
          ctx.collect(elem)
        }catch {
          case ex: Exception => logger.warn(s"msg: $x error: $ex")
        }
      })

      logger.warn("HbaseConfigSource end!!!!!")

      // 间隔时间 一天
      Thread.sleep(1000*60*60*12)

    }

  }

  override def cancel(): Unit = {
    running  = false
  }

}
