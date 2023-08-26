package com.hzw.fdc.service.online

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabWindowDao
import com.hzw.fdc.function.PublicFunction.AddStep.addStep
import com.hzw.fdc.function.PublicFunction.{FdcJsonNodeSchema, FdcKafkaSchema}
import com.hzw.fdc.function.online.MainFabVirtualSensor.MainFabVirtualSensorKeyedBroadcastProcessFunction
import com.hzw.fdc.function.online.MainFabWindow.{MainFabWindowAddStepProcessFunction, _}
import com.hzw.fdc.json.JsonUtil.beanToJsonNode
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author gdj
 * @create 2020-08-13-11:09
 *
 */
class MainFabWindowService extends TService {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowService])

  lazy val MESMessageOutput = new OutputTag[MESMessage]("MESMessage")
  lazy val toolMessageOutput = new OutputTag[toolMessage]("toolMessage")
  lazy val sensorMessageOutput = new OutputTag[sensorMessage]("sensorMessage")
  lazy val dataMissingRatioOutput = new OutputTag[FdcData[IndicatorResult]]("dataMissingRatio")
  lazy val WindowEndRunDataOutput = new OutputTag[RunEventData]("WindowEndRunData")
  lazy val cycleCountDataOutput = new OutputTag[FdcData[IndicatorResult]]("cycleCountData")
  lazy val RunDataOutput = new OutputTag[RunData]("RunData")
  lazy val MESOutput = new OutputTag[JsonNode]("toolMessage")
  lazy val WindowEndOutput = new OutputTag[(String, JsonNode, JsonNode)]("WindowEnd")
  lazy val debugOutput = new OutputTag[String]("debugTest")

  private val Dao = new MainFabWindowDao

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = Dao

  /**
   * 处理数据
   *
   * @return
   */
  override def analyses(): Any = {

    //读取TRANSFORM 数据
    val PTTransformDataStream = getDatas()

    //读取context配置
    val contextConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_CONTEXT_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_CONTEXT_CONFIG_KAFKA_SOURCE_UID)

    //读window配置
    val windowConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_WINDOW_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_WINDOW_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_WINDOW_CONFIG_KAFKA_SOURCE_UID)

    //读Indicator配置 用于dataMissing Indicator
    val indicatorConfigDataStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_INDICATOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_WINDOW_JOB_INDICATOR_CONFIG_KAFKA_SOURCE_UID)

    //用于debug
    val debugConfigStream = getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_DEBUG_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_WINDOW_JOB_DEBUG_CONFIG_KAFKA_SOURCE_UID)

    val ConfigDataStream = contextConfigDataStream.union(windowConfigDataStream, indicatorConfigDataStream,
      debugConfigStream)

    //注册广播变量
    val config = new MapStateDescriptor[String, JsonNode](
      MainFabConstants.config,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    val windowConfigBroadcastDataStream = ConfigDataStream.broadcast(config)

    //PM状态数据
    val PMStatusDataStream = getDao.getKafkaJsonSource(
      ProjectConfig.KAFKA_PM_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_PM_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_PM_KAFKA_SOURCE_UID)

    val pMStatusDataMapStateDescriptor= new MapStateDescriptor[String, JsonNode](
      "PMStatusData",
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[JsonNode] {}))

    //和PM状态数据merge的数据
    val PTUnionPMStatusDataStream: DataStream[JsonNode] = PTTransformDataStream
      .filter(f => {
        try {
          val toolNameStatus = f.findPath(MainFabConstants.toolName).asText().isEmpty
          val chamberNameStatus = f.findPath(MainFabConstants.chamberName).asText().isEmpty
          //过滤dataType为空的
          if (toolNameStatus && chamberNameStatus) {
            logger.warn(ErrorCode("002003b009C", System.currentTimeMillis(), Map("data" -> f.asText()), "dataType is null").toJson)
          }
          !(toolNameStatus && chamberNameStatus)
        } catch {
          case e: Exception => logger.warn(s"Exception :${ExceptionInfo.getExceptionInfo(e)} data $f")
            false
        }
      })
      .keyBy(data => {
        val toolName = data.findPath(MainFabConstants.toolName).asText()
        val chamberName = data.findPath(MainFabConstants.chamberName).asText()
        s"$toolName|$chamberName"
      })
      .connect(PMStatusDataStream.broadcast(pMStatusDataMapStateDescriptor))
      .process(new MainFabPMKeyedProcessFunction)
      .name("pm")
      .uid("pm")

    //和PM状态数据merge的数据 key by 后数据
    val PTKeyedDataStream: KeyedStream[JsonNode, String] = PTUnionPMStatusDataStream
      .filter(ptData => ptData.has(MainFabConstants.traceId) && !ptData.path(MainFabConstants.traceId).isNull)
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })

    //读取virtualSensorConfig配置
    val virtualSensorConfigDataStream = getDao().readMainFabWindowConfigKafka(
      MainFabConstants.virtualSensorConfig,
      ProjectConfig.KAFKA_VIRTUAL_SENSOR_CONFIG_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_VIRTUAL_SENSOR_CONFIG_KAFKA_SOURCE_UID)

    //注册广播变量
    val virtualSensorConfig = new MapStateDescriptor[String, (String, String)](
      MainFabConstants.virtualSensorConfig,
      //Key类型
      BasicTypeInfo.STRING_TYPE_INFO,
      //Value类型
      TypeInformation.of(new TypeHint[(String, String)] {}))

    val virtualSensorConfigBroadcastDataStream = virtualSensorConfigDataStream.broadcast(virtualSensorConfig)

    //计算完virtualSensor的数据
    val virtualSensorDataStream: DataStream[JsonNode] = PTKeyedDataStream
      .connect(virtualSensorConfigBroadcastDataStream)
      .process(new MainFabVirtualSensorKeyedBroadcastProcessFunction)
      .uid("virtual sensor")
      .name("virtual sensor")

    //计算完virtualSensor的数据按照run key by
    val virtualSensorKeyByDataStream: KeyedStream[JsonNode, String] = virtualSensorDataStream
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .process(new MainFabWindowAddStepProcessFunction(false))
      .filter(_.nonEmpty)
      .map(_.get)
      .name("addStep")
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })


    /**
     *  处理runData和mes信息
     */
    val ProcessEndRunDataStream = virtualSensorDataStream
      .keyBy(data => {
              data.findPath(MainFabConstants.traceId).asText()
            })
      .process(new MainFabWindowAddStepProcessFunction(true))
      .filter(_.nonEmpty)
      .map(_.get)
      .keyBy(data => {
        data.findPath(MainFabConstants.traceId).asText()
      })
      .process(new MainFabProcessRunFunction)
      .name("run Task")
      .uid("run Task")

    //MES数据去重
    val MESDataStream = ProcessEndRunDataStream.getSideOutput(MESOutput)


    //ToolMessage写到kafka
    MESDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC,
      new FdcJsonNodeSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_TOOL_MESSAGE_TOPIC
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaToolMessageSink)
      .uid(MainFabConstants.KafkaToolMessageSink)


    /**
     * 拆分window配置
     */
    val windowSplitStream: DataStream[(String, JsonNode, JsonNode)] = virtualSensorKeyByDataStream
      .connect(windowConfigBroadcastDataStream)
      .process(new MainFabWindowConfigSplitKeyedBroadcastProcessFunction)
      .name("WindowConfigSplit")
      .uid("WindowConfigSplit")

    val WindowEndSensorAliasStream = windowSplitStream.getSideOutput(WindowEndOutput)

//    /**
//     * 过滤traceId不存在的脏数据，
//     * ProcessEnd 划分窗口后 聚合成一个run的数据。
//     */
//    val windowsProcessEndDataStream = windowSplitStream
//      .process(new MainFabProcessEndFunction)
//      .name(MainFabConstants.WindowTask)
//      .uid(MainFabConstants.WindowTask)

    /**
     * window end计算
     */
    val windowEndOutPutDataStream = WindowEndSensorAliasStream
      .keyBy(_._1)
      .connect(windowConfigBroadcastDataStream)
      .process(new MainFabWindowEndKeyedBroadcastProcessFunction)
      .uid("windowEnd")
      .name("windowEnd")


    val windowEndDebugTestStream = windowEndOutPutDataStream.getSideOutput(debugOutput)
    val cycleCountDataStream: DataStream[FdcData[IndicatorResult]] = windowEndOutPutDataStream.getSideOutput(cycleCountDataOutput)

    /**
     * 配置了 WindowEnd 的窗口，
     * 需要提前输出run data数据，以供查询indicator
     */
    val WindowEndRunEventDataStream = windowEndOutPutDataStream.getSideOutput(WindowEndRunDataOutput)
    //调整WindowEnd 的窗口 run data格式
    val WindowEndRunDataStream = WindowEndRunEventDataStream.map(new MainFabWindowEndRunDataMapFunction)

    /**
     * 正常的ProcessEnd run结束时，触发计算window 数据。
     * 匹配context和Window配置
     * key by toolName+chamberName+recipeName
     */
    val ProcessEndOutPutDataStream: DataStream[fdcWindowData] = windowSplitStream
      .keyBy(_._1)
      .connect(windowConfigBroadcastDataStream)
      .process(new MainFabProcessEndKeyedBroadcastProcessFunction)
      .uid("processEnd")
      .name("processEnd")

    val processEndDebugTestStream = ProcessEndOutPutDataStream.getSideOutput(debugOutput)
    val debugTestStream = windowEndDebugTestStream.union(processEndDebugTestStream)

    //debug log 写入kafka
    debugTestStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_DEBUG_RESULT_LOG_TOPIC,
      new FdcKafkaSchema[String](ProjectConfig.KAFKA_DEBUG_RESULT_LOG_TOPIC
        //按照tool分区
        , (e: String) => ""
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name("debugTest")
      .uid("debugTest")

    //union Window end 的run
    val RunDataStream = WindowEndRunDataStream.union(ProcessEndRunDataStream)
    //runData 写入kafka
    RunDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC,
      new FdcKafkaSchema[FdcData[RunData]](ProjectConfig.KAFKA_MAINFAB_RUNDATA_TOPIC
        //按照tool分区
        , (e: FdcData[RunData]) => e.datas.toolName + e.datas.chamberName
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaRunDataSink)
      .uid(MainFabConstants.KafkaRunDataSink)


    /**
     * ProcessEnd run结束时 需要
     * 组装dataMissingRatioIndicator发往indicator topic
     *
     * --新增了WindowEnd的cycle window需求,将结果union,一起发往indicator topic
     */
    val dataMissingRatioStream: DataStream[FdcData[IndicatorResult]] = ProcessEndOutPutDataStream.getSideOutput(dataMissingRatioOutput)
    val WindowEndAndProcessEndCycleCount: DataStream[FdcData[IndicatorResult]] = cycleCountDataStream.union(dataMissingRatioStream)
    WindowEndAndProcessEndCycleCount.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC,
      new FdcKafkaSchema[FdcData[IndicatorResult]](ProjectConfig.KAFKA_MAINFAB_INDICATOR_RESULT_TOPIC
        //按照tool分区
        , (e: FdcData[IndicatorResult]) =>
          e.datas.toolName + e.datas.chamberName
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaDataMissingRatioSink)
      .uid(MainFabConstants.KafkaDataMissingRatioSink)


    //ProcessEnd 和WindowEnd
    // 划分好窗口的数据写入kafka
    val outPutDataStream = windowEndOutPutDataStream.union(ProcessEndOutPutDataStream)

    outPutDataStream
      .process(new ParseWindowProcessFunction())
      .addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_WINDOW_DATA_TOPIC,
      new FdcKafkaSchema[fdcWindowData](ProjectConfig.KAFKA_WINDOW_DATA_TOPIC
        //按照tool分区
        , (e: fdcWindowData) => {
          if(e.datas.windowDatasList.nonEmpty){
            s"${e.datas.toolName}|${e.datas.chamberName}|${e.datas.windowDatasList.head.sensorAlias}"
          }else{
            s"${e.datas.toolName}|${e.datas.chamberName}|"
          }
        }
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaWindowSink)
      //添加uid用于监控
      .uid(MainFabConstants.KafkaWindowSink)



    //清洗，过滤版本，过滤event data 准备入库
//    val rawDataDataStream = virtualSensorKeyByDataStream.process(new MainFabWindowJobFilterVersionProcessFunction)


    //raw data写kafka
    virtualSensorKeyByDataStream.addSink(new FlinkKafkaProducer(
      ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC,
      new FdcKafkaSchema[JsonNode](ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC
        //按照tool分区
        , (e: JsonNode) => e.findPath(MainFabConstants.toolName).asText() + e.findPath(MainFabConstants.chamberName).asText()
      )
      , ProjectConfig.getKafkaProperties()
      , FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )).name(MainFabConstants.KafkaRawDataSink)
      .uid(MainFabConstants.KafkaRawDataSink)


  }

  /**
   * 获取PT数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    getDao.getKafkaJsonSource(
      ProjectConfig.KAFKA_MAINFAB_TRANSFORM_DATA_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_WINDOW_JOB,
      MainFabConstants.latest,
      MainFabConstants.MAIN_FAB_ROUTER_JOB_PT_DATA_KAFKA_SOURCE_UID,
      MainFabConstants.MAIN_FAB_ROUTER_JOB_PT_DATA_KAFKA_SOURCE_UID)
  }

}
