//package com.hzw.fdc.function.online.MainFabFile
//
//import java.text.SimpleDateFormat
//
//import com.hzw.fdc.scalabean.{MainFabPTRawData, RawTrace, SensorNameData}
//import com.hzw.fdc.util.ProjectConfig
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
//import org.apache.flink.util.Collector
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.mutable.ListBuffer
//
//class ControlRawTraceFunction extends RichCoFlatMapFunction[RawTrace, MainFabPTRawData, RawTrace]{
//
//  private val logger: Logger = LoggerFactory.getLogger(classOf[ControlRawTraceFunction])
//
//  // {"traceId": IndicatorFileResult}
//  private var rawDataState: MapState[String, ListBuffer[SensorNameData]] = _
//
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    // 获取全局变量
//    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
//    ProjectConfig.getConfig(p)
//
//    val rawDataStateDescription: MapStateDescriptor[String, ListBuffer[SensorNameData]] = new
//      MapStateDescriptor[String, ListBuffer[SensorNameData]]("rawTraceState",
//        TypeInformation.of(classOf[String]), TypeInformation.of(classOf[ListBuffer[SensorNameData]]))
//
//    rawDataState = getRuntimeContext.getMapState(rawDataStateDescription)
//  }
//
//  override def flatMap1(in1: RawTrace, collector: Collector[RawTrace]): Unit = {
//
//    val resList =  if(rawDataState.contains(in1.traceId)){
//      rawDataState.get(in1.traceId)
//    }else{
//      logger.warn(s"rawTrace flatMap1 not exit traceId:${in1.traceId}\t ${in1}" )
//      ListBuffer()
//    }
//
//    val rawTrace = RawTrace(in1.locationName,
//      in1.moduleName,
//      in1.toolName,
//      in1.chamberName,
//      in1.recipeName,
//      in1.toolGroupName,
//      in1.chamberGroupName,
//      in1.runStartTime,
//      in1.runEndTime,
//      in1.runId,
//      in1.traceId,
//      in1.materialName,
//      in1.lotMESInfo,
//      resList.toList
//    )
//    collector.collect(rawTrace)
//
//    // 清除状态
//    rawDataState.remove(in1.traceId)
//  }
//
//  override def flatMap2(in2: MainFabPTRawData, collector: Collector[RawTrace]): Unit = {
//    val timestamp = in2.timestamp
//    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
//    val timestampStr = fm.format(timestamp)
//    val traceId = in2.traceId
//
//    val sensor = SensorNameData(in2.toolName, in2.chamberName, timestampStr, in2.data)
//
//    val resList = if(rawDataState.contains(traceId)){
//      val sensorList = rawDataState.get(traceId)
//      sensorList += (sensor)
//    }else{
//      ListBuffer(sensor)
//    }
//
//    rawDataState.put(traceId, resList)
//  }
//}
