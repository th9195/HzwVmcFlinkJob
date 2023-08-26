//package com.hzw.fdc.function.online.MainFabFile
//
//import com.hzw.fdc.function.online.MainFabWindow.ContextOracleConfig
//import com.hzw.fdc.scalabean.{ConfigData, ContextConfig, ContextConfigData, ErrorCode, RawTrace, RunEventData, SensorNameData}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
//import org.apache.flink.util.Collector
//import com.hzw.fdc.json.MarshallableImplicits._
//import com.hzw.fdc.util.{InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
//import org.apache.flink.api.java.utils.ParameterTool
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.{concurrent, mutable}
//
//class MainFabRunDataKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,
//  RunEventData, (String, String), RawTrace] {
//  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRunDataKeyedBroadcastProcessFunction])
//
//  val contextMap = new concurrent.TrieMap[String,  String]()
//
//  val runStartTimeMap = new concurrent.TrieMap[String,  Long]()
//
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    // 获取全局变量
//    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
//    ProjectConfig.getConfig(p)
//
//    // 初始化ContextConfig
//    val contextConfigList = InitFlinkFullConfigHbase.ContextConfigList
//    for(one <- contextConfigList){
//      addContextConfig(one)
//    }
//    logger.warn("contextMap: " + contextMap)
//  }
//
//  override def processElement(in1: RunEventData, readOnlyContext: KeyedBroadcastProcessFunction[String, RunEventData,
//    (String, String), RawTrace]#ReadOnlyContext, collector: Collector[RawTrace]): Unit = {
//    try {
//      val key = s"${in1.locationName}|${in1.moduleName}|${in1.toolName}|${in1.chamberName}"
//
//      if (in1.dataType == MainFabConstants.eventEnd) {
//        var toolGroupName = ""
//        var chamberGroupName = ""
//
//        if (contextMap.contains(key)) {
//          val tmp = contextMap(key).split("\\|")
//          toolGroupName = tmp.head
//          chamberGroupName = tmp.last
//        }
//
//
//        val rawTrace = RawTrace(in1.locationName,
//          in1.moduleName,
//          in1.toolName,
//          in1.chamberName,
//          in1.recipeName,
//          toolGroupName,
//          chamberGroupName,
//          if (runStartTimeMap.contains(key)) runStartTimeMap(key) else in1.runStartTime,
//          in1.runEndTime,
//          in1.runId,
//          in1.traceId,
//          in1.materialName,
//          in1.lotMESInfo,
//          Nil
//        )
//        collector.collect(rawTrace)
//      } else if (in1.dataType == MainFabConstants.eventStart) {
//        runStartTimeMap.put(key, in1.runStartTime)
//      }
//    }catch {
//      case e: Exception => logger.warn(s"processElement error $e :${in1}")
//    }
//  }
//
//  override def processBroadcastElement(in2: (String, String),
//                                       context: KeyedBroadcastProcessFunction[String, RunEventData,
//                                         (String, String),
//                                         RawTrace]#Context,
//                                       collector: Collector[RawTrace]): Unit = {
//    try {
//      if (in2._1 == MainFabConstants.context) {
//        val contextConfig = in2._2.fromJson[ConfigData[List[ContextConfigData]]]
//        addContextConfig(contextConfig)
//      }
//    } catch {
//      case e: Exception => logger.warn(s"contextConfig json error $e :${in2._2}")
//    }
//  }
//
//  def addContextConfig(configList: ConfigData[List[ContextConfigData]]):Unit = {
//    if (configList.status) {
//      for (elem <- configList.datas) {
//        try {
//          val key = s"${elem.locationName}|${elem.moduleName}|${elem.toolName}|${elem.chamberName}"
//          val toolGroupName = elem.toolGroupName
//          val chamberGroupName = elem.chamberGroupName
//          contextMap.put(key, toolGroupName + "|" + chamberGroupName)
//        } catch {
//          case e: Exception =>
//            logger.warn(ErrorCode("002003b009C", System.currentTimeMillis(), Map("contextConfig" -> elem), e.toString).toJson)
//        }
//      }
//    }
//
//  }
//}
