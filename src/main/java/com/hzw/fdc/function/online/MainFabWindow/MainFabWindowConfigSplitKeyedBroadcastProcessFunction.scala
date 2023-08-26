package com.hzw.fdc.function.online.MainFabWindow


import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.engine.api.ApiControlWindow
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{WindowConfigData, _}
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{ContextDataType, WindowDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
 *  拆分window配置，打散一个tool下所有的sensor, 分发到不同的并行度处理
 */
class MainFabWindowConfigSplitKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,
  JsonNode, JsonNode, (String, JsonNode, JsonNode)] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWindowConfigSplitKeyedBroadcastProcessFunction])

  lazy val WindowEndOutput = new OutputTag[(String, JsonNode, JsonNode)]("WindowEnd")

  // ============================== 从hbase获取历史配置; kafka topic获取增量配置 ==============================

  //contextId  --> tool|chamber
  var contextMap = new concurrent.TrieMap[Long, mutable.Set[String]]()

  // calcTrigger 主要分为两类 一:ProcessEnd  二:WindowEnd
  //{"tool|chamber|calcTrigger"  ---> plan|contextId|controlWindowId --> WindowConfigData}
  var windowConfigMap = new concurrent.TrieMap[String, concurrent.TrieMap[String, WindowConfigData]]()

  // {"tool|chamber|calcTrigger" ---> sensorAlias --> ElemWindowConfigAlias}
  val elemSensorConfig = new concurrent.TrieMap[String, concurrent.TrieMap[String, mutable.Set[ElemWindowConfigAlias]]]()

  //===================================== 拆分================================================================

  // window配置拆分后的结果  {"tool|chamber|calcTrigger" --> sensorPartitionID --> List[WindowConfigData]}
  var windowConfigSplitMap = new concurrent.TrieMap[String, concurrent.TrieMap[Long, List[WindowConfigData]]]()

  //自定义sensor分区 {"tool|chamber|calcTrigger" --> sensorPartitionID --> list[sensorAlias]}
  val sensorPartitionList = new concurrent.TrieMap[String,  concurrent.TrieMap[Long, mutable.Set[String]]]()

  // 划窗口额外需要的sensor条件, 比如stepId  {"tool|chamber|calcTrigger" --> sensorPartitionID --> list[sensorAlias]}
  val sensorPartitionExtraList = new concurrent.TrieMap[String,  concurrent.TrieMap[Long, mutable.Set[String]]]()

  // 解析LogisticWindow 所需要的额外sensor
  val WindowIDExtraSensorMap = new concurrent.TrieMap[Long, List[String]]()

  // 侧道输出日志信息
  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  val job_fun_DebugCode:String = "024001"
  val jobName:String = "MainFabWindowEndWindowService"
  val optionName : String = "MainFabWindowConfigSplitKeyedBroadcastProcessFunction"

  override def open(parameters: Configuration): Unit = {

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val startTime = System.currentTimeMillis()
    // 初始化ContextConfig
    //val contextConfigList = InitFlinkFullConfigHbase.ContextConfigList
    val contextConfigList = readHbaseAllConfig[List[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE, ContextDataType)
    contextConfigList.foreach(addContextMapToContextMap)

    // 初始化windowConfig
    //val runWindowConfigList = InitFlinkFullConfigHbase.RunWindowConfigList
    val runWindowConfigList = readHbaseAllConfig[List[WindowConfigData]](ProjectConfig.HBASE_SYNC_WINDOW_TABLE, WindowDataType)
    runWindowConfigList.foreach(elem => {
      addWindowConfigToWindowConfigMap(elem, true)
    })

    val endTIme = System.currentTimeMillis()
    logger.warn("---SplitSensor Total Time: " + (endTIme - startTime))

//    logger.warn(s"--windowConfigMap-- :${windowConfigMap.toJson} ")
//    logger.warn(s"--elemSensorConfig-- :${elemSensorConfig.toJson} ")
//    logger.warn(s"--windowConfigSplitMap-- :${windowConfigSplitMap.toJson} ")
//    logger.warn(s"--sensorPartitionList-- :${sensorPartitionList.toJson} ")
//
//    logger.warn(s"--sensorMap-- :${sensorMap.toJson} ")
//    logger.warn(s"--sensorPartitionExtraList-- :${sensorPartitionExtraList.toJson} ")
  }

  /**
   *  数据流处理
   */
  override def processElement(in1: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String,
    JsonNode, JsonNode, (String, JsonNode, JsonNode)]#ReadOnlyContext,
                              out: Collector[(String, JsonNode, JsonNode)]): Unit = {
    try {
      val dataType = in1.get(MainFabConstants.dataType).asText()
      val traceId = in1.get(MainFabConstants.traceId).asText()
      val toolName = in1.get(MainFabConstants.toolName).asText()
      val chamberName = in1.get(MainFabConstants.chamberName).asText()

      val calcTriggerList = ListBuffer("WindowEnd")

      calcTriggerList.foreach(calcTrigger => {
        val key = s"$toolName|$chamberName|$calcTrigger"

        // 处理rawData
        if (dataType == MainFabConstants.rawData) {

          if (sensorPartitionList.contains(key)) {
            val sensorPartitionMap = sensorPartitionList(key)
            val sensorAliasConfigSet = elemSensorConfig(key).keySet

            val RawData = toBean[MainFabRawData](in1)

            for (elem <- sensorPartitionMap) {

              val sensorPartitionID = elem._1
              // 获取真实需要的sensor, 两个set的子集
              val sensorAliasSet = elem._2.intersect(sensorAliasConfigSet)

              //处理额外的sensor
              if (sensorPartitionExtraList.contains(key)) {
                val sensorPartitionExtraMap = sensorPartitionExtraList(key)
                if (sensorPartitionExtraMap.contains(sensorPartitionID)) {
                  sensorAliasSet ++= sensorPartitionExtraMap(sensorPartitionID)
                }
              }

              val data = RawData.data.filter((x: sensorData) => {
                sensorAliasSet.contains(x.sensorAlias) && (x.sensorValue != Double.NaN)
              })

              val rawData = RawData.copy(data = data)
              val mainFabRawDataTuple = MainFabRawDataTuple(
                dataType = rawData.dataType,
                timestamp = rawData.timestamp,
                stepId = rawData.stepId,
                data = rawData.data.map(x => {(x.sensorAlias, x.sensorValue, x.unit)})
              )

              if(calcTrigger == "WindowEnd"){
//                logger.warn("WindowEnd1: " + beanToJsonNode[MainFabRawData](rawData))
                readOnlyContext.output(WindowEndOutput,(s"$traceId|$key|$sensorPartitionID",
                  beanToJsonNode[MainFabRawData](rawData), beanToJsonNode[String]("")))
              }else{
                out.collect(s"$traceId|$key|$sensorPartitionID", beanToJsonNode[MainFabRawDataTuple](mainFabRawDataTuple), beanToJsonNode[String](""))
              }
            }
          }
        } else if (dataType == MainFabConstants.eventStart || dataType == MainFabConstants.eventEnd) {
          // 处理event信息
          if (windowConfigSplitMap.contains(key)) {
            val sensorPartitionMap = windowConfigSplitMap(key)
            if(sensorPartitionMap.isEmpty){
              logger.warn("该traceId没有设置indicator: " + traceId)
              readOnlyContext.output(mainFabLogInfoOutput,
                generateMainFabLogInfo(
                  "d001A",
                  "processElement",
                  "该traceId没有设置indicator: ",
                  Map[String,Any]("traceId" -> traceId),
                  in1.toString)
                )
            }else {

              for (elem <- sensorPartitionMap) {
                val sensorPartitionID = elem._1
                val windowConfigList = elem._2

                val windowConfig = ConfigData(
                  `dataType` = "runwindow",
                  serialNo = "",
                  status = true,
                  windowConfigList
                )

                if (calcTrigger == "WindowEnd") {
//                  logger.warn("WindowEnd2: " + (s"$key|$sensorPartitionID", in1, beanToJsonNode[ConfigData[List[WindowConfigData]]](windowConfig)))
                  readOnlyContext.output(WindowEndOutput, (s"$traceId|$key|$sensorPartitionID", in1, beanToJsonNode[ConfigData[List[WindowConfigData]]](windowConfig)))
                } else {
                  out.collect(s"$traceId|$key|$sensorPartitionID", in1,
                    beanToJsonNode[ConfigData[List[WindowConfigData]]](windowConfig))
                }

              }
            }
          }else{
            logger.warn("该traceId没有设置indicator: " + traceId)
            readOnlyContext.output(mainFabLogInfoOutput,
              generateMainFabLogInfo(
                "d001A",
                "processElement",
                "该traceId没有设置indicator: ",
                Map[String,Any]("traceId" -> traceId),
                in1.toString)
            )
          }
        }

      })

    }catch {
      case ex: Exception => logger.warn(ErrorCode("002007d012C", System.currentTimeMillis(),
        Map("msg" -> "拆分window配置,处理数据流失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }

  /**
   *  获取广播变量
   */
  override def processBroadcastElement(in2: JsonNode, context: KeyedBroadcastProcessFunction[String,
    JsonNode, JsonNode, (String, JsonNode, JsonNode)]#Context,
                                       collector: Collector[(String, JsonNode, JsonNode)]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()

      if (dataType == MainFabConstants.context) {
        try {
          val contextConfig = toBean[ConfigData[List[ContextConfigData]]](in2)
          addContextMapToContextMap(contextConfig)

        } catch {
          case e: Exception => logger.warn(s"contextConfig json error data:$in2  Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }

      } else if (dataType == MainFabConstants.runwindow) {
        try {
          val windowConfig = toBean[ConfigData[List[WindowConfigData]]](in2)
          addWindowConfigToWindowConfigMap(windowConfig)
        } catch {
          case e: Exception => logger.warn(s"windowConfig json error  data:$in2  " +
            s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
        }
      }
    } catch {
      case e: Exception => logger.warn(s"windowJob processBroadcastElement error data:$in2  " +
        s"Exception: ${ExceptionInfo.getExceptionInfo(e)}")
    }
  }


  /**
   * 添加window config; 加载所有window
   */
  def addWindowConfigToWindowConfigMap(configList: ConfigData[List[WindowConfigData]], enableOpen: Boolean = false): Unit = {
    try {
      if(configList.datas.isEmpty){
        return
      }
      //println(s"addWindowConfigToWindowConfigMap: $configList")

      /**
       * 删除plan相关的window配置
       */
      val contextIdList =  configList.datas.map(_.contextId)
      val planId = configList.datas.head.controlPlanId.toString
      val calcTriggerList = ListBuffer("ProcessEnd", "WindowEnd")

      for(contextId <- contextIdList) {
        val toolKeyList = contextMap(contextId)
        for (toolKey <- toolKeyList) {
          for (calcTrigger <- calcTriggerList) {
            val key = s"$toolKey|$calcTrigger"
            if (windowConfigMap.contains(key)) {
              try {
                val planConfigMap = windowConfigMap(key)
                for (contextKey <- planConfigMap.keySet) {
                  if (contextKey.toString.startsWith(s"${planId}|")) {
                    logger.warn(s"window del  key: ${key} planKey:" + s"${contextKey}|")
                    planConfigMap.remove(contextKey)
                  }
                }
                windowConfigMap.put(key, planConfigMap)
              } catch {
                case ex: Exception => None
              }
            }
          }
        }
      }


      val totalKeySet = mutable.Set[String]()  // tool|chamber|calcTrigger

      for (elem <- configList.datas) {

        if(elem.calcTrigger == "WindowEnd") {

          val contextId = elem.contextId
          if (contextMap.contains(contextId)) {

            val keySet: mutable.Set[String] = contextMap(contextId).map(_ + "|" + elem.calcTrigger)
            totalKeySet ++= keySet

            // 修改windowConfigMap
            modifyWindowConfigMap(keySet, elem, configList.status)

            // 修改elemSensorConfig
            modifyElemSensorConfig(keySet, elem, configList.status)
          }

          try {
            if (elem.controlWindowType == "LogisticWindow") {
              val windowStart = elem.windowStart.replace("}", "").replace("{", "")
              val windowEnd = elem.windowEnd.replace("}", "").replace("{", "")
              val startSensorList = ApiControlWindow.utilParseSensors(windowStart).asScala
              try {
                startSensorList ++= ApiControlWindow.utilParseSensors(windowEnd).asScala
              } catch {
                case ex: Exception => None
              }
              // LogisticWindow 额外需要的sensor条件
              logger.warn(s"WindowIDExtraSensor controlWindowId: ${elem.controlWindowId}\t${startSensorList.toList}")
              WindowIDExtraSensorMap.put(elem.controlWindowId, startSensorList.toList)
            }
          } catch {
            case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
              Map("range" -> "ApiControlWindow.utilParseSensors调用失败", "elem" -> elem), ex.toString).toJson)
          }
        }
      }


      totalKeySet.foreach(key => {
        if(!enableOpen) {
          try {
            // 打印信息
            println(s"=======${key}  windowConfigMap: ${windowConfigMap(key)}")
            println(s"=======${key}  elemSensorConfig: ${elemSensorConfig(key)}")
          } catch {
            case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
              Map("range" -> "window log print", "configList" -> configList), ex.toString).toJson)
          }
        }

        if(elemSensorConfig.contains(key)) {

          // 获取所有的sensor
          val chamerSensorSet = elemSensorConfig(key).keySet

          /**
           * 拆分sensor
           */
          splitSensorFunction(chamerSensorSet.to[mutable.Set], key)

          // 删除原先的window配置
          try {
            windowConfigSplitMap.remove(key)
          }catch {
            case ex: Exception => None
          }
          /**
           *  拆分window配置
           */
          splitWindowFunction(key)

          /**
           * 筛选出额外的sensor
           */
          getExtraSensorFunction(key)

          if(enableOpen == false) {
            try {
              println(s"=======${key}  splitWindowFunction: ${windowConfigSplitMap(key)}")
              println(s"=======${key}  sensorPartitionList: ${sensorPartitionList(key)}")
              if (sensorPartitionExtraList.contains(key)) {
                println(s"=======${key}  sensorPartitionExtraList: ${sensorPartitionExtraList(key)}")
              }
            } catch {
              case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
                Map("range" -> "log print"), ex.toString).toJson)
            }
          }
        }
      })

    } catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("range" -> "添加window config配置失败", "configList" -> s"${configList}"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   *  拆分sensor
   */
  def splitSensorFunction(chamerSensorSet: mutable.Set[String], key: String): Unit = {
    try {

        for (elem <- chamerSensorSet) {
          // 获取sensorPartitionID
          val sensorPartitionID = (elem.hashCode % ProjectConfig.WINDOWEND_SENSOR_SPLIT_GROUP_COUNT).abs.toLong
          if (sensorPartitionList.contains(key)) {
            val partitionMap = sensorPartitionList(key)
            if (partitionMap.contains(sensorPartitionID)) {
              val sensorSet = partitionMap(sensorPartitionID)
              sensorSet.add(elem)
              partitionMap.put(sensorPartitionID, sensorSet)
              sensorPartitionList.put(key, partitionMap)
            } else {
              partitionMap.put(sensorPartitionID, mutable.Set(elem))
              sensorPartitionList.put(key, partitionMap)
            }
          } else {
            val partitionMapNew = concurrent.TrieMap[Long, mutable.Set[String]](sensorPartitionID -> mutable.Set(elem))
            sensorPartitionList.put(key, partitionMapNew)
          }
        }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("range" -> "拆分sensor失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   *  拆分window配置, 同一个tool + chamebr下的window配置
   */
  def splitWindowFunction(key: String): Unit ={
      /**
       * 拆分window配置
      **/
      for(elem <- sensorPartitionList(key)){
        try {
          val sensorPartitionID = elem._1

          // 获取所有sensor
          val sensorList = sensorPartitionList(key)(sensorPartitionID)

          val resWindowConfigDataNewMap = concurrent.TrieMap[String, WindowConfigData]()

          // 获取所有的细分window配置
          val ElemWindowConfigAliasMap = new concurrent.TrieMap[Long, ListBuffer[ElemWindowConfigAlias]]()
          for (sensorSvid <- sensorList) {
            if(elemSensorConfig(key).contains(sensorSvid)) {
              val elemWindowConfigAliasListSet = elemSensorConfig(key)(sensorSvid)
              elemWindowConfigAliasListSet.foreach(elem => {

                if (ElemWindowConfigAliasMap.contains(elem.controlWindowId)) {
                  val ElemWindowConfigAliasList = ElemWindowConfigAliasMap(elem.controlWindowId)
                  ElemWindowConfigAliasList += elem
                  ElemWindowConfigAliasMap.put(elem.controlWindowId, ElemWindowConfigAliasList)
                } else {
                  ElemWindowConfigAliasMap.put(elem.controlWindowId, ListBuffer(elem))
                }
              })
            }
          }

          // 组装成WindowConfigData对象
          for (elem <- ElemWindowConfigAliasMap) {
            try {
              val controlWindowId = elem._1
              val WindowConfigAliasList = elem._2.map(x => {
                WindowConfigAlias(
                  sensorAliasId = x.sensorAliasId,
                  sensorAliasName = x.sensorAliasName,
                  svid = x.svid,
                  indicatorId = x.indicatorId,
                  chamberName = x.chamberName,
                  toolName = x.toolName
                )
              }).toSet.toList

              val windowToolConfigMap = windowConfigMap(key)
              for(windowToolConfig <- windowToolConfigMap.values){
                if(windowToolConfig.controlWindowId == controlWindowId){
                  val windowConfigData = windowToolConfig
                  val windowConfigDataNew = WindowConfigData(
                    contextId = windowConfigData.contextId,
                    controlWindowId = windowConfigData.controlWindowId,
                    parentWindowId = windowConfigData.parentWindowId,
                    controlWindowType = windowConfigData.controlWindowType,
                    isTopWindows = windowConfigData.isTopWindows,
                    isConfiguredIndicator = windowConfigData.isConfiguredIndicator,
                    windowStart = windowConfigData.windowStart,
                    windowEnd = windowConfigData.windowEnd,
                    controlPlanId = windowConfigData.controlPlanId,
                    controlPlanVersion = windowConfigData.controlPlanVersion,
                    calcTrigger = windowConfigData.calcTrigger,
                    sensorAlias = WindowConfigAliasList
                  )

                  resWindowConfigDataNewMap.put(s"${windowConfigData.controlPlanId}|${windowConfigData.contextId}|${windowConfigData.controlWindowId}", windowConfigDataNew)
                }
              }
            }catch {
              case ex: Exception => logger.warn(s"WindowConfigData error: ${key} | ${elem._1} | ${ex.toString}")
            }
          }

          val resWindowConfigDataNewMapTMp = concurrent.TrieMap[String, WindowConfigData]()

          /**
           * 解析获取子window的所有父window
           */
          def parseWindowConfig(resWindowConfigDataNew: WindowConfigData): Unit = {
            try {
              if (!resWindowConfigDataNew.isTopWindows) {
                val parentWindowId = resWindowConfigDataNew.parentWindowId
                // 如果父window不在map里面, 需要添加进来
                if (!resWindowConfigDataNewMap.contains(s"${resWindowConfigDataNew.controlPlanId}|${resWindowConfigDataNew.contextId}|${parentWindowId}")) {
                  var windowConfigData: WindowConfigData = null
                  var calcTrigger = ""
                  val tmpMap = windowConfigMap(key)
                  val planKey = s"${resWindowConfigDataNew.controlPlanId}|${resWindowConfigDataNew.contextId}|${parentWindowId}"
                  if(tmpMap.contains(planKey)){
                    windowConfigData = tmpMap(planKey)
                    calcTrigger = windowConfigData.calcTrigger
                  }else{
                    logger.warn(s"parentWindowId key: $key planKey: ${planKey}")
                    val recoderKeyList = key.split("\\|")
                    val recoderKey = s"${recoderKeyList(0)}|${recoderKeyList(1)}|WindowEnd"
                    windowConfigData = windowConfigMap(recoderKey)(s"${resWindowConfigDataNew.controlPlanId}|${resWindowConfigDataNew.contextId}|${parentWindowId}")
                    calcTrigger = "ProcessEnd"
                  }
                  val nowWindowConfigData = WindowConfigData(
                    contextId = windowConfigData.contextId,
                    controlWindowId = windowConfigData.controlWindowId,
                    parentWindowId = windowConfigData.parentWindowId,
                    controlWindowType = windowConfigData.controlWindowType,
                    isTopWindows = windowConfigData.isTopWindows,
                    isConfiguredIndicator = false,
                    windowStart = windowConfigData.windowStart,
                    windowEnd = windowConfigData.windowEnd,
                    controlPlanId = windowConfigData.controlPlanId,
                    controlPlanVersion = windowConfigData.controlPlanVersion,
                    calcTrigger = calcTrigger,
                    sensorAlias = List()
                  )

                  resWindowConfigDataNewMapTMp.put(s"${windowConfigData.controlPlanId}|${windowConfigData.contextId}|" +
                    s"${windowConfigData.controlWindowId}", nowWindowConfigData)
                  // 递归获取父window
                  parseWindowConfig(nowWindowConfigData)
                }
              }
            }catch {
              case ex: Exception => logger.warn(ErrorCode("002008d001C", System.currentTimeMillis(),
                Map("range" -> "拆分window配置失败", "WindowConfigData" -> resWindowConfigDataNew), ex.toString).toJson)
            }
          }

          // 判断该window是不是子window,如果是需要把父window也写进去
          for (elem <- resWindowConfigDataNewMap) {
            val resWindowConfigDataNew = elem._2
            parseWindowConfig(resWindowConfigDataNew)
          }

          for(elem <- resWindowConfigDataNewMapTMp){
            resWindowConfigDataNewMap.put(elem._1, elem._2)
          }

          if(windowConfigSplitMap.contains(key)){
            val partitionMap = windowConfigSplitMap(key)
            partitionMap.put(sensorPartitionID, resWindowConfigDataNewMap.values.toList)
            windowConfigSplitMap.put(key, partitionMap)
          }else{
            val windowConfigPartition = concurrent.TrieMap[Long, List[WindowConfigData]](sensorPartitionID -> resWindowConfigDataNewMap.values.toList)
            windowConfigSplitMap.put(key, windowConfigPartition)
          }
        }catch {
          case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
            Map("range" -> "拆分window配置失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
        }
      }
  }

  /**
   *  筛选出额外的sensor
   */
  def getExtraSensorFunction(key: String): Unit = {
    try{
      for(elem <- sensorPartitionList(key)) {
        val sensorPartitionID = elem._1

        // 筛选额外的sensor
        val resWindowConfigDataList = windowConfigSplitMap(key)(sensorPartitionID).distinct

        val sensorSet = mutable.Set[String]()
        resWindowConfigDataList.foreach(elem => {
          if(WindowIDExtraSensorMap.contains(elem.controlWindowId)){
            sensorSet ++= WindowIDExtraSensorMap(elem.controlWindowId)
          }
        })

        if(sensorSet.nonEmpty) {
          val extraSensorMap = if (sensorPartitionExtraList.contains(key)) {
            val extraSensorMap = sensorPartitionExtraList(key)
            extraSensorMap.put(sensorPartitionID, sensorSet)

            extraSensorMap
          } else {
            val extraSensorMapNew = concurrent.TrieMap[Long, mutable.Set[String]](sensorPartitionID -> sensorSet)

            extraSensorMapNew
          }

          sensorPartitionExtraList.put(key, extraSensorMap)
        }
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("range" -> "筛选出额外的sensor失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }

  /**
   *  修改elemSensorConfig
   */
  def modifyElemSensorConfig(keySet: mutable.Set[String], windowConfigData: WindowConfigData, status: Boolean): Unit = {
    try{
      // 组装ElemWindowConfigAlias
      val elemWindowConfigAliasList = windowConfigData.sensorAlias.map(x => {
        ElemWindowConfigAlias(controlWindowId = windowConfigData.controlWindowId,
          sensorAliasId = x.sensorAliasId,
          sensorAliasName = x.sensorAliasName,
          svid = x.svid,
          indicatorId = x.indicatorId,
          chamberName = x.chamberName,
          toolName = x.toolName)
      })

      if (status) {
        // 添加
        keySet.foreach(key => {

//          // 清空原来的配置
//          if(elemSensorConfig.contains(key)){
//            elemSensorConfig.remove(key)
//          }

          elemWindowConfigAliasList.foreach(elem => {

             val sensorAliasName = elem.sensorAliasName.toString
             if(sensorAliasName != null) {

               // 存在 tool | chamber
               if (elemSensorConfig.contains(key)) {
                 val sensorConfigMap = elemSensorConfig(key)

                 // 存在sensorAliasName
                 if (sensorConfigMap.contains(sensorAliasName)) {
                   val aliasSet = sensorConfigMap(sensorAliasName)
                   aliasSet.add(elem)
                   sensorConfigMap.put(sensorAliasName, aliasSet)
                 } else {
                   sensorConfigMap.put(sensorAliasName, mutable.Set(elem))
                 }

                 elemSensorConfig.put(key, sensorConfigMap)
               } else {
                 val svidMap = concurrent.TrieMap[String, mutable.Set[ElemWindowConfigAlias]](sensorAliasName.toString -> mutable.Set(elem))
                 elemSensorConfig.put(key, svidMap)
               }
             }
          })
        })

      }else{
        // 删除
        keySet.foreach(key => {

          elemWindowConfigAliasList.foreach(elem => {
            val sensorAliasName = elem.sensorAliasName.toString

            // 存在 tool | chamber
            if(elemSensorConfig.contains(key)){
              val sensorConfigMap = elemSensorConfig(key)

              // 存在svid
              if(sensorConfigMap.contains(sensorAliasName)){
                val aliasSet = sensorConfigMap(sensorAliasName)
                aliasSet.remove(elem)
                sensorConfigMap.put(sensorAliasName, aliasSet)
              }
              elemSensorConfig.put(key, sensorConfigMap)
            }
          })
        })
      }
    } catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("range" -> "修改elemSensorConfig配置失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }

  /**
   *  修改WindowConfigMap
   */
  def modifyWindowConfigMap(keySet: mutable.Set[String], windowConfigData: WindowConfigData, status: Boolean): Unit = {
    try{
      val planKey = s"${windowConfigData.controlPlanId}|${windowConfigData.contextId}|${windowConfigData.controlWindowId}"

      // 删除或者添加
      if (status) {
        keySet.foreach(key => {
          try {
            val elem = WindowConfigData(
              contextId = windowConfigData.contextId,
              controlWindowId = windowConfigData.controlWindowId,
              parentWindowId = windowConfigData.parentWindowId,
              controlWindowType = windowConfigData.controlWindowType,
              isTopWindows = windowConfigData.isTopWindows,
              isConfiguredIndicator = windowConfigData.isConfiguredIndicator,
              windowStart = windowConfigData.windowStart,
              windowEnd = windowConfigData.windowEnd,
              controlPlanId = windowConfigData.controlPlanId,
              controlPlanVersion = windowConfigData.controlPlanVersion,
              calcTrigger = windowConfigData.calcTrigger,
              sensorAlias = List()
            )

//            logger.warn(s"MainFabLog: addWindowConfig :contextId: ${elem.contextId} config :$elem")

            if (!windowConfigMap.contains(key)) {
              val windowMap = concurrent.TrieMap[String, WindowConfigData](planKey -> elem)
              windowConfigMap.put(key, windowMap)
            } else {
              val configMap = windowConfigMap(key)

              if (!configMap.contains(planKey)) {
                //新window
                configMap.put(planKey, elem)
                windowConfigMap.put(key, configMap)
              } else {
                //已有,看版本是不是比当前大，如果大才更新
                val oldVersion = configMap(planKey).controlPlanVersion

                if (oldVersion <= elem.controlPlanVersion) {
                  configMap.put(planKey, elem)
                  windowConfigMap.put(key, configMap)
                } else {
                  logger.warn(s"window 配置 后台推送了一个小版本，没有更新 当前生效老版本：$oldVersion 推送的新版本：${elem.controlPlanVersion} data:${elem.toJson}")
                }
              }
            }
          } catch {
            case e: Exception =>
              logger.warn(s"add WindowConfig Exception:${ExceptionInfo.getExceptionInfo(e)} data:${windowConfigData.toJson}")
          }
        })
      }else {
        logger.warn(s"MainFabLog: Delete WindowEndConfig: contextId : ${windowConfigData.contextId}  config :$windowConfigData")
        keySet.foreach(key => {
          if (!windowConfigMap.contains(key)) {
            logger.warn(s"MainFabLog: Delete WindowEndConfigMap error")
          } else {
            val configMap = windowConfigMap(key)
            configMap.remove(planKey)
            windowConfigMap.put(key, configMap)
          }
        })
      }
    } catch {
        case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
          Map("range" -> "修改WindowConfigMap配置失败"), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  /**
   * 添加context config
   */
  def addContextMapToContextMap(configList: ConfigData[List[ContextConfigData]]): Unit = {
    try {
      for (elem <- configList.datas) {
        val key = s"${elem.toolName}|${elem.chamberName}"
        if (configList.status) {
          if(contextMap.contains(elem.contextId)){
            val KeySet = contextMap(elem.contextId)
            KeySet.add(key)
            contextMap.put(elem.contextId, KeySet)
          }else{
            contextMap.put(elem.contextId, mutable.Set(key))
          }
        }else{
          if(contextMap.contains(elem.contextId)){
            val KeySet = contextMap(elem.contextId)
            KeySet.remove(key)
            contextMap.put(elem.contextId, KeySet)
          }
        }
      }
    }catch {
      case e: Exception =>
        logger.warn(ErrorCode("002003b010C", System.currentTimeMillis(), Map(), e.toString).toJson)
    }
  }

  /**
    * 生成日志信息
    * @param debugCode
    * @param message
    * @param paramsInfo
    * @param dataInfo
    * @param exception
    * @return
    */
  def generateMainFabLogInfo(debugCode:String,funName:String,message:String,paramsInfo:Map[String,Any]=null,dataInfo:String="",exception:String = ""): MainFabLogInfo = {
    MainFabLogInfo(mainFabDebugCode = job_fun_DebugCode + debugCode,
      jobName = jobName,
      optionName = optionName,
      functionName = funName,
      logTimestamp = DateTimeUtil.getCurrentTimestamp,
      logTime = DateTimeUtil.getCurrentTime(),
      message = message,
      paramsInfo = paramsInfo,
      dataInfo = dataInfo,
      exception = exception)
  }
}

