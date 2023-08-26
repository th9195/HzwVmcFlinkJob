package com.hzw.fdc.function.online.MainFabFile

import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{ConfigData, ContextConfigData, ErrorCode, MainFabPTRawData, MainFabRawData, RawTrace, RunEventData, SensorNameData}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{ContextDataType, readHbaseAllConfig}

import scala.collection.JavaConversions.iterableAsScalaIterable
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}

class MainFabRawDataKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,JsonNode,
  JsonNode, (List[String], String, String, Long)] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRawDataKeyedBroadcastProcessFunction])

  val contextMap = new concurrent.TrieMap[String,  ContextConfigData]()

  private var eventStartState:ValueState[RunEventData] = _

  private var sensorNameState: ListState[String] = _

  private var contentState: ListState[String] = _

  // {"traceId": (time1, time2)}
  private val traceIdTimeMap = new concurrent.TrieMap[String,  mutable.Set[String]]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val contextConfigList =  if(ProjectConfig.MAINFAB_READ_HBASE_CONFIG_ONCE) {
      InitFlinkFullConfigHbase.ContextConfigList
    }else {
      readHbaseAllConfig[List[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE, ContextDataType)
    }

    //    val contextConfigList =

    contextConfigList.foreach(addContextConfig)

    // 26小时过期
    val ttlConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    val eventStartStateDescription = new
        ValueStateDescriptor[RunEventData]("FileRunEventStartState", TypeInformation.of(classOf[RunEventData]))
    eventStartStateDescription.enableTimeToLive(ttlConfig)
    eventStartState = getRuntimeContext.getState(eventStartStateDescription)

    val rawDataStateDescription: ListStateDescriptor[String] = new
        ListStateDescriptor[String]("FileListRawTraceState", TypeInformation.of(classOf[String]))
    rawDataStateDescription.enableTimeToLive(ttlConfig)
    sensorNameState = getRuntimeContext.getListState(rawDataStateDescription)

    val contentStateDescription: ListStateDescriptor[String] = new
        ListStateDescriptor[String]("FileListContentState", TypeInformation.of(classOf[String]))
    contentStateDescription.enableTimeToLive(ttlConfig)
    contentState = getRuntimeContext.getListState(contentStateDescription)

    InitFlinkFullConfigHbase.ContextConfigList.clear()
  }

  /**
   *  数据流
   */
  override def processElement(value: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode,
    JsonNode, (List[String], String, String, Long)]#ReadOnlyContext, collector: Collector[(List[String], String, String, Long)]): Unit = {
    try {
      val dataType = value.get(MainFabConstants.dataType).asText()

      if (dataType == MainFabConstants.eventEnd) {
        var toolGroupName = ""
        var chamberGroupName = ""
        var recipeGroupName = ""
        var toolId = ""
        var chamberId = ""
        var recipeId = ""

        val RunEventEnd = toBean[RunEventData](value)

        val key = s"${RunEventEnd.toolName}|${RunEventEnd.chamberName}|${RunEventEnd.recipeName}"

        if (contextMap.contains(key)) {
          val context = contextMap(key)
          toolGroupName = context.toolGroupName
          chamberGroupName = context.chamberGroupName
          recipeGroupName = context.recipeGroupName
          toolId = context.toolId.toString
          chamberId = context.chamberId.toString
          recipeId = context.recipeId.toString
        }

        val RunEventStart = eventStartState.value()

        if(RunEventStart != null) {
          val rawTrace = RawTrace(RunEventEnd.locationName,
            RunEventEnd.moduleName,
            RunEventEnd.toolName,
            toolId,
            RunEventEnd.chamberName,
            chamberId,
            RunEventEnd.recipeName,
            recipeId,
            toolGroupName,
            chamberGroupName,
            recipeGroupName,
            RunEventEnd.dataMissingRatio.toString,
            RunEventStart.runStartTime,
            RunEventEnd.runEndTime,
            RunEventEnd.runId,
            RunEventEnd.traceId,
            RunEventEnd.materialName,
            RunEventEnd.lotMESInfo,
            List()
          )

          val res = parseRawTrace(rawTrace)
          val content = res._1
          // 文件内容不能为空   修复 F2P2-51 【2.0】【一线问题】odhown time 期间。写文件会写两份，部分文件一直是running状态
          if (content.nonEmpty && traceIdTimeMap.contains(RunEventEnd.traceId)) {
            collector.collect(res)
          }

        } else{
          logger.warn(s"011007d002A: " + RunEventEnd)
        }

        traceIdTimeMap.remove(RunEventEnd.traceId)
        contentState.clear()
        sensorNameState.clear()
        eventStartState.clear()

        close()

      } else if (dataType == MainFabConstants.eventStart) {

        val RunEventStart = toBean[RunEventData](value)
        eventStartState.update(RunEventStart)

      } else if(dataType == MainFabConstants.rawData){
        val RawData = toBean[MainFabPTRawData](value)

        if(!traceIdTimeMap.contains(RawData.traceId)){
          traceIdTimeMap.put(RawData.traceId, mutable.Set())
        }
        val timeSet = traceIdTimeMap(RawData.traceId)

//        val sensorNameList = sensorNameState.get().toList
        val sensorNameList = sensorNameState.get().to[ListBuffer]
        if(sensorNameList.nonEmpty){
          // content 内容不为空, 之前有rawdata数据
          val sensorNameTmp = RawData.data.map(x => {
            x.sensorName
          }).distinct

          sensorNameTmp.foreach(elem => {
            if(!sensorNameList.contains(elem)){
              sensorNameState.add(elem)
              sensorNameList.append(elem)
            }
          })
          // content 内容不为空, 之前有rawdata数据

          val content = addRawData(RawData, sensorNameList.toList)

          if(!timeSet.contains(RawData.timestamp.toString)){
            contentState.add(content)
            timeSet.add(RawData.timestamp.toString)
            traceIdTimeMap.put(RawData.traceId, timeSet)
          }

        }else{
          // content内容为空, 代表这是eventstart后的第一条rawdata数据
          var headLine = s"Timestamp\t"
          val sensorNameList = RawData.data.map(x => {
            x.sensorName
          }).distinct

          sensorNameList.foreach(sensorNameState.add(_))

          // 0 down time 期间去重
          timeSet.add(RawData.timestamp.toString)
          traceIdTimeMap.put(RawData.traceId, timeSet)

          for (senorName <- sensorNameList) {
            headLine += s"$senorName\t"
          }
          headLine += "\n"

          contentState.add(headLine)

          contentState.add(addRawData(RawData, sensorNameList))
        }
      }
    }catch {
      case e: Exception => logger.warn(s"processElement error $e :${value}")
    }
  }

  /**
   *  广播流
   */
  override def processBroadcastElement(in2: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode,
    JsonNode, (List[String], String, String, Long)]#Context,
                                       collector: Collector[(List[String], String, String, Long)]): Unit = {
    try {
      val dataType = in2.get(MainFabConstants.dataType).asText()
      if (dataType == MainFabConstants.context) {
        val contextConfig = toBean[ConfigData[List[ContextConfigData]]](in2)
        addContextConfig(contextConfig)
      }
    } catch {
      case e: Exception => logger.warn(s"contextConfig json error $e :${in2}")
    }
  }


  /**
   *  增加当前sensor的原始数据
   */
  def addRawData(RawData: MainFabPTRawData, sensorNameList: List[String]): String = {
    val contentMsg = new StringBuilder()

    val map = RawData.data.map(x => {
      (x.sensorName, x.sensorValue)
    }).toMap

    val timestamp = RawData.timestamp
    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
    val timestampStr = fm.format(timestamp)

    contentMsg.append(timestampStr + "\t")
    for (sensor <- sensorNameList) {
      val sensorValue = if (map.contains(sensor)) {
        val sensorValue = map(sensor)
        if(sensorValue == "N.A.") "" else  sensorValue
      } else {
        ""
      }
      contentMsg.append(sensorValue + "\t")
    }
    contentMsg.append("\n")

    contentMsg.toString()
  }


  /**
   *  解析rawTrace
   */
  def parseRawTrace(value: RawTrace): (List[String], String, String, Long) = {
    try{
      val toolName = value.toolName
      val chamberName = value.chamberName
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val timestampStr = fm.format(value.runEndTime).replace(" ", "T")

      val runId = toolName + "--" + chamberName + "--" + value.runStartTime

      val NA = "N.A."
      // 输出内容
      var content = "<ProcessContext>\n  <Location>\n"
      content += "    <Factory name=\"%s\"/>\n".format(value.locationName)
      content += "    <Line/>\n"
      content += "    <Area name=\"%s\"/>\n".format(value.moduleName)
      content += "    <Department/>\n"
      content += "    <EquipmentType name=\"%s\"/>\n".format(value.toolGroupName)
      content += "    <Equipment name=\"%s\"/>\n".format(value.toolName)
      content += "    <ModuleType name=\"%s\"/>\n".format(value.chamberGroupName)
      content += "    <Module name=\"%s\"/>\n".format(value.chamberName)
      content += s"    <SubSystems/>\n"
      content += s"  </Location>\n"

      content += s"  <Process>\n"
      content += "    <Recipe name=\"%s\"/>\n".format(value.recipeName)
      content += "    <RecipeGroup name=\"%s\"/>\n".format(value.recipeGroupName)
      content += "    <ControlJobName/>\n"
      content += "    <ProcessJobName name=\"%s\"/>\n".format(NA)
      content += s"    <ProcessTags>\n"
      content += "      <Tag name=\"EquipmentID\" value=\"" + value.toolId + "\"/>\n"
      content += "      <Tag name=\"ModuleID\" value=\"" + value.chamberId + "\"/>\n"
      content += "      <Tag name=\"RecipeID\" value=\"" + value.recipeId + "\"/>\n"
      content += "      <Tag name=\"DCContextID\" value=\"" + runId + "\"/>\n"
      val startTime = fm.format(value.runStartTime).replace(" ", "T")
      content += s"    </ProcessTags>\n"
      content += "    <ProcessTime start=\"" + startTime + "\" stop=\"" + timestampStr + "\" DCQV=\"%s\"/>\n".format(value.dataMissingRatio.replace("-1.0", "3.33"))
      content += s"  </Process>\n"


      content += "  <Material materialName=\"%s\">\n".format(value.materialName)
      val lotMESInfo = value.lotMESInfo
      if (lotMESInfo.nonEmpty) {
        for (lotMes <- lotMESInfo if lotMes.nonEmpty) {

          content += "    <Lot name=\"%s\" carrier=\"%s\" layer=\"%s\" operation=\"%s\" product=\"%s\" route=\"%s\" stage=\"%s\" technology=\"%s\" type=\"%s\">\n".
            format(lotMes.get.lotName.get, lotMes.get.carrier.get, lotMes.get.layer.get, lotMes.get.operation.get, lotMes.get.product.get, lotMes.get.route.get, lotMes.get.stage.get, lotMes.get.technology.get, lotMes.get.lotType.get)
          content += s"      <LotProcessTags/>\n"
          content += s"      <LotTags/>\n"
          content += s"      <Wafers>\n"
          val wafers = lotMes.get.wafers
          if(wafers.nonEmpty) {
            for (wafer <- wafers) {
              content += "        <Wafer name=\"%s\" carrierSlot=\"%s\">\n".format(wafer.get.waferName.get, wafer.get.carrierSlot.get)
              content += s"          <WaferTags>\n"
              content += "            <Tag name=\"AeWaferKey\" value=\"" + NA + "\"/>\n"
              content += s"          </WaferTags>\n"
              content += s"          <WaferProcessTags/>\n"
              content += s"        </Wafer>\n"
            }
          }else{
            content += "        <Wafer name=\"%s\" carrierSlot=\"%s\">\n".format(NA, NA)
            content += s"          <WaferTags>\n"
            content += "            <Tag name=\"AeWaferKey\" value=\"" + NA + "\"/>\n"
            content += s"          </WaferTags>\n"
            content += s"          <WaferProcessTags/>\n"
            content += s"        </Wafer>\n"
          }
          content += s"      </Wafers>\n"
          content += s"    </Lot>\n"

        }
      }else{
        content += "    <Lot name=\"%s\" carrier=\"%s\" layer=\"%s\" operation=\"%s\" product=\"%s\" route=\"%s\" stage=\"%s\" technology=\"%s\" type=\"%s\">\n".
          format(NA, NA, NA, NA, NA, NA, NA, NA, NA)
        content += s"      <LotProcessTags/>\n"
        content += s"      <LotTags/>\n"
        content += s"      <Wafers>\n"
        content += "        <Wafer name=\"%s\" carrierSlot=\"%s\">\n".format(NA, NA)
        content += s"          <WaferTags>\n"
        content += "            <Tag name=\"AeWaferKey\" value=\"" + NA + "\"/>\n"
        content += s"          </WaferTags>\n"
        content += s"          <WaferProcessTags/>\n"
        content += s"        </Wafer>\n"
        content += s"      </Wafers>\n"
        content += s"    </Lot>\n"
      }

      content += s"  </Material>\n"
      content += s"</ProcessContext>\n"

      val sensorContentMsgList = contentState.get().toList
      if(sensorContentMsgList.nonEmpty){
        val res: List[String] = content::sensorContentMsgList
        (res, value.toolName, value.chamberName, value.runStartTime)
      }else{
        (List(), "", "", 0L)
      }

      //      val contentMsg = new StringBuffer(content)
      //
      //      var headLine = s"Timestamp\t"
      //      val sensorNameSet = value.ptSensor.flatMap(x => {
      //        x.data.map(_.sensorName)
      //      }).distinct
      //
      //      for (senorName <- sensorNameSet) {
      //        headLine += s"$senorName\t"
      //      }
      //      contentMsg.append(headLine + "\n")
      //
      //      // 按时间排序
      //      val ptSensor = value.ptSensor.sortWith(_.timestamp < _.timestamp)
      //
      //      for (sensor <- ptSensor) {
      //        contentMsg.append(sensor.timestamp + "\t")
      //        val map = sensor.data.map(x => {
      //          (x.sensorName, x.sensorValue)
      //        }).toMap
      //        for (sensorName <- sensorNameSet) {
      //          val sensorValue = if (map.contains(sensorName)) {
      //            map(sensorName)
      //          } else {
      //            s"$NA"
      //          }
      //          contentMsg.append(sensorValue + "\t")
      //        }
      //        contentMsg.append("\n")
      //      }

    }catch {
      case exception: Exception => logger.warn(ErrorCode("011007d001C", System.currentTimeMillis(), Map("msg" -> "解析RawTrace异常"),
        ExceptionInfo.getExceptionInfo(exception)).toJson)
        (List(), "", "", 0L)
    }
  }

  //  /**
  //   *   缓存rawData信息
  //   */
  //  def addRawDataState(in2: MainFabPTRawData): Unit = {
  //    val timestamp = in2.timestamp
  //    val fm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
  //    val timestampStr = fm.format(timestamp)
  //
  //    val sensor = SensorNameData(in2.toolName, in2.chamberName, timestampStr, in2.data)
  //    rawDataState.add(sensor)
  //  }

  /**
   *  增加context配置
   */
  def addContextConfig(configList: ConfigData[List[ContextConfigData]]):Unit = {
    if (configList.status) {
      for (elem <- configList.datas) {
        try {
          val key = s"${elem.toolName}|${elem.chamberName}|${elem.recipeName}"
          contextMap.put(key, elem)
        } catch {
          case e: Exception =>
            logger.warn(ErrorCode("002003b009C", System.currentTimeMillis(),
              Map("contextConfig" -> elem), e.toString).toJson)
        }
      }
    }
  }
}
