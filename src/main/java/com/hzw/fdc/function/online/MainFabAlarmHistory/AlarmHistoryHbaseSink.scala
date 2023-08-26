package com.hzw.fdc.function.online.MainFabAlarmHistory

import java.text.SimpleDateFormat
import java.util.{Date, Optional}

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ProjectConfig, SnowFlake}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}


class AlarmHistoryHbaseSink extends RichSinkFunction[List[JsonNode]] {

  var mutator: BufferedMutator = _
  var mutator_run: BufferedMutator = _

  var connection: Connection = _
  var count = 0
  val INDICATOR_BATCH_WRITE_HBASE_NUM = 500
  val snowFlake: SnowFlake = new SnowFlake(0, 0)
  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmHistoryHbaseSink])

  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    //创建hbase连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    connection = ConnectionFactory.createConnection(conf)

    /**
     * 连接hbase表
     */
    startHbaseTable()
  }

  /**
   * 每条数据写入
   *
   */
  override def invoke(jsonNodes: List[JsonNode]): Unit = {
    jsonNodes.grouped(INDICATOR_BATCH_WRITE_HBASE_NUM).foreach(datas => {
      try {
        datas.foreach(jsonNode => {
          try {
            val dataType = jsonNode.findPath("dataType").asText()
            if ("AlarmLevelRule".equals(dataType)) {
              processAlarm2Hbase(jsonNode)
            } else if ("micAlarm".equals(dataType)) {
              processMicAlarm2Hbase(jsonNode)
            }
          }catch {
            case ex: Exception => logger.warn("invoke datas error: " + ex.toString)
          }
        })
        mutator.flush()
        mutator_run.flush()
      }catch {
        case ex: Exception => logger.warn("flush error: " + ex.toString)
          /**
           * 连接hbase表
           */
          startHbaseTable()
      }
    })
  }
//  override def invoke(jsonNode: List[JsonNode]): Unit = {
//    var table: Table = null
//    try {
//      table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_ALARM_HISTROY_TABLE))
//      val dataType = jsonNode.findPath("dataType").asText()
//      if ("AlarmLevelRule".equals(dataType)) {
//
//        // todo AlarmLevelRule
//        processAlarm2Hbase(table, jsonNode)
//
//      } else if ("micAlarm".equals(dataType)) {
//        jsonNode.foreach(elem => {
//          processMicAlarm2Hbase(table, elem)
//        })
//      }
//      //      resultFuture.complete(List("ok"))
//    } catch {
//      case ex: Exception =>
//        logger.warn(s"alarm job invokeAlarmHistoryHbaseSink Exception:$ex data: $jsonNode")
//      //        resultFuture.complete(List("fail"))
//    }finally {
//      if(table != null){
//        table.close()
//      }
//    }
//  }


  /**
   *  hbase表连接
   */
  def startHbaseTable(): Unit ={
    try {
      val tableName: TableName = TableName.valueOf(ProjectConfig.HBASE_ALARM_HISTROY_TABLE)
      val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
      //设置缓存2m，当达到1m时数据会自动刷到hbase
      params.writeBufferSize(2 * 1024 * 1024) //设置缓存的大小
      mutator = connection.getBufferedMutator(params)

      //mainfab_rundata_table表连接
      val tableName_run: TableName = TableName.valueOf(ProjectConfig.HBASE_MAINFAB_RUNDATA_TABLE)
      val params_run: BufferedMutatorParams = new BufferedMutatorParams(tableName_run)
      params_run.writeBufferSize(128 * 1024)
      mutator_run = connection.getBufferedMutator(params_run)
    }catch {
      case ex: Exception => logger.warn("startHbaseTable error: " + ex.toString)
    }
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    if(mutator != null){
      mutator.close()
    }
    if(mutator_run != null){
      mutator_run.close()
    }
    if (connection != null) {
      connection.close()
    }
    super.close()

  }

  def hasLength(str: String): String = {
    if (str != null) {
      str
    } else {
      ""
    }
  }


  /**
   * 处理 AlarmLevelRule 数据 写入Hbase mainfab_alarm_history 表 + 写入一个isAlarm 标识位到 Hbase mainfab_rundata_table 表
   *
   */
  def processAlarm2Hbase(jsonNode: JsonNode): Unit = {
    val value = toBean[FdcData[AlarmRuleResult]](jsonNode)
    val alarm = value.datas
    if (alarm.ruleTrigger.equals("N/A")) {
      //          resultFuture.complete(List("ok"))
      return
    }

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val PRODUCT_NAME = if(alarm.productName!=null) StringUtils.removeEnd(StringUtils.removeStart(alarm.productName.toString,"List("),")") else ""
    val STAGE = if(alarm.stage!=null) StringUtils.removeEnd(StringUtils.removeStart(alarm.stage.toString,"List("),")") else ""
    val CONTROL_PLAN_VERSION = if(alarm.controlPlanVersion!=null) alarm.controlPlanVersion.toString else ""
    val RUN_ID = if(alarm.runId!=null) alarm.runId else ""
    val MATERIAL_NAME = if(alarm.materialName != null) alarm.materialName else ""
    val TOOL_GROUP_ID = if(alarm.toolGroupId != null) alarm.toolGroupId.toString else ""
    val TOOL_GROUP_NAME = if(alarm.toolGroupName != null) alarm.toolGroupName else ""
    val TOOL_ID = if(alarm.toolId != null ) alarm.toolId.toString else ""
    val TOOL_NAME = if(alarm.toolName != null) alarm.toolName else ""
    val CHAMBER_GROUP_ID = if(alarm.chamberGroupId != null) alarm.chamberGroupId.toString else ""
    val CHAMBER_GROUP_NAME = if (alarm.chamberGroupName != null) alarm.chamberGroupName else ""
    val CHAMBER_ID = if(alarm.chamberId != null) alarm.chamberId.toString else ""
    val CHAMBER_NAME = if(alarm.chamberName != null) alarm.chamberName else ""
    val RECIPE_ID = if(alarm.recipeId != null) alarm.recipeId.toString else ""
    val RECIPE_NAME = if(alarm.recipeName != null) alarm.recipeName else ""
    val CONTROL_PLAN_ID = if(alarm.controlPlnId != null) alarm.controlPlnId.toString else ""
    val CONTROL_PLAN_NAME = if(alarm.controlPlanName != null) alarm.controlPlanName else ""
    val CONFIG_MISSING_RADIO = if(alarm.configMissingRatio != null) alarm.configMissingRatio.toString else ""
    val DATA_MISSING_RADIO = if(alarm.dataMissingRatio != null) alarm.dataMissingRatio.toString else ""
    val RUN_START_TIME = if(alarm.runStartTime != null) format.format(new Date(alarm.runStartTime)) else ""
    val RUN_END_TIME = if(alarm.runEndTime != null) format.format(new Date(alarm.runEndTime)) else ""
    val WINDOW_START_TIME = if(alarm.windowStartTime != null)format.format(new Date(alarm.windowStartTime)) else ""
    val WINDOW_END_TIME = if(alarm.windowEndTime != null) format.format(new Date(alarm.windowEndTime)) else ""
    val WINDOW_DATA_CREATE_TIME = if(alarm.windowDataCreateTime != null) format.format(new Date(alarm.windowDataCreateTime)) else ""
    val ALARM_CREATE_TIME = if(alarm.alarmCreateTime != null) format.format(new Date(alarm.alarmCreateTime)) else ""
    val LOCATION_ID = if(alarm.locationId != null) alarm.locationId.toString else ""
    val LOCATION_NAME = if(alarm.locationName != null) alarm.locationName else ""
    val MODULE_ID = if(alarm.moduleId != null) alarm.moduleId.toString else ""
    val MODULE_NAME = if(alarm.moduleName != null) alarm.moduleName else ""
    val AREA = if(alarm.area != null) alarm.area else ""
    val SECTION = if(alarm.section != null) alarm.section else ""
    val MES_CHAMBER_NAME = if(alarm.mesChamberName != null) alarm.mesChamberName else ""
    val LOT_MES_INFO_LOTNAMES = if(alarm.lotMESInfo != null) alarm.lotMESInfo.map(lotOption => {
      var lotname = ""
      if(lotOption.nonEmpty){
        val lot = lotOption.get
        lotname = lot.lotName.getOrElse("")
      }
      lotname
    }).mkString("/") else ""

    val LOT_MES_INFO_LAYERS = if(alarm.lotMESInfo != null) alarm.lotMESInfo.map(lotOption => {
      var layer = ""
      if(lotOption.nonEmpty){
        val lot = lotOption.get
        layer = lot.layer.getOrElse("")
      }
      layer
    }).mkString("/") else ""

    val LOT_MES_INFO_WAFERS = if(alarm.lotMESInfo != null) alarm.lotMESInfo.map(lot => {
      if(lot.nonEmpty){
        val wafers = lot.get.wafers
        if(wafers.nonEmpty){
          wafers.map(wafer => {
            var waferName = ""
            if(wafer.get.waferName.nonEmpty){
              waferName = wafer.get.waferName.getOrElse("")
            }
            waferName
          }).mkString("$")
        }
      }
    }).mkString("/") else ""

    val ALARM_TYPE = "alarm"
    val ALARM_ID = if(alarm.indicatorId != null) alarm.indicatorId.toString else ""
    val ALARM_NAME = if(alarm.indicatorName != null) alarm.indicatorName else ""
    val ALARM_VALUE = if(alarm.indicatorValue != null) alarm.indicatorValue else ""

    val rules: List[Rule] = alarm.getRULE
    var ALARM_ACTION = ""
    var ALARM_NOTIFICATION = ""
    var ALARM_LIMIT = ""
    if(rules.nonEmpty){
      rules.map(rule => {
        rule.alarmInfo.map(alarmRuleParam =>{
          alarmRuleParam.action.map(action=>{
            if( action.sign.nonEmpty  && "action".equalsIgnoreCase(action.sign.get) && action.`type`.nonEmpty){
              ALARM_ACTION = ALARM_ACTION + "/" + action.`type`.get
            }
          })
        })
      })

      rules.foreach((rule: Rule) => {
        rule.alarmInfo.foreach(alarmRuleParam =>{
          alarmRuleParam.action.foreach(action=>{
            if(action.sign.nonEmpty  && "notification".equalsIgnoreCase(action.sign.get) && action.`type`.nonEmpty){
              ALARM_NOTIFICATION = ALARM_NOTIFICATION + "/" + action.`type`.get
            }
          })
        })
      })

      val headRule: Rule = rules.head
      val limit = headRule.limit
      ALARM_LIMIT = getAlarmLimit(limit)
    }

    val ALARM_ACTION_NAME = ALARM_ACTION + ALARM_NOTIFICATION

    val ALARM_RULE_TRIGGER = if(alarm.ruleTrigger != null) alarm.ruleTrigger else ""
    val ALARM_INDICATOR_CREATE_TIME = if(alarm.indicatorCreateTime != null) format.format(new Date(alarm.indicatorCreateTime)) else ""
    val ALARM_LEVEL = if(alarm.alarmLevel != null) alarm.alarmLevel.toString else ""

    val toolMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(TOOL_NAME))
    val saltKey = StringUtils.leftPad(Integer.toString(Math.abs(toolMd5.hashCode % 10000)), 4, '0')
    val timestatmp = alarm.alarmCreateTime
    val reverseTime = Long.MaxValue - timestatmp
    val alarmNo = snowFlake.nextId().toString
    val rowkey = s"$saltKey|$toolMd5|$reverseTime|$alarmNo"
    val put = new Put(Bytes.toBytes(rowkey))

    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT_NAME"), Bytes.toBytes(PRODUCT_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(STAGE))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_VERSION"), Bytes.toBytes(CONTROL_PLAN_VERSION))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MATERIAL_NAME"), Bytes.toBytes(MATERIAL_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_GROUP_ID"), Bytes.toBytes(TOOL_GROUP_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_GROUP_NAME"), Bytes.toBytes(TOOL_GROUP_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_ID"), Bytes.toBytes(TOOL_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_NAME"), Bytes.toBytes(TOOL_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_GROUP_ID"), Bytes.toBytes(CHAMBER_GROUP_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_GROUP_NAME"), Bytes.toBytes(CHAMBER_GROUP_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_ID"), Bytes.toBytes(CHAMBER_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_NAME"), Bytes.toBytes(CHAMBER_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE_ID"), Bytes.toBytes(RECIPE_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE_NAME"), Bytes.toBytes(RECIPE_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_ID"), Bytes.toBytes(CONTROL_PLAN_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_NAME"), Bytes.toBytes(CONTROL_PLAN_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONFIG_MISSING_RADIO"), Bytes.toBytes(CONFIG_MISSING_RADIO))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("DATA_MISSING_RADIO"), Bytes.toBytes(DATA_MISSING_RADIO))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_START_TIME"), Bytes.toBytes(WINDOW_START_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_END_TIME"), Bytes.toBytes(WINDOW_END_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(WINDOW_DATA_CREATE_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_CREATE_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOCATION_ID"), Bytes.toBytes(LOCATION_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOCATION_NAME"), Bytes.toBytes(LOCATION_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MODULE_ID"), Bytes.toBytes(MODULE_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MODULE_NAME"), Bytes.toBytes(MODULE_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("AREA"), Bytes.toBytes(AREA))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SECTION"), Bytes.toBytes(SECTION))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MES_CHAMBER_NAME"), Bytes.toBytes(MES_CHAMBER_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOT_MES_INFO_LOTNAMES"), Bytes.toBytes(LOT_MES_INFO_LOTNAMES))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOT_MES_INFO_LAYERS"), Bytes.toBytes(LOT_MES_INFO_LAYERS))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOT_MES_INFO_WAFERS"), Bytes.toBytes(LOT_MES_INFO_WAFERS))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TYPE"), Bytes.toBytes(ALARM_TYPE))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ID"), Bytes.toBytes(ALARM_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(ALARM_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_VALUE"), Bytes.toBytes(ALARM_VALUE))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION_NAME"), Bytes.toBytes(ALARM_ACTION_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION"), Bytes.toBytes(ALARM_ACTION))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LIMIT"), Bytes.toBytes(ALARM_LIMIT))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE"), Bytes.toBytes(ALARM_RULE_TRIGGER))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_INDICATOR_CREATE_TIME"), Bytes.toBytes(ALARM_INDICATOR_CREATE_TIME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_NOTIFICATION"), Bytes.toBytes(ALARM_NOTIFICATION))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes(ALARM_LEVEL))

//    logger.error(s"rowkey == ${rowkey}")
//    table.put(put)

    //F2P2-2 添加isAlarm 标识位到mainfab_rundata_table
    var isAlarm = 0
    if(!rules.isEmpty){
      isAlarm = 1
    }
    mutator_run.mutate(buildPut(TOOL_NAME,CHAMBER_NAME,alarm.runStartTime.toString,isAlarm))

    mutator.mutate(put)
    //每满500条刷新一下数据
    if (count >= INDICATOR_BATCH_WRITE_HBASE_NUM){
      mutator.flush()
      mutator_run.flush()
      count = 0
    }
    count = count + 1
  }



  /**
   * 处理 MicAlarmData 数据 写入Hbase mainfab_alarm_history 表 + 写入一个isAlarm 标识位到 Hbase mainfab_rundata_table 表
   */
  def processMicAlarm2Hbase(jsonNode: JsonNode) = {
    val value = toBean[MicAlarmData](jsonNode)
    val alarm = value.datas
    val TOOL_NAME = alarm.toolName
    val CHAMBER_NAME = alarm.chamberName
    val toolMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(TOOL_NAME))
    val saltKey = StringUtils.leftPad(Integer.toString(Math.abs(toolMd5.hashCode % 10000)), 4, '0')

    val timestatmp = alarm.alarmCreateTime
    val reverseTime = Long.MaxValue - timestatmp

    val alarmNo = snowFlake.nextId().toString
    val rowkey = s"$saltKey|$toolMd5|$reverseTime|$alarmNo"

    val put = new Put(Bytes.toBytes(rowkey))

    val RUN_ID = alarm.runId
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val RUN_START_T = format.format(new Date(alarm.runStartTime))
    val RUN_END_T = format.format(new Date(alarm.runEndTime))
    val ALARM_T = format.format(new Date(timestatmp))
    val OCAP: String = if (alarm.action.nonEmpty) {
      val actions = alarm.action.map(_.`type`).filter(x => x.nonEmpty).distinct
      actions.mkString(",")
    } else {
      ""
    }
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_NAME"), Bytes.toBytes(TOOL_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_NAME"), Bytes.toBytes(CHAMBER_NAME))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(if (Optional.ofNullable(alarm.micName).isPresent) alarm.micName else ""))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_NAME"), Bytes.toBytes(if (Optional.ofNullable(alarm.controlPlanName).isPresent) alarm.controlPlanName else ""))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes("0"))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE"), Bytes.toBytes(""))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_ACTION_NAME"), Bytes.toBytes(OCAP))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_T))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_T))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_T))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(ALARM_T))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("DESCRIPTION"), Bytes.toBytes("mic"))

    //F2P2-2 添加isAlarm 标识位到mainfab_rundata_table
    var isAlarm = 0
    if(!alarm.action.isEmpty){
      isAlarm = 1
    }
    mutator_run.mutate(buildPut(TOOL_NAME,CHAMBER_NAME,alarm.runStartTime.toString,isAlarm))

    mutator.mutate(put)
    //每满500条刷新一下数据
    if (count >= INDICATOR_BATCH_WRITE_HBASE_NUM){
      mutator.flush()
      mutator_run.flush()
      count = 0
    }
    count = count + 1
  }

  def buildPut(TOOL_NAME: String,CHAMBER_NAME: String,RUN_START_T: String,isAlarm: Int):Put ={
    val saltKey_run = s"${TOOL_NAME}_${CHAMBER_NAME}".hashCode % 10
    val put_run = new Put(Bytes.toBytes(s"${saltKey_run}_${TOOL_NAME}_${CHAMBER_NAME}_${RUN_START_T}"))
    put_run.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("IS_ALARM"), Bytes.toBytes(isAlarm))
  }

  /**
   * 获取ALARM_LIMIT
   * @param limit
   * @return
   */
  def getAlarmLimit(limit: AlarmRuleLimit): String = {
    var alarm_limit = ""

    if(limit.LSL.nonEmpty){
      alarm_limit = alarm_limit + limit.LSL.get + "/"
    }else{
      alarm_limit = alarm_limit + "/"
    }

    if(limit.LBL.nonEmpty){
      alarm_limit = alarm_limit + limit.LBL.get + "/"
    }else{
      alarm_limit = alarm_limit + "/"
    }

    if(limit.LCL.nonEmpty){
      alarm_limit = alarm_limit + limit.LCL.get + "/"
    }else{
      alarm_limit = alarm_limit + "/"
    }

    if(limit.UCL.nonEmpty){
      alarm_limit = alarm_limit + limit.UCL.get + "/"
    }else{
      alarm_limit = alarm_limit + "/"
    }

    if(limit.UBL.nonEmpty){
      alarm_limit = alarm_limit + limit.UBL.get + "/"
    }else{
      alarm_limit = alarm_limit + "/"
    }

    if(limit.USL.nonEmpty){
      alarm_limit = alarm_limit + limit.USL.get
    }

    alarm_limit
  }

  //  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = ???
  //
  //  override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = ???

}
