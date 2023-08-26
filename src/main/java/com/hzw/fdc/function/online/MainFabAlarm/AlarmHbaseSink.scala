package com.hzw.fdc.function.online.MainFabAlarm

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{AlarmRuleAction, AlarmRuleParam, AlarmRuleResult, ConfigData, FdcData}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.BufferedMutator
import org.apache.hadoop.hbase.client.BufferedMutatorParams
import org.slf4j.{Logger, LoggerFactory}


class AlarmHbaseSink extends RichSinkFunction[List[JsonNode]] {

  var connection: Connection = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmHbaseSink])

  var mutator: BufferedMutator = _
  var count = 0
  val INDICATOR_BATCH_WRITE_HBASE_NUM = 500

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
  override def invoke(value: List[JsonNode]): Unit = {
    value.grouped(INDICATOR_BATCH_WRITE_HBASE_NUM).foreach(datas => {
      try {
        datas.foreach(invokeIndicatorDataSite)
        mutator.flush()
      }catch {
        case ex: Exception => logger.warn("flush error: " + ex.toString)
          /**
           * 连接hbase表
           */
          startHbaseTable()
      }
    })
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    if(mutator != null){
      mutator.close()
    }
    if(connection != null){
      connection.close()
    }
  }




//  /**
//   * 插入hbase
//   *
//   */
//  def invokeIndicatorDataSite(alarmRuleResultJson: JsonNode): Unit = {
//    try {
//      val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_INDICATOR_DATA_TABLE))
//
//      val alarmRuleResult = toBean[FdcData[AlarmRuleResult]](alarmRuleResultJson)
//      val value = alarmRuleResult.datas
//
//      // rowkey查询的主要字段
//      val TOOL_ID = value.toolName
//      val CHAMBER_ID = value.chamberName
//      val INDICATOR_ID = value.indicatorId
//
//      val indicatorSpecialID = TOOL_ID + CHAMBER_ID + INDICATOR_ID
//
//      val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)), 4, '0')
//
//      val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))
//
//
//      val timestatmp = value.windowStartTime
//      val reverseTime = Long.MaxValue - timestatmp
//
//      val rowkey = s"$slatKey|$indicatorSpecialMd5|$reverseTime"
//
//
//      val put = new Put(Bytes.toBytes(rowkey))
//
//      val RUN_ID = value.runId
//      val INDICATOR_NAME = value.indicatorName
//      val INDICATOR_VALUE = value.indicatorValue
//      val RUN_START_TIME = value.runStartTime
//      val RUN_END_TIME = value.runEndTime
//
//      val PRODUCT_ID = if (value.productName.nonEmpty) value.productName.reduce(_ + "," + _) else ""
//      val STAGE = if (value.productName.nonEmpty) value.stage.reduce(_ + "," + _) else ""
//
//      val ALARM_LEVEL = value.alarmLevel
//      val SWITCH_STATUS = value.switchStatus
//      val UNIT = value.unit
//
//
//      val LIMIT = value.limit
//
//      val OCAP: String = if (value.RULE.nonEmpty) {
//        val AlarmRuleParamList: List[AlarmRuleParam] = value.RULE.flatMap(_.alarmInfo)
//        val actionList: List[AlarmRuleAction] = AlarmRuleParamList.flatMap(_.action)
//        val actions = actionList.map(_.`type`).filter(x => x.nonEmpty).map(_.get).distinct
//        actions.mkString(",")
//      } else {
//        ""
//      }
//
//
//      val RULE_TRIGGER = value.ruleTrigger
//      val CONTROL_PLAN_VERSION = value.controlPlanVersion
//      val INDICATOR_TIME = value.indicatorCreateTime
//      val ALARM_TIME = value.alarmCreateTime
//      val RECIPE = value.recipeName
//      val WINDOW_START_TIME = value.windowStartTime
//      val WINDOW_END_TIME = value.windowEndTime
//
//
//      val area = if (value.area != null) value.area else ""
//      val section = if (value.section != null) value.section else ""
//      val mesChamberName = if (value.mesChamberName != null) value.mesChamberName else ""
//      val pmStatus = value.pmStatus
//      val pmTimestamp = value.pmTimestamp
//
//
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_ID"), Bytes.toBytes(TOOL_ID))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_ID"), Bytes.toBytes(CHAMBER_ID))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(RECIPE))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_ID"), Bytes.toBytes(INDICATOR_ID))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(INDICATOR_NAME))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_VALUE"), Bytes.toBytes(INDICATOR_VALUE))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT_ID"), Bytes.toBytes(PRODUCT_ID))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(STAGE))
//
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_TIME))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_TIME))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_START_TIME"), Bytes.toBytes(WINDOW_START_TIME))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_END_TIME"), Bytes.toBytes(WINDOW_END_TIME))
//
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes(ALARM_LEVEL))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LIMIT"), Bytes.toBytes(LIMIT))
//
//
//      if (!OCAP.equals("")) {
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("OCAP"), Bytes.toBytes(OCAP))
//      }
//
//
//      if (pmStatus.equals(MainFabConstants.pmEnd)) {
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PM_FLAG"), Bytes.toBytes(pmStatus))
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PM_TIME"), Bytes.toBytes(pmTimestamp))
//      }
//
//
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE_TRIGGER"), Bytes.toBytes(RULE_TRIGGER))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_VERSION"), Bytes.toBytes(CONTROL_PLAN_VERSION))
//
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_TIME"), Bytes.toBytes(INDICATOR_TIME))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_TIME))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SWITCH_STATUS"), Bytes.toBytes(SWITCH_STATUS))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("UNIT"), Bytes.toBytes(UNIT))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("AREA"), Bytes.toBytes(area))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SECTION"), Bytes.toBytes(section))
//      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MES_CHAMBER_NAME"), Bytes.toBytes(mesChamberName))
//
//      table.put(put)
//      table.close()
//    } catch {
//      case ex: Exception => logger.warn(s"alarmHbaseSink  invokeIndicatorDataSite Exception:${ExceptionInfo.getExceptionInfo(ex)}")
//    }
//  }



  /**
   * 插入hbase
   *
   */
  def invokeIndicatorDataSite(elem: JsonNode): Unit = {
    try {
      try {

        val alarmRuleResult = toBean[ConfigData[AlarmRuleResult]](elem)
        val value = alarmRuleResult.datas

        // rowkey查询的主要字段
        val TOOL_ID = value.toolName
        val CHAMBER_ID = value.chamberName
        val INDICATOR_ID = value.indicatorId

        val indicatorSpecialID = TOOL_ID + CHAMBER_ID + INDICATOR_ID

        val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)),
          4, '0')

        val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))


        val timestatmp = value.windowStartTime
        val reverseTime = Long.MaxValue - timestatmp

        val rowkey = s"$slatKey|$indicatorSpecialMd5|$reverseTime"


        val put = new Put(Bytes.toBytes(rowkey))

        val RUN_ID = value.runId
        val INDICATOR_NAME = value.indicatorName
        val INDICATOR_VALUE = value.indicatorValue
        val RUN_START_TIME = value.runStartTime
        val RUN_END_TIME = value.runEndTime

        val PRODUCT_ID = if (value.productName.nonEmpty) value.productName.reduce(_ + "," + _) else ""
        val STAGE = if (value.productName.nonEmpty) value.stage.reduce(_ + "," + _) else ""

        val ALARM_LEVEL = value.alarmLevel
        val SWITCH_STATUS = value.switchStatus
        val UNIT = value.unit


        val LIMIT = value.limit

        val OCAP: String = if (value.RULE.nonEmpty) {
          val AlarmRuleParamList: List[AlarmRuleParam] = value.RULE.flatMap(_.alarmInfo)
          val actionList: List[AlarmRuleAction] = AlarmRuleParamList.flatMap(_.action)
          val actions = actionList.map(_.`type`).filter(x => x.nonEmpty).map(_.get).distinct
          actions.mkString(",")
        } else {
          ""
        }


        val RULE_TRIGGER = value.ruleTrigger
        val CONTROL_PLAN_VERSION = value.controlPlanVersion
        val INDICATOR_TIME = value.indicatorCreateTime
        val ALARM_TIME = value.alarmCreateTime
        val RECIPE = value.recipeName
        val WINDOW_START_TIME = value.windowStartTime
        val WINDOW_END_TIME = value.windowEndTime
        //          val SPEC_ID = if(value.specId != null){value.specId}else{"0"}
        val CONTROL_PLAN_ID = value.controlPlnId.toString

        val area = if (value.area != null) value.area else ""
        val section = if (value.section != null) value.section else ""
        val mesChamberName = if (value.mesChamberName != null) value.mesChamberName else ""
        val pmStatus = value.pmStatus
        val pmTimestamp = value.pmTimestamp


        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_ID"), Bytes.toBytes(TOOL_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_ID"), Bytes.toBytes(CHAMBER_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(RECIPE))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_ID"), Bytes.toBytes(INDICATOR_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(INDICATOR_NAME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_VALUE"), Bytes.toBytes(INDICATOR_VALUE))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT_ID"), Bytes.toBytes(PRODUCT_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(STAGE))

        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_TIME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_TIME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_START_TIME"), Bytes.toBytes(WINDOW_START_TIME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_END_TIME"), Bytes.toBytes(WINDOW_END_TIME))

        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes(ALARM_LEVEL))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LIMIT"), Bytes.toBytes(LIMIT))


        if (!OCAP.equals("")) {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("OCAP"), Bytes.toBytes(OCAP))
        }


        if (pmStatus.equals(MainFabConstants.pmEnd)) {
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PM_FLAG"), Bytes.toBytes(pmStatus))
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PM_TIME"), Bytes.toBytes(pmTimestamp))
        }


        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE_TRIGGER"), Bytes.toBytes(RULE_TRIGGER))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_VERSION"), Bytes.toBytes(CONTROL_PLAN_VERSION))

        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_TIME"), Bytes.toBytes(INDICATOR_TIME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_TIME))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SWITCH_STATUS"), Bytes.toBytes(SWITCH_STATUS))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("UNIT"), Bytes.toBytes(UNIT))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("AREA"), Bytes.toBytes(area))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SECTION"), Bytes.toBytes(section))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MES_CHAMBER_NAME"), Bytes.toBytes(mesChamberName))
        //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SPEC_ID"), Bytes.toBytes(SPEC_ID))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_ID"), Bytes.toBytes(CONTROL_PLAN_ID))

        mutator.mutate(put)
        //每满500条刷新一下数据
        if (count >= INDICATOR_BATCH_WRITE_HBASE_NUM){
          mutator.flush()
          count = 0
        }
        count = count + 1

      } catch {
        case ex: Exception => logger.warn(s"alarmHbaseSink elemt Exception:${ExceptionInfo.getExceptionInfo(ex)} data: $elem")
      }
    } catch {
      case ex: Exception => logger.warn(s"alarmHbaseSink  invokeIndicatorDataSite Exception:${ExceptionInfo.getExceptionInfo(ex)}")

        /**
         * 连接hbase表
         */
        startHbaseTable()
    }
  }

  /**
   *  hbase表连接
   */
  def startHbaseTable(): Unit ={
    try {
      val tableName: TableName = TableName.valueOf(ProjectConfig.HBASE_INDICATOR_DATA_TABLE)
      val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
      //设置缓存2m，当达到1m时数据会自动刷到hbase
      params.writeBufferSize(1 * 1024 * 1024) //设置缓存的大小
      mutator = connection.getBufferedMutator(params)
    }catch {
      case ex: Exception => logger.warn("startHbaseTable error: " + ex.toString)
    }
  }



  //  /**
  //   * 插入hbase
  //   *
  //   */
  //  def invokeIndicatorDataSite(alarmRuleResultList: List[JsonNode]): Unit = {
  //    val resultList: ListBuffer[Put] = new ListBuffer()
  //    try {
  //      for (elem <- alarmRuleResultList) {
  //        try {
  //
  //          val alarmRuleResult = toBean[ConfigData[AlarmRuleResult]](elem)
  //          val value = alarmRuleResult.datas
  //
  //          // rowkey查询的主要字段
  //          val TOOL_ID = value.toolName
  //          val CHAMBER_ID = value.chamberName
  //          val INDICATOR_ID = value.indicatorId
  //
  //          val indicatorSpecialID = TOOL_ID + CHAMBER_ID + INDICATOR_ID
  //
  //          val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)), 4, '0')
  //
  //          val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))
  //
  //
  //          val timestatmp = value.windowStartTime
  //          val reverseTime = Long.MaxValue - timestatmp
  //
  //          val rowkey = s"$slatKey|$indicatorSpecialMd5|$reverseTime"
  //
  //
  //          val put = new Put(Bytes.toBytes(rowkey))
  //
  //          val RUN_ID = value.runId
  //          val INDICATOR_NAME = value.indicatorName
  //          val INDICATOR_VALUE = value.indicatorValue
  //          val RUN_START_TIME = value.runStartTime
  //          val RUN_END_TIME = value.runEndTime
  //
  //          val PRODUCT_ID = if (value.productName.nonEmpty) value.productName.reduce(_ + "," + _) else ""
  //          val STAGE = if (value.productName.nonEmpty) value.stage.reduce(_ + "," + _) else ""
  //
  //          val ALARM_LEVEL = value.alarmLevel
  //          val SWITCH_STATUS = value.switchStatus
  //          val UNIT = value.unit
  //
  //
  //          val LIMIT = value.limit
  //
  //          val OCAP: String = if (value.RULE.nonEmpty) {
  //            val AlarmRuleParamList: List[AlarmRuleParam] = value.RULE.flatMap(_.alarmInfo)
  //            val actionList: List[AlarmRuleAction] = AlarmRuleParamList.flatMap(_.action)
  //            val actions = actionList.map(_.`type`).filter(x => x.nonEmpty).map(_.get).distinct
  //            actions.mkString(",")
  //          } else {
  //            ""
  //          }
  //
  //
  //          val RULE_TRIGGER = value.ruleTrigger
  //          val CONTROL_PLAN_VERSION = value.controlPlanVersion
  //          val INDICATOR_TIME = value.indicatorCreateTime
  //          val ALARM_TIME = value.alarmCreateTime
  //          val RECIPE = value.recipeName
  //          val WINDOW_START_TIME = value.windowStartTime
  //          val WINDOW_END_TIME = value.windowEndTime
  //          val CONTROL_PLAN_ID = value.controlPlnId.toString
  //
  //
  //          val area = if (value.area != null) value.area else ""
  //          val section = if (value.section != null) value.section else ""
  //          val mesChamberName = if (value.mesChamberName != null) value.mesChamberName else ""
  //          val pmStatus = value.pmStatus
  //          val pmTimestamp = value.pmTimestamp
  //
  //
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_ID"), Bytes.toBytes(RUN_ID))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TOOL_ID"), Bytes.toBytes(TOOL_ID))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_ID"), Bytes.toBytes(CHAMBER_ID))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(RECIPE))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_ID"), Bytes.toBytes(INDICATOR_ID))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_NAME"), Bytes.toBytes(INDICATOR_NAME))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_VALUE"), Bytes.toBytes(INDICATOR_VALUE))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT_ID"), Bytes.toBytes(PRODUCT_ID))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(STAGE))
  //
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_START_TIME"), Bytes.toBytes(RUN_START_TIME))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_END_TIME"), Bytes.toBytes(RUN_END_TIME))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_START_TIME"), Bytes.toBytes(WINDOW_START_TIME))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WINDOW_END_TIME"), Bytes.toBytes(WINDOW_END_TIME))
  //
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_LEVEL"), Bytes.toBytes(ALARM_LEVEL))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LIMIT"), Bytes.toBytes(LIMIT))
  //
  //
  //          if (!OCAP.equals("")) {
  //            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("OCAP"), Bytes.toBytes(OCAP))
  //          }
  //
  //
  //          if (pmStatus.equals(MainFabConstants.pmEnd)) {
  //            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PM_FLAG"), Bytes.toBytes(pmStatus))
  //            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PM_TIME"), Bytes.toBytes(pmTimestamp))
  //          }
  //
  //
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RULE_TRIGGER"), Bytes.toBytes(RULE_TRIGGER))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_VERSION"), Bytes.toBytes(CONTROL_PLAN_VERSION))
  //
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_TIME"), Bytes.toBytes(INDICATOR_TIME))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ALARM_TIME"), Bytes.toBytes(ALARM_TIME))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SWITCH_STATUS"), Bytes.toBytes(SWITCH_STATUS))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("UNIT"), Bytes.toBytes(UNIT))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("AREA"), Bytes.toBytes(area))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SECTION"), Bytes.toBytes(section))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MES_CHAMBER_NAME"), Bytes.toBytes(mesChamberName))
  //          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CONTROL_PLAN_ID"), Bytes.toBytes(CONTROL_PLAN_ID))
  //
  //          resultList.append(put)
  //        } catch {
  //          case ex: Exception => logger.warn(s"alarmHbaseSink elemt Exception:${ExceptionInfo.getExceptionInfo(ex)} data: $elem")
  //        }
  //      }
  //
  //      // hbase入库
  //      mutatorToHbase(connection, ProjectConfig.HBASE_INDICATOR_DATA_TABLE, resultList)
  //
  //    } catch {
  //      case ex: Exception => logger.warn(s"alarmHbaseSink  invokeIndicatorDataSite Exception:${ExceptionInfo.getExceptionInfo(ex)}")
  //    }finally {
  //      resultList.clear()
  //    }
  //  }

  //  //向hbase插入数据
  //  def mutatorToHbase(connect: Connection, tableName: String): Unit = {
  //    try {
  //      val params = new BufferedMutatorParams(TableName.valueOf(tableName))
  //      params.writeBufferSize(2 * 1024 * 1024)
  //
  //      //开启hbase插入异常监控
  //      val listeners = new BufferedMutator.ExceptionListener() {
  //        @throws[RetriesExhaustedWithDetailsException]
  //        override def onException(e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
  //          for (i <- 0 until e.getNumExceptions) {
  //            logger.warn("插入失败 " + e.getRow(i) + ".")
  //          }
  //        }
  //      }
  //
  //      //开启异常监听
  //      params.listener(listeners)
  //      val bufferedMutator = connect.getBufferedMutator(params)
  //
  //      val iter = resultListState.get().iterator()
  //
  //
  //      while (iter.hasNext){
  //        val one = iter.next()
  //        bufferedMutator.mutate(one)
  //      }
  //
  //      bufferedMutator.flush()
  //
  //      bufferedMutator.close()
  //      //批量提交数据插入 每满200条刷新一下数据
  //    }catch {
  //      case ex: Exception => logger.warn("mutatorToHbase error: " + ex.toString)
  //    }
  //
  //  }
}
