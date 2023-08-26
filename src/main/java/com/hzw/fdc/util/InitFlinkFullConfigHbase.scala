package com.hzw.fdc.util

import com.hzw.fdc.function.online.MainFabAlarm.AlarmSwitchEventConfig
import com.hzw.fdc.function.online.MainFabIndicator.FdcIndicatorConfigBroadcastProcessFunction
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{AlarmRuleConfig, AutoLimitConfig, ConfigData, ContextConfig, ContextConfigData, ControlPlanConfig, ControlPlanConfig2, IndicatorConfig, MicConfig, VirtualSensorConfig, VirtualSensorConfigData, Window2Config, WindowConfigData}
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 *
 * 在4.4 节里面已经描述了，发生全量同步的时候，会动态生成一个version，
 * 并根据这个version建立对应的一组全量同步表，flink在读取的时候根据这个version就能找到正确的表并进行读取，
 * 并且，同步程序会保证在所有全量同步表数据全部同步完成后再更新flink_full_sync_time里面的starttime记录，
 * 这样就能保证flink总能读到完整的信息，并且每次读到的都是最新的同步的数据。
 *
 * 全量同步何时触发？第一种方式是定时出发，比如每周一18:00，之前与客户的沟通会议中客户也能接受。
 * 如果后续还有需要，可以提供更精细的解决方案，比如，当flink_sync_log里从上次更新到现在存在5000条新消息时，
 * 则自触发发全量同步
 *
 * 读取hbase配置信息
 */
object InitFlinkFullConfigHbase {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcIndicatorConfigBroadcastProcessFunction])

  val ColumnFamily = "f1"
  val IndicatorDataType = "indicatorconfig"
  val MicDataType = "micalarmconfig"
  val AlarmSwitchDataType = "alarmSwitchEventConfig"
  val VirtualSensorDataType = "virtualSensorConfig"
  val ContextDataType = "context"
  val AlarmConfigDataType = "alarmconfig"
  val AutoLimitDataType = "autoLimitSettings"
  val WindowDataType = "runwindow"

  val EwmaRetargetDataType = "retargetOnceTrigger"


  // ALGO_TYPE = 1 basic indicator查询
  lazy val BasicIndicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] =
    readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType, "")

  // ALGO_TYPE = 2  custom indicator查询
  lazy val CustomIndicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] =
    readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType, "")

  // ALGO_TYPE = 3  calculated indicator查询
  lazy val CalculatedIndicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] =
    readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType, "")


  // 全量indicator
  lazy val IndicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] =
    readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

  lazy val MicConfigList: ListBuffer[ConfigData[MicConfig]] =
    readHbaseAllConfig[MicConfig](ProjectConfig.HBASE_SYNC_MIC_ALARM_TABLE, MicDataType)

  lazy val AlarmSwitchConfigList: ListBuffer[ConfigData[AlarmSwitchEventConfig]] =
    readHbaseAllConfig[AlarmSwitchEventConfig](ProjectConfig.HBASE_SYNC_CHAMBER_ALARM_SWITCH_TABLE, AlarmSwitchDataType)

  lazy val VirtualSensorConfigList: ListBuffer[ConfigData[VirtualSensorConfigData]] =
    readHbaseAllConfig[VirtualSensorConfigData](ProjectConfig.HBASE_SYNC_VIRTUAL_SENSOR_TABLE, VirtualSensorDataType)

  lazy val ContextConfigList: ListBuffer[ConfigData[List[ContextConfigData]]] =
    readHbaseAllConfig[List[ContextConfigData]](ProjectConfig.HBASE_SYNC_CONTEXT_TABLE, ContextDataType)


  lazy val AlarmConfigList: ListBuffer[ConfigData[AlarmRuleConfig]] =
    readHbaseAllConfig[AlarmRuleConfig](ProjectConfig.HBASE_SYNC_RULE_TABLE, AlarmConfigDataType)

  lazy val AutoLimitConfigList: ListBuffer[ConfigData[AutoLimitConfig]] =
    readHbaseAllConfig[AutoLimitConfig](ProjectConfig.HBASE_SYNC_AUTO_LIMIT_TABLE, AutoLimitDataType)


  lazy val RunWindowConfigList: ListBuffer[ConfigData[List[WindowConfigData]]] =
    readHbaseAllConfig[List[WindowConfigData]](ProjectConfig.HBASE_SYNC_WINDOW_TABLE, WindowDataType)


  lazy val controlPlanConfigList: ListBuffer[ConfigData[List[ControlPlanConfig]]] =
    readHbaseAllConfig[List[ControlPlanConfig]](ProjectConfig.HBASE_SYNC_CONTROL_PLAN_TABLE, MainFabConstants.controlPlanConfig)

  lazy val controlPlanConfig2List: ListBuffer[ConfigData[ControlPlanConfig2]] =
    readHbaseAllConfig[ControlPlanConfig2](ProjectConfig.HBASE_SYNC_CONTROLPLAN2_TABLE, MainFabConstants.controlPlanConfig2)

  lazy val Window2ConfigList: ListBuffer[ConfigData[Window2Config]] =
    readHbaseAllConfig[Window2Config](ProjectConfig.HBASE_SYNC_WINDOW2_TABLE, MainFabConstants.window2Config)


  var StartTime: String = ""

  var Version = ""

  def creatConnectionHbase(): Connection = {
    //创建hbase连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    conf.set("hbase.client.keyvalue.maxsize", "52428800")
    ConnectionFactory.createConnection(conf)
  }


  /**
   *  读取hbase所有的配置信息
   */
  def readHbaseAllConfig[T](FullTableName: String, IncrementTableColumn: String, FullTableColumn: String="")
                           (implicit m : Manifest[T]): ListBuffer[ConfigData[T]] = {

    val connection = creatConnectionHbase()

    try {
      logger.warn(s"readHbaseAllConfig start!!!")

      // 初始化获取hbase表的 startTime 和version
      initFlinkFullSyncTime(connection)

      val tableName = FullTableName + Version
      val dataType = IncrementTableColumn

      // 获取全量表的配置
      val fullList = readFullHbase[T](connection, tableName, FullTableColumn, dataType)

      if(dataType == AlarmConfigDataType){
        logger.warn("全量表读取完毕,继续读取增量表信息...")
      }

      // 获取增量表的配置
      val incrementList = readIncrementHbase[ConfigData[T]](connection, IncrementTableColumn)

      logger.warn(s"$tableName readHbaseAllConfig end!!!  ")

      fullList ++= incrementList
    }finally {
      // 关闭连接
      connection.close()
    }
  }

  /**
   *  读取hbase 表
   */
  def readHbaseAllConfigByTable[T](tableName: String, dataType: String, FullTableColumn: String="")
                           (implicit m : Manifest[T]): ListBuffer[ConfigData[T]] = {

    val connection = creatConnectionHbase()
    try {
      logger.warn(s"readHbaseAllConfigByTable ${tableName} start!!!")

      // 获取全量表的配置
      val fullList = readFullHbase[T](connection, tableName, FullTableColumn, dataType)

      logger.warn(s"readHbaseAllConfigByTable ${tableName} end !!! size == ${fullList.size}")
      fullList
    }finally {
      // 关闭连接
      connection.close()
    }
  }


  /**
   *  读取hbase表flink_full_sync_time
   */
  def initFlinkFullSyncTime(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_FULL_SYNC_TIME_TABLE))

    /**
     *  获取starttime
     */
    val startTimeGet: Get = new Get(Bytes.toBytes("starttime"))
    val result: Result = table.get(startTimeGet)

    StartTime = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("data")))
    Version = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("version")))

    logger.warn(s"initFlinkFullSyncTime StartTime: $StartTime\t Version: $Version")
  }


  /**
   *  读取全量表的hbase配置信息
   */
  def readFullHbase[T](connection: Connection, tableName: String, column: String, dataType: String)
                      (implicit m : Manifest[T]): ListBuffer[ConfigData[T]] = {
    val result = new ListBuffer[ConfigData[T]]
    var resultScan: ResultScanner   = null

    val table: Table = connection.getTable(TableName.valueOf(tableName))

    try{
      // 全量查询
      val scan = new Scan()
      scan.setMaxResultSize(ProjectConfig.SET_MAX_RESULT_SIZE)
      scan.setCaching(ProjectConfig.SET_SCAN_CACHE_SIZE)
//      scan.setCaching(20)

      // 过滤列
      if(column != ""){
        scan.addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(column))
      }

      if(dataType == AlarmConfigDataType){
        logger.warn("开始scan全表....")
      }

      resultScan = table.getScanner(scan)
      val it = resultScan.iterator()

      if(dataType == AlarmConfigDataType){
        logger.warn("scan完成,开始遍历组装list返回...")
      }

      while (it.hasNext){
        try {
          val next: Result = it.next()
          val cells = next.listCells().asScala

          cells.foreach(kv => {
            val value = Bytes.toString(CellUtil.cloneValue(kv))
            // context 配置特殊处理, hbase表value不一样
            if(dataType == ContextDataType){
              result.append(ConfigData(dataType , "", status = true, List(value.fromJson[ContextConfigData]).toJson.fromJson[T]))
            }else{
              result.append(ConfigData(dataType , "", status = true, value.fromJson[T]))
            }
          })
        }catch {
          case e: Exception => logger.warn(s"FullHbase next: $e")
        }
      }
    }catch {
      case e: Exception => logger.warn(s"$e")
    }finally {
      if(resultScan != null) {
        resultScan.close()
      }
      table.close()

      if(dataType == AlarmConfigDataType){
        logger.warn("遍历完成,关闭连接~...")
      }
    }

    logger.warn(s"readFullHbase SIZE:" + result.size)
    result
  }

  /**
   * flink_sync_log记录的是发送给flink的每一条消息的记录。 rowkey定义为 timestamp_[4位编号]_[消息的md5]。
   * 每行有一个f1列簇，f1列簇中有一个data列，data列中存储一个完整的json对象，描述一整个发送的消息的信息。
   * data列的数据格式因消息类型不同而不同，与写入kafka里的数据内容和格式一样。
   *
   * 样例: {"dataType":"indicatorconfig","serialNo":"68f9af8b-b139-45d5-89ff-3be132af2c37-$-30d48c178b07
   * 5a607df8fdb4c4ff5ca1","status":true,"datas":{"driftStatus":true,"indicatorName":"All Time%CMPSEN003%Min","controlPlanName":"T88%T88CG%cmprec
   * ipe88%YA_88xxxynew1s","contextId":904,"missingRatio":20,"logisticStatus":false,"sensorAlias":"CMPSEN003","indicatorId":4118,"controlPlanVers
   * ion":26,"calculatedStatus":false,"CONTROL_PLAN_ID":884,"controlWindowId":1184,"algoParam":"1","algoType":"1","w2wType":"By Tool-Chamber-Reci
   * pe","algoClass":"min","controlPlanId":884}}
   *
   * 读取增量表的hbase配置信息
   */
  def readIncrementHbase[T](connection: Connection, IncrementTableColumn: String)(implicit m : Manifest[T]): ListBuffer[T] = {

    val result = new ListBuffer[T]
    var resultScan: ResultScanner   = null

    val nowTime = System.currentTimeMillis().toString

    val table: Table = connection.getTable(TableName.valueOf(ProjectConfig.HBASE_SYNC_LOG_TABLE))

    try{
      val scan = new Scan()
      scan.setMaxResultSize(ProjectConfig.SET_MAX_RESULT_SIZE)
      scan.setCaching(ProjectConfig.SET_SCAN_CACHE_SIZE)

      // 过滤rowkey
      scan
        .setStartRow(Bytes.toBytes(StartTime.toString))
        .setStopRow(Bytes.toBytes(nowTime))
        .addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(IncrementTableColumn))

      if(IncrementTableColumn == AlarmConfigDataType){
        logger.warn("开始scan增量表....")
      }


      resultScan = table.getScanner(scan)
      val it = resultScan.iterator()

      if(IncrementTableColumn == AlarmConfigDataType){
        logger.warn("增量表查询完成,开始遍历组装数据....")
      }

      while (it.hasNext){
        try {
          val next: Result = it.next()
          val v = Bytes.toString(next.value())
          val res = v.fromJson[Map[String, Any]]

          // 获取同类型的配置
          if (res.getOrElse("dataType", "") == IncrementTableColumn) {
            result.append(v.fromJson[T])
          }
        }catch {
          case e: Exception => logger.warn(s"IncrementHbase next: $e")
        }
      }
    }catch {
      case e: Exception => logger.warn(s"$e")
    }finally {
      if(resultScan != null) {
        resultScan.close()
      }
      table.close()
      if(IncrementTableColumn == AlarmConfigDataType){
        logger.warn("增量表遍历完成,关闭资源...")
      }
    }
    logger.warn(s"readIncrementHbase SIZE:" + result.size)
    result
  }
}
