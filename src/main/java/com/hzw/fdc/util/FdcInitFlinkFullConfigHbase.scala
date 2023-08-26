package com.hzw.fdc.util

import com.hzw.fdc.function.online.MainFabIndicator.FdcIndicatorConfigBroadcastProcessFunction
import com.hzw.fdc.json.MarshallableImplicits.{Marshallable, Unmarshallable}
import com.hzw.fdc.scalabean.{ConfigData, ContextConfigData, IndicatorConfig, ControlPlanConfig, Window2Config}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
 * 读取hbase配置信息
 */
object FdcInitFlinkFullConfigHbase {
  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcIndicatorConfigBroadcastProcessFunction])

  val ColumnFamily = "f1"
  val ContextDataType = "context"
  val ControlPlanDataType = "ControlPlanConfig"
  val Window2DataType = "Window2Config"
  val IndicatorDataType = "indicatorconfig"


  lazy val LongRunControlPlanConfigList: ListBuffer[ConfigData[List[ControlPlanConfig]]] =
    readHbaseAllConfig[List[ControlPlanConfig]](FdcProjectConfig.HBASE_SYNC_LONG_RUN_CONTROL_PLAN_TABLE, ControlPlanDataType)

  lazy val LongRunWindowConfigList: ListBuffer[ConfigData[Window2Config]] =
    readHbaseAllConfig[Window2Config](FdcProjectConfig.HBASE_SYNC_LONG_RUN_WINDOW_TABLE, Window2DataType)

  lazy val LongRunIndicatorConfigList: ListBuffer[ConfigData[IndicatorConfig]] =
    readHbaseAllConfig[IndicatorConfig](FdcProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)


  var StartTime: String = ""
  var Version = ""
  def creatConnectionHbase(): Connection = {
    //创建hbase连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", FdcProjectConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", FdcProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
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
   *  读取hbase表flink_full_sync_time
   */
  def initFlinkFullSyncTime(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf(FdcProjectConfig.HBASE_FULL_SYNC_TIME_TABLE))

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
      scan.setCaching(500)

      // 过滤列
      if(column != ""){
        scan.addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(column))
      }

      resultScan = table.getScanner(scan)
      val it = resultScan.iterator()
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

    val table: Table = connection.getTable(TableName.valueOf(FdcProjectConfig.HBASE_SYNC_LOG_TABLE))

    try{
      val scan = new Scan()

      // 过滤rowkey
      scan
        .setStartRow(Bytes.toBytes(StartTime.toString))
        .setStopRow(Bytes.toBytes(nowTime))
        .addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(IncrementTableColumn))

      resultScan = table.getScanner(scan)
      val it = resultScan.iterator()
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
    }
    logger.warn(s"readIncrementHbase SIZE:" + result.size)
    result
  }
}
