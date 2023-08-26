package com.hzw.fdc.util.hbaseUtils


import com.hzw.fdc.scalabean.{AutoLimitIndicatorResult, OfflineAutoLimitRunData}
import com.hzw.fdc.util.MainFabConstants.IS_DEBUG
import com.hzw.fdc.util.{LocalPropertiesConfig, ProjectConfig}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * HbaseUtil
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/11 14:01
 * @since 1.0.0
 * @update 2022/8/11 14:01
 * */
object HbaseUtil {

  var admin: Admin = null
  var conn: Connection = null

  private val logger: Logger = LoggerFactory.getLogger(classOf[HbaseUtilJava])



  def main(args: Array[String]): Unit = {

    IS_DEBUG = true
    initProjectConfig()

    getConnection()

//    testGetIndicatorData()
    testGetRunData
    close()
  }


  /**
   *
   * @param toolName
   * @param chamberName
   * @param startTimestamp
   * @param endTimestamp
   * @param columns
   * @param tableName
   * @return
   */
  def getRunDataFromHbaseByParams(toolName:String,
                                        chamberName:String,
                                        startTimestamp:Long,
                                        endTimestamp:Long,
                                        columns:List[String],
                                        tableName :String) = {

    val key = s"${toolName}_${chamberName}".hashCode % 10
    val startRowkey = s"${key}_${toolName}_${chamberName}_${startTimestamp}"
    val endRowkey = s"${key}_${toolName}_${chamberName}_${endTimestamp}"
//    logger.warn(s"startRowkey == ${startRowkey}")
//    logger.warn(s"endRowkey == ${endRowkey}")

    getRunData(tableName, startRowkey, endRowkey, columns)
  }


  /**
   *
   * @param tableName
   * @param startRowkey
   * @param endRowkey
   * @param columns
   * @return
   */
  def getRunData(tableName: String,startRowkey:String,endRowkey:String,columns:List[String]) = {
    // 根据rowkey 查询 数据
    val resultScanner = getDataByRowkey(tableName, startRowkey, endRowkey, columns)

    val autoLimitRunDataList = ListBuffer[OfflineAutoLimitRunData]()

//    logger.warn(s"scanner length = ${resultScanner.size}")

    if(resultScanner != null){
      try{
        resultScanner.map(row => {
          val autoLimitRunData = OfflineAutoLimitRunData()
          row.rawCells.map(cell => {

            autoLimitRunData.setRowkey(Bytes.toString(CellUtil.cloneRow(cell)))
            val columnName: String = Bytes.toString(CellUtil.cloneQualifier(cell))
            columnName match {
              case "RUN_START_TIME" => {
                autoLimitRunData.setRun_start_time(Bytes.toLong(CellUtil.cloneValue(cell)))
              }
              case "RUN_END_TIME" => {
                autoLimitRunData.setRun_end_time(Bytes.toLong(CellUtil.cloneValue(cell)))
              }
              case "RECIPE" => {
                autoLimitRunData.setRecipe(Bytes.toString(CellUtil.cloneValue(cell)))
              }
              case "PRODUCT" => {
                autoLimitRunData.setProducts(Bytes.toString(CellUtil.cloneValue(cell)))
              }
              case "STAGE" => {
                autoLimitRunData.setStages(Bytes.toString(CellUtil.cloneValue(cell)))
              }
            }
          })
          autoLimitRunDataList.append(autoLimitRunData)
        })
      }catch {
        case e:Exception => {
          e.printStackTrace()
          logger.error(s"getIndicatorData error!")
        }
      }

    }

    autoLimitRunDataList
  }



  /**
   *
   * @param toolName
   * @param chamberName
   * @param indicatorId
   * @param startTimestamp
   * @param endTimestamp
   * @param columns
   * @param tableName
   * @return
   */
  def getIndicatorDataFromHbaseByParams(toolName:String,
                                        chamberName:String,
                                        indicatorId:String,
                                        startTimestamp:Long,
                                        endTimestamp:Long,
                                        columns:List[String],
                                        tableName :String) = {

    val indicatorSpecialID = toolName + chamberName + indicatorId
    val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)), 4, '0')
    val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))

    val startReverseTime = Long.MaxValue - startTimestamp
    val endReverseTime = Long.MaxValue - endTimestamp
    val startRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + endReverseTime
    val endRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + startReverseTime
//    logger.warn(s"startRowkey == ${startRowkey}")
//    logger.warn(s"endRowkey == ${endRowkey}")
    val autoLimitIndicatorResultList = getIndicatorData(tableName, startRowkey, endRowkey, columns)

    autoLimitIndicatorResultList
  }
  /**
   *
   * @param tableName
   * @param startRowkey
   * @param endRowkey
   * @param columns
   * @return
   */
  def getIndicatorData(tableName: String,startRowkey:String,endRowkey:String,columns:List[String]) = {
    // 收集封装数据 AutoLimitIndicatorResult
    val autoLimitIndicatorResultList = ListBuffer[AutoLimitIndicatorResult]()

    // 根据rowkey 查询 数据
    val resultScanner = getDataByRowkey(tableName, startRowkey, endRowkey, columns)

    if(resultScanner != null){
      try{
        resultScanner.map(row => {
          val autoLimitIndicatorResult = AutoLimitIndicatorResult()

          row.rawCells.map(cell => {
            val columnName: String = Bytes.toString(CellUtil.cloneQualifier(cell))
            columnName match {
              case "INDICATOR_ID" => {
                autoLimitIndicatorResult.setIndicatorId(Bytes.toLong(CellUtil.cloneValue(cell)))
              }
              case "INDICATOR_VALUE" => {
                autoLimitIndicatorResult.setIndicatorValue(Bytes.toString(CellUtil.cloneValue(cell)))
              }
              case "TOOL_ID" => {
                autoLimitIndicatorResult.setToolName(Bytes.toString(CellUtil.cloneValue(cell)))
              }
              case "CHAMBER_ID" => {
                autoLimitIndicatorResult.setChamberName(Bytes.toString(CellUtil.cloneValue(cell)))
              }
              case "RECIPE" => {
                autoLimitIndicatorResult.setRecipeName(Bytes.toString(CellUtil.cloneValue(cell)))
              }
              case "PRODUCT_ID" => {
                autoLimitIndicatorResult.setProduct(Bytes.toString(CellUtil.cloneValue(cell)).split(",").toList)
              }
              case "STAGE" => {
                autoLimitIndicatorResult.setStage(Bytes.toString(CellUtil.cloneValue(cell)).split(",").toList)
              }
            }

          })

          autoLimitIndicatorResultList.append(autoLimitIndicatorResult)
        })
      }catch {
        case e:Exception => {
          e.printStackTrace()
          logger.error(s"getIndicatorData error!")
        }
      }

    }

    autoLimitIndicatorResultList
  }


  /**
   *
   * @param tableName
   * @param startRowkey
   * @param endRowkey
   * @param columns
   * @return
   */
  def getDataByRowkey(tableName: String,startRowkey:String,endRowkey:String,columns:List[String]) = {

    var scanner: ResultScanner = null
    val table = conn.getTable(TableName.valueOf(tableName))
    try{

      val scan = new Scan
      scan.setStartRow(Bytes.toBytes(startRowkey))
      scan.setStopRow(Bytes.toBytes(endRowkey))
      if(null != columns && columns.nonEmpty){
        columns.foreach(column => {
          scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes(column))
        })
      }
      scanner= table.getScanner(scan)
    }catch {
      case exception: Exception => {
        exception.printStackTrace()
        logger.error("readHbase error!")
      }
    }finally {
      table.close()
    }

    scanner
  }

  /**
   * 获取连接
   */
  def getConnection() = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)

    try {
      // todo 创建hbase的连接对象
      val es: ExecutorService = Executors.newCachedThreadPool
      conn = ConnectionFactory.createConnection(conf,es)
//      conn = ConnectionFactory.createConnection(conf)

      // todo 根据连接对象, 获取相关的管理对象: admin (跟表相关的操作)  Table(跟表数据相关的操作)
      admin = conn.getAdmin()
    } catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s" getConnection error")
      }
    }

  }

  /**
   * 关闭资源
   *
   * @throws Exception
   */
  def close(): Unit = {
    if(admin != null){
      try{
        admin.close()
      }catch {
        case e:Exception => {
          e.printStackTrace()
          logger.error(s" close admin error!")
        }
      }
    }

    if(conn != null){
      try{
        conn.close()
      }catch {
        case e:Exception => {
          e.printStackTrace()
          logger.error(s" close conn error!")
        }
      }
    }

  }


  /**
   * 初始化全局变量
   * @return
   */
  def initProjectConfig() = {

    if(IS_DEBUG){
      ProjectConfig.PROPERTIES_FILE_PATH = LocalPropertiesConfig.config.getProperty("config_path")
      ProjectConfig.initConfig()
    }

    getConnection()
  }


  /**
   * 获取RunData 测试
   */
  def testGetRunData() = {

    val tableName = "mainfab_rundata_table"
    val toolName = "tom_tool_02"
    val chamberName = "tom_chamber_01"

    val startTimestatmp = 1661826605109l
    val endTimestatmp = 1661913005109l

    val columns = List[String](
          "RUN_START_TIME",
      "RUN_END_TIME")

    val start = System.currentTimeMillis()
    val autoLimitRunDataList: ListBuffer[OfflineAutoLimitRunData] = getRunDataFromHbaseByParams(toolName, chamberName, startTimestatmp, endTimestatmp, columns, tableName)
    val length = autoLimitRunDataList.length

    val sortedRunDataList = autoLimitRunDataList.sortBy(elem => {
      elem.run_start_time
    }).reverse

    sortedRunDataList.foreach(println(_))

    println(s"length == ${length}")
    println(s"cost time == ${System.currentTimeMillis() - start}")

  }


  /**
   * 获取IndicatorData 测试
   */
  def testGetIndicatorData() = {
    val toolName = "tom_tool_01"
    val chamberName = "tom_chamber_01"
    val indicatorId = 37143l
    val indicatorSpecialID = toolName + chamberName + indicatorId
    val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)), 4, '0')
    val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))

    val startTimestatmp = 1640966400000l
    val endTimestatmp = 1660872028000l
    val startReverseTime = Long.MaxValue - startTimestatmp
    val endReverseTime = Long.MaxValue - endTimestatmp
    val startRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + endReverseTime
    val endRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + startReverseTime
    System.out.println("rowkey == 7394|99e5bc25aa63e9fa3dbba5639a16322f|9223370376670625796")
    System.out.println("startRowkey == " + startRowkey)
    System.out.println("endRowkey == " + endRowkey)

    val columns = List[String](
      "INDICATOR_ID",
      "SPEC_ID",
      "INDICATOR_VALUE",
      "TOOL_ID",
      "CHAMBER_ID",
      "RECIPE",
      "PRODUCT_ID",
      "STAGE",
      "ALARM_TIME")

    val startTime: Long = System.currentTimeMillis()
    val autoLimitIndicatorResultList = getIndicatorData("mainfab_indicatordata_table", startRowkey, endRowkey, columns)

    val endTime = System.currentTimeMillis()

  }

}
