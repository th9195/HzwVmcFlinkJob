package com.hzw.fdc.util.hbaseUtils

import com.hzw.fdc.util.SnowFlake
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

import scala.util.control.Breaks

object HbaseGetRowKeyUtil {

  // java -classpath MainFabFlinkJob-v1.5dev-49f9da4-230117171521.jar com.hzw.fdc.util.hbaseUtils.HbaseGetRowUtil



  def main(args: Array[String]): Unit = {
    val loop = new Breaks;
    loop.breakable{
      while (true){
        println(s"--------------------------请选择序号--------------------------")
        println(s"1 -- 计算mainfab_indicatordata_table 表数据")
        println(s"2 -- 计算mainfab_alarm_history_table 表数据")
        println(s"3 -- 计算mainfab_rundata_table 表数据")
        println(s"其它字符 -- 退出")
        println(s"-------------------------------------------------------------")
        print("\n\n请输入你的 选择:")
        val choice = Console.readLine()
        choice match {
          case "1" => {
            // todo 计算mainfab_indicatordata_table 表的rawkey (start / end )
            calcMainfab_indicatordata_table_rawkey()
          }
          case "2" => {
            // todo  计算mainfab_alarm_history_table 表的rawkey 前缀
            calcMainfab_alarm_history_table_rawkey()
          }
          case "3" => {
            // todo  计算mainfab_rundata_table 表数据
            calcMainfab_mainfab_rundata_table_rowkey()
          }
          case _ => {
            loop.break()
          }
        }
        println("\n\n")
      }
    }
  }

  /**
   * 计算mainfab_rundata_table 表数据
   */
  def calcMainfab_mainfab_rundata_table_rowkey() = {
    print("请输入你的 TOOL_NAME:")
    val toolName = Console.readLine()

    print("请输入你的 CHAMBER_NAME:")
    val chamberName = Console.readLine()

    print("请输入你的 RUN_START_TIME:")
    val RUN_START_TIME = Console.readLine().toLong

    println("Your params is : ")
    println(s"\ttoolName = ${toolName}")
    println(s"\tchamberName = ${chamberName}")
    println(s"\tWINDOW_START_TIME = ${RUN_START_TIME}")

    get_mainfab_rundata_table_rowkey(toolName, chamberName, RUN_START_TIME)


  }

  /**
   * 计算mainfab_alarm_history_table 表的rawkey 前缀
   */
  def calcMainfab_alarm_history_table_rawkey() = {

    val snowFlake: SnowFlake = new SnowFlake(0, 0)


    //    val TOOL_NAME = "BNOSC02"
    //    val ALARM_CREATE_TIME = 1673878377151

    print("请输入你的 TOOL_NAME:")
    val TOOL_NAME = Console.readLine()

    print("请输入你的 ALARM_CREATE_TIME:")
    val ALARM_CREATE_TIME = Console.readLine().toLong

    val toolMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(TOOL_NAME))
    val saltKey = StringUtils.leftPad(Integer.toString(Math.abs(toolMd5.hashCode % 10000)), 4, '0')
    val timestatmp = ALARM_CREATE_TIME
    val reverseTime = Long.MaxValue - timestatmp
    val alarmNo = snowFlake.nextId().toString
    //    val rowkey = s"$saltKey|$toolMd5|$reverseTime|$alarmNo"
    val rowkeyPrefix = s"$saltKey|$toolMd5|$reverseTime|"
    println(s"rowkeyPrefix == ${rowkeyPrefix}")

    val hbaseScanFilterSql = s"""scan 'mainfab_alarm_history', FILTER => "PrefixFilter ('${rowkeyPrefix}')""""

    println(s"hbaseScanFilterSql == ${hbaseScanFilterSql}")
  }


  /**
   * 计算mainfab_indicatordata_table 表的rawkey (start / end )
   */
  def calcMainfab_indicatordata_table_rawkey() = {

    print("请输入你的 TOOL_NAME:")
    val toolName = Console.readLine()

    print("请输入你的 CHAMBER_NAME:")
    val chamberName = Console.readLine()

    print("请输入你的 INDICATOR_ID:")
    val indicatorId = Console.readLine()

    print("请输入你的 WINDOW_START_TIME:")
    val WINDOW_START_TIME = Console.readLine().toLong

    println("Your params is : ")
    println(s"\ttoolName = ${toolName}")
    println(s"\tchamberName = ${chamberName}")
    println(s"\tindicatorId = ${indicatorId}")
    println(s"\tWINDOW_START_TIME = ${WINDOW_START_TIME}")

    get_mainfab_indicatordata_table_Rowkey(toolName, chamberName, indicatorId, WINDOW_START_TIME)
  }

  def get_mainfab_rundata_table_rowkey(toolName: String, chamberName: String, RUN_START_TIME: Any) = {

    val keyPrefix = s"${toolName}_${chamberName}".hashCode % 10

    val rowkey = s"${keyPrefix}_${toolName}_${chamberName}_${RUN_START_TIME}"

    println(s"rowkey == ${rowkey}")

    val hbaseGetSql =
      s"""get 'mainfab_rundata_table' , '${rowkey}','f1:CARRIER:toString','f1:CHAMBER_NAME:toString','f1:CREATE_TIME:toLong','f1:ERROR_CODE:toInt', 'f1:LAYER:toString', 'f1:LOT_NAME:toString', 'f1:MATERIAL_NAME:toString', 'f1:MISSING_RATIO:toString', 'f1:OPERATION:toString', 'f1:PROCESS_TIME:toLong', 'f1:PRODUCT:toString', 'f1:RECIPE:toString', 'f1:ROUTE:toString', 'f1:RUN_DATA_NUM:toInt', 'f1:RUN_END_TIME:toLong', 'f1:RUN_ID:toString', 'f1:RUN_START_TIME:toLong', 'f1:STAGE:toString', 'f1:STEP:toString', 'f1:TECHNOLOGY:toString', 'f1:TOOL_NAME:toString', 'f1:TYPE:toString', 'f1:WAFER_NAME:toString' """.stripMargin
    println(s"hbase shell get sql == ${hbaseGetSql}")
  }


  /**
   * 计算mainfab_indicatordata_table 表的rawkey (start / end )
   * @param toolName
   * @param chamberName
   * @param indicatorId
   * @param startTimestamp
   * @param endTimestamp
   */
  def get_mainfab_indicatordata_table_Rowkey(toolName:String,
                                             chamberName:String,
                                             indicatorId:String,
                                             WINDOW_START_TIME:Long) = {

    val indicatorSpecialID = toolName + chamberName + indicatorId
    val slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode % 10000)), 4, '0')
    val indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID))

    val reverseTime = Long.MaxValue - WINDOW_START_TIME
    val rowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + reverseTime

    println(s"rowkey == ${rowkey}")

    val hbaseGetSql =
      s"""get 'mainfab_indicatordata_table' , '${rowkey}','f1:ALARM_LEVEL:toInt','f1:ALARM_TIME:toLong','f1:AREA:toString','f1:CHAMBER_ID:toString','f1:CONTROL_PLAN_ID:toString','f1:CONTROL_PLAN_VERSION:toInt','f1:INDICATOR_ID:toLong','f1:INDICATOR_NAME:toString','f1:INDICATOR_TIME:toLong','f1:INDICATOR_VALUE:toString','f1:LIMIT:toString','f1:MES_CHAMBER_NAME:toString','f1:PRODUCT_ID:toString','f1:RECIPE:toString','f1:RULE_TRIGGER:toString','f1:RUN_END_TIME:toLong','f1:RUN_ID:toString' ,'f1:RUN_START_TIME:toLong' ,'f1:SECTION:toString' ,'f1:STAGE:toString' ,'f1:SWITCH_STATUS:toString' ,'f1:TOOL_ID:toString' ,'f1:UNIT:toString' ,'f1:WINDOW_END_TIME:toLong' ,'f1:WINDOW_START_TIME:toLong'""".stripMargin
    println(s"hbase shell get sql == ${hbaseGetSql}")
  }

}
