package com.hzw.hbase

import com.hzw.fdc.util.SnowFlake
import com.hzw.fdc.util.hbaseUtils.HbaseUtil.getIndicatorData
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

import scala.util.control.Breaks

/**
 * HbaseTestScala
 *
 * @desc:
 * @author tobytang
 * @date 2022/9/19 14:04
 * @since 1.0.0
 * @update 2022/9/19 14:04
 * */
object HbaseTestScala {

  // java -classpath MainFabFlinkJob-v1.5dev-49f9da4-230117170105.jar com.hzw.fdc.util.hbaseUtils.HbaseUtilJava
  // scala -classpath MainFabFlinkJob-v1.5dev-49f9da4-230117170105.jar com.hzw.fdc.util.hbaseUtils.HbaseUtil

  def main(args: Array[String]): Unit = {
    val loop = new Breaks;
    loop.breakable{
      while (true){
        println(s"--------------------------请选择序号--------------------------")
        println(s"1 -- 计算mainfab_indicatordata_table 表的rawkey (start / end )")
        println(s"2 -- 计算mainfab_alarm_history_table 表的rawkey 前缀")
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
          case _ => {
            loop.break()
          }
        }
        println("\n\n")
      }
    }
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

    print("请输入你的 toolName:")
    val toolName = Console.readLine()

    print("请输入你的 chamberName:")
    val chamberName = Console.readLine()

    print("请输入你的 indicatorId:")
    val indicatorId = Console.readLine()

    print("请输入你的 startTimestamp:")
    val WINDOW_START_TIME = Console.readLine().toLong

    println("Your params is : ")
    println(s"\ttoolName = ${toolName}")
    println(s"\tchamberName = ${chamberName}")
    println(s"\tindicatorId = ${indicatorId}")
    println(s"\tWINDOW_START_TIME = ${WINDOW_START_TIME}")

    get_mainfab_indicatordata_table_Rowkey(toolName, chamberName, indicatorId, WINDOW_START_TIME)
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
