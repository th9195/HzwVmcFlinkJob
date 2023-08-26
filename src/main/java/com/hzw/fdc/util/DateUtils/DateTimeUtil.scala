package com.hzw.fdc.util.DateUtils

import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.Date

/**
 * DateTimeUtils
 *
 * @desc:
 * @author tobytang
 * @date 2021/12/17 11:05
 * @since 1.0.0
 * @update 2021/12/17 11:05
 * */
object DateTimeUtil {


  final val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val FMT_MONTH = DateTimeFormatter.ofPattern("yyyy-MM")
  private val FMT_DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val FMT_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val FMT_DATEHOUR = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00")
  private val FMT_DATEHOUR59 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:59:59")
  private val LOCAL_ZONE = ZoneId.of("Asia/Shanghai")

  def main(args: Array[String]): Unit = {
//    println(getYesterday)
//    println(getToday)
//    println(getNextDay("2021-12-17"))
//    println(getDiffDay("2021-12-17", 15))
//
//    println(getWeekTime(1))
//    println(getWeekStartTime(2))
//
//    println(getThisYear)
//    println("getMonth: " + getMonthTime(3))
//    println("getMonths: " + getMonthTimes(3))
//    println("getDayTimes: " + getDayTimes(3)) //1640534400

    println(getDiff("2022-01-01 00:22:30", "2022-02-10 00:00:00", "day"))

  }

  def getThisYear: String = {
    val date = LocalDate.now()
    date.getYear.toString
  }

  /**
   * 获取昨天的日期
   * 格式：yyyy-MM-dd
   *
   * @return 昨天的日期
   */
  def getYesterday: String = {
    getDayTime(-1)
  }

  /**
   * 获取今天的日期
   * 格式：yyyy-MM-dd
   *
   * @return 今天的日期
   */
  def getToday: String = {
    getDayTime(0)
  }

  /**
   * 获取当前时间 yyyy-MM-dd HH:mm:ss
   */
  def getCurrentTime(format:String = "yyyy-MM-dd HH:mm:ss"):String = {
    val currentTime = new SimpleDateFormat(format).format(new Date)
    currentTime
  }

  /**
   * 获取当前时间的时间戳
   *
   * @return
   */
  def getCurrentTimestamp: Long = {
    System.currentTimeMillis()
  }

  /**
   * 根据输入的日期获取下一天的日期
   * 格式：yyyy-MM-dd
   *
   * @param part_day 输入日期
   * @return 下一天日期
   */
  def getNextDay(part_day: String): String = {
    getDiffDay(part_day, 1)
  }

  /**
   * 根据输入的日期和间隔天数获取指定日期下间隔天数后的日期
   * 格式：yyyy-MM-dd
   *
   * @param part_day 输入日期
   * @param diff     间隔天数
   * @return yyyy-MM-dd
   */
  def getDiffDay(part_day: String, diff: Int): String = {
    val date = LocalDate.parse(part_day)
    date.plusDays(diff).toString
  }


  /**
   *
   * @param stimer
   * @param etimer
   * @return
   */
  def getDiff(stimer: String, etimer: String, unit: String = "hour"): Long = {
    val stimestamp: Long = getTimestampByTime(stimer)
    val etimestamp: Long = getTimestampByTime(etimer)
    if (stimestamp >= etimestamp) {
      log.info(s"getDiff error stimer == ${stimer}, etimer == ${etimer} , unit == ${unit}")
      0
    } else {
      unit.toLowerCase match {
        case "second" => (etimestamp - stimestamp)
        case "minute" => (etimestamp - stimestamp) / 60
        case "hour" => (etimestamp - stimestamp) / 60 / 60
        case "day" => (etimestamp - stimestamp)  / 60 / 60 / 24
        case _ => {
          log.info(s"getDiff error stimer == ${stimer}, etimer == ${etimer} , unit == ${unit}")
          0
        }
      }

    }
  }

  /**
   * 获取距离当前周指定周数的某一周的日期范围
   *
   * @param week 间隔周数
   * @return 格式为"yyyyMMdd-yyyyMMdd"的日期范围
   */
  def getWeekTime(week: Int): (String, String) = {
    val date = LocalDate.now()
    val weekDate = date.plusWeeks(week)
    val wd = weekDate.getDayOfWeek.getValue
    val weekStart = weekDate.minusDays(wd - 1)
    val weekEnd = weekDate.plusDays(7 - wd)
    // 取得N周前的日期范围
    (weekStart.toString, weekEnd.toString)
  }

  /**
   * 获取距离当前周指定周数的某一周的周一的日期
   *
   * @param week 间隔周数
   * @return 格式为"yyyy-MM-dd"的日期
   */
  def getWeekStartTime(week: Int): String = {
    val date = LocalDate.now()
    val weekDate = date.plusWeeks(week)
    val wd = weekDate.getDayOfWeek.getValue
    val weekStart = weekDate.minusDays(wd - 1)
    weekStart.toString
  }

  /**
   * 获取距离当前日期指定天数的日期
   *
   * @param day 间隔天数
   * @return 格式为"yyyy-MM-dd"的日期
   */
  def getDayTime(day: Int): String = {
    val localDate = LocalDate.now()
    localDate.plusDays(day).toString
  }


  /**
   * 获取距离当前日期指定月数的日期
   *
   * @param month 间隔月数
   * @return 格式为"yyyy-MM"的日期
   */
  def getMonthTime(month: Int): String = {
    val date = LocalDate.now()
    date.plusMonths(month).format(FMT_MONTH)
  }

  /**
   * 获取距离当前日期所属月份指定月数的月份的unix时间戳
   *
   * @param month 间隔月数
   * @return unix时间戳 秒
   */
  def getMonthTimes(month: Int): Long = {
    val monthDay = getMonthTime(month)
    val date = LocalDate.parse(s"$monthDay-01", FMT_DAY)
    date.atStartOfDay(LOCAL_ZONE).toEpochSecond
  }

  /**
   * 获取距离当前日期指定天数的日期的unix时间戳
   *
   * @param day 间隔天数
   * @return unix时间戳 秒
   */
  def getDayTimes(day: Int): Long = {
    val date = LocalDate.parse(getDayTime(day))
    date.atStartOfDay(LOCAL_ZONE).toEpochSecond
  }

  /**
   * 日期字符串转为unix时间戳 毫秒值
   *
   * @param date 日期字符串
   * @param fmt  格式化
   * @return
   */
  def getDateFormatTimes(date: String, fmt: String): Long = {
    val dtf = DateTimeFormatter.ofPattern(fmt)
    LocalDate.parse(date, dtf).atStartOfDay(LOCAL_ZONE).toInstant.toEpochMilli
  }

  /**
   *
   * @param hour
   * @return
   */
  def getHour(hour: Int): String = {
    val date = LocalDateTime.now()
    date.plusHours(hour).format(FMT_DATEHOUR)
  }

  /**
   * 根据时间戳 获取 对应格式的时间
   * 秒
   *
   * @param timestamp
   * @return
   */
  def getTimeByTimestamp10(timestamp: Long, time_format: String): String = {
    val fmt = DateTimeFormatter.ofPattern(time_format)
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp * 1000), LOCAL_ZONE).format(fmt)
  }

  /**
   * 根据时间戳 获取 对应格式的时间
   * 毫秒
   *
   * @param timestamp
   * @return
   */
  def getTimeByTimestamp13(timestamp: Long, time_format: String): String = {
    val fmt = DateTimeFormatter.ofPattern(time_format)
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), LOCAL_ZONE).format(fmt)
  }


  /**
   * 根据输入的时间格式 生成想要的时间格式
   *
   * @param target_format
   * @param source_format
   * @param sourceDateTime
   * @return targetDateTime
   */
  def getDateByTime(sourceDateTime: String, target_format: String = "yyyy-MM-dd", source_format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val date = LocalDateTime.parse(sourceDateTime, DateTimeFormatter.ofPattern(source_format))
    date.format(DateTimeFormatter.ofPattern(target_format))
  }


  /**
   * 根据输入时间 获取时间戳
   *
   * @param source_format
   * @param sourceDateTime
   * @return
   */
  def getTimestampByTime(sourceDateTime: String,source_format: String = "yyyy-MM-dd HH:mm:ss",unit:String = "s"): Long = {
    val sourcefm = new SimpleDateFormat(source_format)
    if(!StringUtils.isEmpty(sourceDateTime)){
      val date: Date = sourcefm.parse(sourceDateTime)
      val timestamp = date.getTime
      unit match {
        case "ms" =>timestamp
        case _=>timestamp/1000
      }

    }else{
      log.info(s"sourceDateTime == ${sourceDateTime}")
      println(s"sourceDateTime == ${sourceDateTime}")
      0
    }
  }


  /**
   * 获取当天累加秒数
   * @param sourceDateTime
   * @param source_format
   * @return
   */
  def get_sec_in_day(sourceDateTime: String,source_format: String = "yyyy-MM-dd HH:mm:ss") = {
    val sourcefm = new SimpleDateFormat(source_format)
    val date: Date = sourcefm.parse(sourceDateTime)
    val hours = date.getHours
    val minutes = date.getMinutes
    val seconds = date.getSeconds
    seconds + minutes*60 + hours*60*60
  }

  /**
   * 获取当天累加秒数
   * @param sourceDateTime
   * @param source_format
   * @return
   */
  def get_sec_in_day_bytimestamp10(timestamp:Long) = {
    val dateTime = getTimeByTimestamp10(timestamp, "yyyy-MM-dd HH:mm:ss")
    get_sec_in_day(dateTime)
  }



}
