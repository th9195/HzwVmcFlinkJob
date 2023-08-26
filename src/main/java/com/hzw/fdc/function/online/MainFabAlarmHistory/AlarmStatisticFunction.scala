package com.hzw.fdc.function.online.MainFabAlarmHistory

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

/**
 * @author Liuwt
 * @date 2021/10/2614:44
 */
class AlarmStatisticFunction extends ProcessWindowFunction[(String, String, Long, mutable.HashMap[String, Int]), (String, String), String, TimeWindow] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmStatisticFunction])

  override def process(prefix: String, context: Context, elements: Iterable[(String, String, Long, mutable.HashMap[String, Int])], out: Collector[(String, String)]): Unit = {
    try {
      if (elements.nonEmpty) {
        val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val time = dateFormat.format(new Date())
        val statisticList = mutable.ListBuffer[(String, String)]()
        val mergeLevelMapByPrefix = new mutable.HashMap[String, mutable.HashMap[String, Int]]()
        val levelMapByPrefix = elements.groupBy(x => x._2).values
        for (x <- levelMapByPrefix) {
          val mergeLevelMap = mergeLevelMapByPrefix.getOrElse(prefix, new mutable.HashMap[String, Int]())
          for (levelMap <- x.toList.sortBy(a => a._3).map(a => a._4)) {
            for (z <- levelMap) {
              val level = z._1
              val count = z._2
              mergeLevelMap.put(level, mergeLevelMap.getOrElse(level, 0) + count)
            }
            mergeLevelMapByPrefix.put(prefix, mergeLevelMap)
          }
          statisticList.append((x.head._1 + "-" + time.substring(0, 10) + "-" + time.substring(10, 12), buildString(mergeLevelMapByPrefix)))
        }
        statisticList.filter(s => s._2.nonEmpty).foreach(s => out.collect(s))
      }
    } catch {
      case ex: Exception => logger.warn("alarm统计出现异常", ex)
    }
  }

  //("chamber;indicatorName;indicatorId;version;" -> ("1"->1,"2"->2),"chamber;indicatorName;indicatorId;version;" -> ("1"->1,"2"->2))
  private def buildString(mergeLevelMapByPrefix: mutable.HashMap[String, mutable.HashMap[String, Int]]): String = {
    if (mergeLevelMapByPrefix.isEmpty) {
      return ""
    }
    var total = 0
    //chamber;indicatorName;indicatorId;version;1:2,2:3,3:1;2|chamber;indicatorName;indicatorId;1:2,2:3,3:1;2
    val mergeLevelStringByPrefix = mergeLevelMapByPrefix.map(mergeLevelMap => {
      //chamber;indicatorName;indicatorId;version;
      val prefix = mergeLevelMap._1
      //("1"->1,"2"->2)
      val levelMap = mergeLevelMap._2
      val trigger = levelMap.remove("trigger").getOrElse(0)
      //1:2,2:3,3:1;2
      val levelString = levelMap.map(y => y._1 + ":" + y._2).reduce(_ + "," + _)
      total += levelMap.values.sum
      levelMap.put("trigger", trigger)
      //chamber;indicatorName;indicatorId;version;1:2,2:3,3:1;2
      prefix + ";" + levelString + ";" + trigger
    }).reduce(_ + "|" + _)
    //20|chamber;indicatorName;indicatorId;version;1:2,2:3,3:1;2|chamber;indicatorName;indicatorId;1:2,2:3,3:1;2
    total + "|" + mergeLevelStringByPrefix
  }

}
