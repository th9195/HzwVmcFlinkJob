package com.hzw.fdc.function.online.MainFabAlarmHistory

import com.hzw.fdc.scalabean.AlarmStatistic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * @author Liuwt
 * @date 2021/10/2614:44
 */
class AlarmStatisticOneMinMergeFunction extends ProcessWindowFunction[AlarmStatistic, (String, String, Long, mutable.HashMap[String, Int]), String, TimeWindow] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmStatisticOneMinMergeFunction])

  override def process(toolName: String, context: Context, elements: Iterable[AlarmStatistic], out: Collector[(String, String, Long, mutable.HashMap[String, Int])]): Unit = {
    try {
      if (elements.nonEmpty) {
        val map = elements.groupBy(a => a.chamberName + ";" + a.indicatorName + ";" + a.indicatorId + ";" + a.version)
        for (x <- map) {
          val levelMap = new mutable.HashMap[String, Int]()
          x._2.foreach(x => {
            val level = x.level.toString
            levelMap.put(level, levelMap.getOrElse(level, 0) + 1)
            levelMap.put("trigger", levelMap.getOrElse("trigger", 0) + x.trigger)
          })
          out.collect((toolName, x._1, x._2.head.time, levelMap))
        }
      }
    } catch {
      case ex: Exception => logger.error("alarm1分钟统计出现异常", ex)
    }
  }

}
