package com.hzw.fdc.function.online.MainFabAlarmHbase

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
class AlarmStatisticFunction extends ProcessWindowFunction[AlarmStatistic, (String, String, String), String, TimeWindow] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlarmStatisticFunction])

  override def process(toolName: String, context: Context, elements: Iterable[AlarmStatistic], out: Collector[(String, String, String)]): Unit = {
    try {
      val now = System.currentTimeMillis()
      val h1 = now - 60 * 60 * 1000
      val h12 = now - 12 * 60 * 60 * 1000
      val d1 = now - 24 * 60 * 60 * 1000
      val d3 = now - 3 * 24 * 60 * 60 * 1000
      val h1s = mutable.ListBuffer[AlarmStatistic]()
      val h12s = mutable.ListBuffer[AlarmStatistic]()
      val d1s = mutable.ListBuffer[AlarmStatistic]()
      val d3s = mutable.ListBuffer[AlarmStatistic]()
      val d7s = mutable.ListBuffer[AlarmStatistic]()
      elements.foreach(a => {
        if (a.time >= h1) {
          h1s.append(a)
          h12s.append(a)
          d1s.append(a)
          d3s.append(a)
          d7s.append(a)
        } else if (a.time >= h12) {
          h12s.append(a)
          d1s.append(a)
          d3s.append(a)
          d7s.append(a)
        } else if (a.time >= d1) {
          d1s.append(a)
          d3s.append(a)
          d7s.append(a)
        } else if (a.time >= d3) {
          d3s.append(a)
          d7s.append(a)
        } else {
          d7s.append(a)
        }
      })
      val h1Total = h1s.size
      val h12Total = h12s.size
      val d1Total = d1s.size
      val d3Total = d3s.size
      val d7Total = d7s.size

      //20|chamber;indicatorName;indicatorId;1:2,2:3,3:1;2|chamber;indicatorName;indicatorId;1:2,2:3,3:1;2
      val statisticList = mutable.ListBuffer[(String, String, String)]()
      if (h1s.nonEmpty) {
        statisticList.append((toolName, h1Total + "|" + h1s.groupBy(x => x.indicatorId).flatMap(e => {
          val indicatorId = e._1
          val indicatorName = e._2.head.indicatorName
          e._2.groupBy(z => z.chamberName).map(z1 => {
            val chamberName = z1._1
            val levelString = z1._2.groupBy(y => y.level).map(l => {
              l._1.toString + ":" + l._2.size
            }).reduce(_ + "," + _)
            val triggerCount = e._2.map(y => y.trigger).sum
            chamberName + ";" + indicatorName + ";" + indicatorId + ";" + levelString + ";" + triggerCount
          })
        }).reduce(_ + "|" + _), "f1h"))
      }
      if (h12s.nonEmpty) {
        statisticList.append((toolName, h12Total + "|" + h12s.groupBy(x => x.indicatorId).flatMap(e => {
          val indicatorId = e._1
          val indicatorName = e._2.head.indicatorName
          e._2.groupBy(z => z.chamberName).map(z1 => {
            val chamberName = z1._1
            val levelString = z1._2.groupBy(y => y.level).map(l => {
              l._1.toString + ":" + l._2.size
            }).reduce(_ + "," + _)
            val triggerCount = e._2.map(y => y.trigger).sum
            chamberName + ";" + indicatorName + ";" + indicatorId + ";" + levelString + ";" + triggerCount
          })
        }).reduce(_ + "|" + _), "f12h"))
      }
      if (d1s.nonEmpty) {
        statisticList.append((toolName, d1Total + "|" + d1s.groupBy(x => x.indicatorId).flatMap(e => {
          val indicatorId = e._1
          val indicatorName = e._2.head.indicatorName
          e._2.groupBy(z => z.chamberName).map(z1 => {
            val chamberName = z1._1
            val levelString = z1._2.groupBy(y => y.level).map(l => {
              l._1.toString + ":" + l._2.size
            }).reduce(_ + "," + _)
            val triggerCount = e._2.map(y => y.trigger).sum
            chamberName + ";" + indicatorName + ";" + indicatorId + ";" + levelString + ";" + triggerCount
          })
        }).reduce(_ + "|" + _), "f1d"))
      }
      if (d3s.nonEmpty) {
        statisticList.append((toolName, d3Total + "|" + d3s.groupBy(x => x.indicatorId).flatMap(e => {
          val indicatorId = e._1
          val indicatorName = e._2.head.indicatorName
          e._2.groupBy(z => z.chamberName).map(z1 => {
            val chamberName = z1._1
            val levelString = z1._2.groupBy(y => y.level).map(l => {
              l._1.toString + ":" + l._2.size
            }).reduce(_ + "," + _)
            val triggerCount = e._2.map(y => y.trigger).sum
            chamberName + ";" + indicatorName + ";" + indicatorId + ";" + levelString + ";" + triggerCount
          })
        }).reduce(_ + "|" + _), "f3d"))
      }
      if (d7s.nonEmpty) {
        statisticList.append((toolName, d7Total + "|" + d7s.groupBy(x => x.indicatorId).flatMap(e => {
          val indicatorId = e._1
          val indicatorName = e._2.head.indicatorName
          e._2.groupBy(z => z.chamberName).map(z1 => {
            val chamberName = z1._1
            val levelString = z1._2.groupBy(y => y.level).map(l => {
              l._1.toString + ":" + l._2.size
            }).reduce(_ + "," + _)
            val triggerCount = e._2.map(y => y.trigger).sum
            chamberName + ";" + indicatorName + ";" + indicatorId + ";" + levelString + ";" + triggerCount
          })
        }).reduce(_ + "|" + _), "f7d"))
      }

      statisticList.foreach(s => out.collect(s))
    } catch {
      case ex: Exception => logger.error("alarm统计出现异常", ex)
    }
  }

}
