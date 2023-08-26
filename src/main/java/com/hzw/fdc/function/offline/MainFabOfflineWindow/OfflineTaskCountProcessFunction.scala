package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * @author ：gdj
 * @date ：Created in 2021/7/29 11:04
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class OfflineTaskCountProcessFunction extends ProcessWindowFunction[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), Long, GlobalWindow] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineTaskCountProcessFunction])

  override def process(key: Long, context: Context, elements: Iterable[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData)], out: Collector[(OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData)]): Unit = {


    try {
      val results = elements.toList


        val sortResults = results.sortBy(run => run._3.runStart)

        for (oneRun <- sortResults) {
          out.collect(oneRun)
        }

    } catch {
      case e:Exception =>logger.warn(s"Offline Error OfflineTaskCountProcessFunction data :${elements.toJson}  Exception :${ExceptionInfo.getExceptionInfo(e)}")
    }

  }
}
