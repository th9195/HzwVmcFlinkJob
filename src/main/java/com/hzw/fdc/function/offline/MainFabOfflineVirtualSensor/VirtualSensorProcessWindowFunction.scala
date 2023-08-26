package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor

import java.text.DecimalFormat

import com.hzw.fdc.scalabean.{OfflineVirtualSensorOpentsdbResult, OpenTSDBPoint}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
 *  聚合在一起，批量写入
 */
class VirtualSensorProcessWindowFunction extends ProcessWindowFunction[OfflineVirtualSensorOpentsdbResult,
  List[OpenTSDBPoint], String, TimeWindow] with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VirtualSensorProcessWindowFunction])

  var fmt: DecimalFormat = _
  var numberPattern: Regex = "^[A-Za-z0-9.\\-\\_\\/]+$".r

  override def process(key: String, context: Context, elements: Iterable[OfflineVirtualSensorOpentsdbResult],
                       out: Collector[List[OpenTSDBPoint]]): Unit = {
    val OpenTSDBPointList: ListBuffer[OpenTSDBPoint] = ListBuffer()
    try {
      for(rawData <- elements) {
        val data = rawData.data
        val metric = s"${rawData.toolName}.${rawData.chamberName}.${rawData.data.svid}"

        val tags: mutable.Map[String, String] = mutable.Map()
        if(data.stepId.nonEmpty) {
          tags.put("stepId", data.stepId.toString)
        }
        if(data.stepName.nonEmpty) {
          val stepName = numberPattern.findFirstMatchIn(data.stepName)
          if(stepName.nonEmpty){
            tags.put("stepName", rawData.data.stepName.replace(" ", "_"))
          }else{
            logger.warn(s"${rawData.toolName}.${rawData.chamberName}.${data.svid} ${data.stepName}  " +
              s"stepName no write opentsdb reason: style")
          }
        }

        if (data.unit.nonEmpty) {
          val tagsUnit: String = numberPattern.findFirstMatchIn(data.unit) match {
            case Some(_) => data.unit
            case None => stringToUnicode(data.unit)
          }
          tags.put("unit", tagsUnit)
        }

        OpenTSDBPointList.append(OpenTSDBPoint(metric = metric,
          value = data.sensorValue,
          timestamp = rawData.timestamp,
          tags = tags.toMap))
      }
      if(OpenTSDBPointList.nonEmpty){
        out.collect(OpenTSDBPointList.toList)
      }
    }catch {
      case ex: Exception => logger.warn(s"VirtualSensorProcessWindowFunction error: ${ex.toString}")
    }


    /**
     * String转unicode
     *
     * @param unicode
     * @return
     */
    def stringToUnicode(unicode: String): String = {
      val chars = unicode.toCharArray
      val builder = new StringBuilder

      for (i <- 0 until chars.size) {
        val c = unicode.charAt(i)

        builder.append(String.format("\\u%04x", Integer.valueOf(c)))
      }
      builder.toString()
    }
  }



}
