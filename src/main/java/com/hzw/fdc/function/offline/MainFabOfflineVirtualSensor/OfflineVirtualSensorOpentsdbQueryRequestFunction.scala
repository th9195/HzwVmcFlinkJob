package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{OfflineVirtualSensorOpentsdbQuery, OfflineVirtualSensorTask}
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.util

class OfflineVirtualSensorOpentsdbQueryRequestFunction extends FlatMapFunction[JsonNode, OfflineVirtualSensorOpentsdbQuery] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineVirtualSensorOpentsdbQueryRequestFunction])

  /**
   * 把离线任务以metric(tool.chamber.sensorAlias)为单位拆分 转换为opentsdb查询类
   * 一个task有100个run, 会被拆分为1个查询类
   *
   *  离线任务
   */
  override def flatMap(json: JsonNode, collector: Collector[OfflineVirtualSensorOpentsdbQuery]): Unit = {

    logger.warn(s"OfflineVirtualSensorOpentsdbQueryRequestFunction json $json")

    try {
      val offlineVirtualSensorTask = toBean[OfflineVirtualSensorTask](json)

      var runCount = 20
      try{
        val gap = (offlineVirtualSensorTask.runData.head.runEnd - offlineVirtualSensorTask.runData.head.runStart)/1000

        if(gap > 60*60*8){
          runCount = 3
        }else if(gap > 1.5 * 60 * 60){
          //1个小时的run
          runCount = 6
        }


        if(gap > 30 && gap < 310){
          runCount = 150
        }

        // 小于30秒的run
        if(gap < 30){
          runCount = 500
        }

        logger.warn(s" virtualRunCount ${offlineVirtualSensorTask.runData.head.runId} runEnd : ${offlineVirtualSensorTask.runData.head.runEnd} runStart: ${offlineVirtualSensorTask.runData.head.runStart} ${runCount}")
      }catch {
        case ex: Exception => logger.warn("runCount error : " + ex.toString)
      }

      for(virtualConfig <- offlineVirtualSensorTask.virtualConfigList) {

        for (param <- virtualConfig.param.distinct) {
          if ("SENSOR_ALIAS_ID".equals(param.paramDataType) && Option(param.paramValue).nonEmpty && Option(param.svidMap).nonEmpty) {

            offlineVirtualSensorTask.runData.sortBy(_.runStart).grouped(runCount).foreach(runList => {
              param.svidMap.foreach(elem => {

                val key = elem._1.split("\\|")
                val toolName = key.head
                val chamberName = key.last
                val svid = elem._2

                val metric = String.format("%s.%s.%s", toolName, chamberName, svid)
                val runStart = runList.head.runStart
                val runEnd = runList.last.runEnd

                val configKey = toolName + "|" + chamberName

                val resRunDataList = runList.filter(elem => {
                  val runIdSpit = elem.runId.split("--", -1)
                  val toolName = runIdSpit(0)
                  val chamberName = runIdSpit(1)
                  val key = toolName + "|" + chamberName

                  key == configKey
                })

                if(resRunDataList.nonEmpty) {
                  try {
                    val tags = new util.HashMap[String, String]()
                    tags.put("stepId", "*")
                    val qualityQuery = Map(
                      "tags" -> tags,
                      "metric" -> metric,
                      "aggregator" -> "none"
                    )
                    val query = Map(
                      "start" -> runStart.toString,
                      "end" -> runEnd.toString,
                      "msResolution" -> true,
                      "queries" -> List(qualityQuery)
                    )
                    val queryStr = query.toJson
                    val task = offlineVirtualSensorTask.copy(runData = resRunDataList)
                    collector.collect(OfflineVirtualSensorOpentsdbQuery(task, queryStr))
                  } catch {
                    case e: Exception =>
                      logger.warn(s" MainFabOfflineWindowApplication flatMap error : $e  OfflineTask ： $offlineVirtualSensorTask ")
                  }
                }
              })
            })
          } else {
            logger.warn(s" OfflineVirtualSensorOpentsdbQueryRequestFunction flatMap  null error :  OfflineTask ： $offlineVirtualSensorTask ")
          }
        }
      }
    } catch {
      case e: Exception => logger.warn(s" OfflineVirtualSensorOpentsdbQueryRequestFunction flatMap error ExceptionInfo :${ExceptionInfo.getExceptionInfo(e)}  OfflineTask ： $json")
    }


  }
}
