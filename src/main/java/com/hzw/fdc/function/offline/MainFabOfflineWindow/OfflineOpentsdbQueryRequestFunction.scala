package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util

import org.apache.flink.streaming.api.functions.KeyedProcessFunction


class OfflineOpentsdbQueryRequestFunction extends KeyedProcessFunction[String, JsonNode, OfflineOpentsdbQuery] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineOpentsdbQueryRequestFunction])

  /**
   * 把离线任务以metric(tool.chamber.sensorAlias)为单位拆分 转换为opentsdb查询类
   * 一个task有100个run, 会被拆分为1个查询类
   *
   *   离线任务
   */
  override def processElement(value: JsonNode, ctx: KeyedProcessFunction[String, JsonNode, OfflineOpentsdbQuery]#Context,
                              out: Collector[OfflineOpentsdbQuery]): Unit = {
    try {
      val task = toBean[OfflineTask](value)

      //递归获取 Tree 中的sensorList
      val sensorList = getSensorList(task.indicatorTree).distinct
      logger.warn(s"taskId: ${task.taskId} sensorList: ${sensorList}")


      if (sensorList.size != 0) {

        var runCount = 50
        try{
          val gap = (task.runData.head.runEnd - task.runData.head.runStart)/1000

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
          logger.warn(s"${task.runData.head.runId} runEnd : ${task.runData.head.runEnd} runStart: ${task.runData.head.runStart} ${runCount}")
        }catch {
          case ex: Exception => logger.warn("runCount error : " + ex.toString)
        }

        val toolList = task.runData.sortBy(_.runStart).groupBy(elem => {
          val runIdSpit = elem.runId.split("--", -1)
          val toolName = runIdSpit(0)
          val chamberName = runIdSpit(1)
          toolName + "|" + chamberName
        })

        for(elem <- toolList) {
          elem._2.grouped(runCount).foreach(runList => {
            val start = runList.head.runStart
            val end = runList.last.runEnd

            for (sensor <- sensorList) {

              val tags = new util.HashMap[String, String]()
              tags.put("stepId", "*")
              val qualityQuery = Map(
                "tags" -> tags,
                "metric" -> String.format("%s.%s.%s", sensor.toolName, sensor.chamberName, sensor.svid),
                "aggregator" -> "none"
              )
              val query = Map(
                "start" -> start.toString,
                "end" -> end.toString,
                "msResolution" -> true,
                "queries" -> List(qualityQuery)
              )
              val configKey = sensor.toolName + "|" + sensor.chamberName

              val queryStr = query.toJson
              val resRunData = runList.filter(elem => {
                val runIdSpit = elem.runId.split("--", -1)
                val toolName = runIdSpit(0)
                val chamberName = runIdSpit(1)
                val key = toolName + "|" + chamberName

                key == configKey
              })

              if (resRunData.nonEmpty) {

                val offlineOpentsdbQuery = OfflineOpentsdbQuery(task =
                  task.copy(dataType = task.dataType, taskId = task.taskId, runData = resRunData, indicatorTree = task.indicatorTree,
                    batchId = task.batchId),
                  toolName = sensor.toolName,
                  chamberName = sensor.chamberName,
                  sensorAlias = sensor.sensorAliasName,
                  sensorList = sensorList,
                  svid = sensor.svid,
                  queryStr = queryStr)

                out.collect(offlineOpentsdbQuery)
              }

            }
          })
        }
      } else {
        logger.warn(s"sensorList is null : ${value.toJson}")
      }
    } catch {
      case e: Exception => logger.warn(s"OfflineOpentsdbQueryRequestFunction error : ${ExceptionInfo.getExceptionInfo(e)}")
    }


  }

  /**
   * 获取 indicatorNode的 sensor list
   *
   */
  def getSensorList(indicatorTree:IndicatorTree): List[WindowConfigAlias] ={

    if(indicatorTree.nexts.nonEmpty) {
      val list2=for (elem <- indicatorTree.nexts) yield {

        if(indicatorTree.current.windowTree.nonEmpty){
          getSensorList(elem) ++ getWindowTreeSensorList(indicatorTree.current.windowTree.get)
        }else{
          getSensorList(elem)
        }

      }

     val list= list2.reduceLeft(_ ++ _)
      list
    }else{
      if(indicatorTree.current.windowTree.nonEmpty){
        getWindowTreeSensorList(indicatorTree.current.windowTree.get)
      }else Nil
    }

  }

  /**
   * 遍历 window Tree的 sensor list 并且拼接
   *
   * @param windowNode
   * @return
   */
  def getWindowTreeSensorList(windowNode: WindowTree): List[WindowConfigAlias] = {


    if (windowNode.next.nonEmpty) {
      //有下一个节点，递归调用并且拼接
      windowNode.current.sensorAlias ++ getWindowTreeSensorList(windowNode.next.get)
    } else {
      //没有下一个节点，返回
      windowNode.current.sensorAlias
    }

  }

}
