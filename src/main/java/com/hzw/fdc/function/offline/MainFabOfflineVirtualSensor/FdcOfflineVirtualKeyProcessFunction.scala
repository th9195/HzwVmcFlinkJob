package com.hzw.fdc.function.offline.MainFabOfflineVirtualSensor


import com.hzw.fdc.scalabean.{ErrorCode, OfflineVirtualSensorOpentsdbResult,
  OfflineVirtualSensorOpentsdbResultValue, VirtualResult}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class FdcOfflineVirtualKeyProcessFunction extends KeyedProcessFunction[String, VirtualResult, OfflineVirtualSensorOpentsdbResult] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[FdcOfflineVirtualKeyProcessFunction])

  /**
   * 数据流
   */
  override def processElement(x: VirtualResult, context: KeyedProcessFunction[String, VirtualResult,
    OfflineVirtualSensorOpentsdbResult]#Context, collector: Collector[OfflineVirtualSensorOpentsdbResult]): Unit = {
    try{
      var resultList = mutable.ListBuffer[OfflineVirtualSensorOpentsdbResult]()
      val dps = x.dps.sortBy(t => {t.timestamp})
      dps.foreach(elem => {
        resultList.append(OfflineVirtualSensorOpentsdbResult(
          taskId=x.taskId,
          toolName=x.toolName,
          chamberName=x.chamberName,
          batchId=x.batchId,
          timestamp=elem.timestamp,
          runId=x.runId,
          lastSensorStatus = false,
          virtualConfigList=x.task.virtualConfigList,
          data= OfflineVirtualSensorOpentsdbResultValue(x.sensorAliasName, elem.sensorValue, x.sensorAliasName,
            elem.stepName, elem.stepId, elem.unit),
          errorMsg = ""
        ))
      })

      if(resultList.nonEmpty){
        val lastTime = resultList.last.copy(lastSensorStatus = true)
        resultList = resultList.dropRight(1) :+ lastTime

        for(elem <- resultList){
          collector.collect(elem)
        }
      }

    }catch {
      case ex: Exception => logger.warn(ErrorCode("002007d001C", System.currentTimeMillis(),
        Map("function" -> "FdcOfflineVirtualKeyProcessFunction"), ex.toString).toString)
    }
  }

}

