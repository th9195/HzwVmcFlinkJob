package com.hzw.fdc.function.online.MainFabWindow


import com.hzw.fdc.scalabean.{WindowData, fdcWindowData, windowListData}
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.mutable.ListBuffer

class ParseWindowProcessFunction extends ProcessFunction[fdcWindowData, fdcWindowData] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ParseWindowProcessFunction])

  override def processElement(i: fdcWindowData, context: ProcessFunction[fdcWindowData, fdcWindowData]#Context,
                              collector: Collector[fdcWindowData]): Unit = {
    try{
          val windowData = i
          val myDatas: windowListData = windowData.datas
          val windowDataList: List[WindowData] = myDatas.windowDatasList
          val windowMap = new concurrent.TrieMap[String, ListBuffer[WindowData]]()

          for(windowData: WindowData <- windowDataList){
            if(windowMap.contains(windowData.sensorAlias)){
              val windowList: ListBuffer[WindowData] = windowMap(windowData.sensorAlias)
              windowList.append(windowData)
              windowMap.put(windowData.sensorAlias, windowList)
            }else{
              windowMap.put(windowData.sensorAlias, ListBuffer(windowData))
            }
          }
          for(window <- windowMap.values){
            val windowData = myDatas.copy(windowDatasList = window.toList, windowEndDataCreateTime=System.currentTimeMillis())
            collector.collect(fdcWindowData("fdcWindowDatas", windowData))
          }
    }catch {
      case ex: Exception => logger.error(s"根据sensorAlias 打散 error; ExceptionInfo: ${ExceptionInfo.getExceptionInfo(ex)}")
    }
  }
}
