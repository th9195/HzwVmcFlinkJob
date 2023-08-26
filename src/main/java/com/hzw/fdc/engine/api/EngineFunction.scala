package com.hzw.fdc.engine.api

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow
import com.hzw.fdc.scalabean.{MainFabRawData, MainFabRawDataTuple, OfflineMainFabRawData, windowRawData}
import org.apache.flink.api.common.state.ListState

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：Created in 2021/7/29 11:33
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
object EngineFunction {

  /**
   *
   */
  def buildWindow2DataFlowData(rawDataList: ListBuffer[(Long, Long, Seq[(String, Double, String)])]): IDataFlow = {

    val flow = ApiControlWindow.buildDataFlow()

    val iter = rawDataList.iterator

    while (iter.hasNext){
      val one = iter.next()
      val dp = ApiControlWindow.buildDataPacket()
      val stepId = one._1.toInt
      val timestamp = one._2
      dp.setTimestamp(timestamp)
      dp.setStepId(stepId)

      for (sensor <- one._3) {
        val s1 = ApiControlWindow.buildSensorData()
        s1.setSensorName("")
        s1.setStepId(stepId)
        s1.setSensorAlias(sensor._1)
        s1.setValue(sensor._2)
        s1.setTimestamp(timestamp)
        s1.setUnit(sensor._3)
        dp.getSensorDataMap.put(s1.getSensorAlias, s1)
      }
      flow.getDataList.add(dp)
    }

    flow
  }


  /**
   *
   * @param rawdataList
   * @return
   */
  def buildDataFlowData(rawdataList: List[windowRawData]): (IDataFlow, mutable.Set[String]) = {
    // 获取所有的SensorAlias
    val SensorAliasSet: mutable.Set[String] = mutable.Set()

    val flow = ApiControlWindow.buildDataFlow()

      for (one <- rawdataList) {
        val dp = ApiControlWindow.buildDataPacket()
        val stepId = one.stepId.toInt
        val timestamp = one.timestamp
        dp.setTimestamp(timestamp)
        dp.setStepId(stepId)

        for (sensor <- one.rawList) {
          val s1 = ApiControlWindow.buildSensorData()
          s1.setSensorName("")
          s1.setStepId(stepId)
          s1.setSensorAlias(sensor._1)
          s1.setValue(sensor._2)
          s1.setTimestamp(timestamp)
          s1.setUnit(sensor._3)
          dp.getSensorDataMap.put(s1.getSensorAlias, s1)

          SensorAliasSet.add(sensor._1)
        }
        flow.getDataList.add(dp)
      }

    (flow, SensorAliasSet)
  }

  /**
   *
   * @param rawdataList
   * @return
   */
  def buildOfflineDataFlowData(rawdataList: List[OfflineMainFabRawData]): IDataFlow = {

    val flow = ApiControlWindow.buildDataFlow()


    for (one <- rawdataList) {
      val dp = ApiControlWindow.buildDataPacket()
      val stepId = one.stepId.toInt
      dp.setTimestamp(one.timestamp)
      dp.setStepId(stepId)

      for (sensor <- one.data.distinct) {
        val s1 = ApiControlWindow.buildSensorData()
        s1.setSensorName(sensor.sensorName)
        s1.setStepId(stepId)
        s1.setSensorAlias(sensor.sensorAlias)
        s1.setValue(sensor.sensorValue)
        s1.setTimestamp(one.timestamp)
        s1.setUnit(sensor.unit)
        dp.getSensorDataMap.put(s1.getSensorAlias, s1)
      }
      flow.getDataList.add(dp)
    }
    flow
  }


}
