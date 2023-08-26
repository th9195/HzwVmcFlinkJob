package com.hzw.fdc.function.online.MainFabAlarmHistory

;

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits.Unmarshallable
import com.hzw.fdc.scalabean.{AlarmRuleResult, AlarmStatistic, FdcData, MicAlarmData}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory};

/**
 * @author Liuwt
 * @date 2021/12/2810:24
 */
class AlarmStatisticBuildFunction extends MapFunction[JsonNode, AlarmStatistic] {

  override def map(jsonNode: JsonNode): AlarmStatistic = {
    val dataType = jsonNode.findPath("dataType").asText()
    if ("AlarmLevelRule".equals(dataType)) {
      val value = toBean[FdcData[AlarmRuleResult]](jsonNode)
      val alarm = value.datas
      if (!alarm.ruleTrigger.equals("N/A")) {
        val toolName = alarm.toolName
        val indicatorName = alarm.indicatorName
        val triggerFlag = alarm.getRULE.map(r => {
          // 触发了action
          if (r.alarmInfo.exists(i => i.action.nonEmpty)) {
            1
          } else {
            0
          }
        }).sum
        var trigger = 0
        if (triggerFlag > 0) {
          trigger = 1
        }
        AlarmStatistic(toolName, alarm.chamberName, alarm.alarmCreateTime, indicatorName, alarm.indicatorId.toString, alarm.alarmLevel, trigger, alarm.controlPlanVersion)
      } else {
        AlarmStatistic("", "", 0L, "", "0", 0, 0, 0)
      }
    } else if ("micAlarm".equals(dataType)) {
      val value = toBean[MicAlarmData](jsonNode)
      val alarm = value.datas
      val toolName = alarm.toolName
      val indicatorName = alarm.micName
      val level = 0
      // 触发了action
      val trigger = if (alarm.action.map(x => x.`type`).nonEmpty) {
        1
      } else {
        0
      }
      AlarmStatistic(toolName, alarm.chamberName, alarm.alarmCreateTime, indicatorName, "0", level, trigger, alarm.controlPlanVersion)
    } else {
      AlarmStatistic("", "", 0L, "", "0", 0, 0, 0)
    }
  }

}
