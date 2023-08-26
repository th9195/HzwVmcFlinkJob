package com.hzw.fdc.scalabean

import java.sql.Timestamp


case class HiveReportData(locationId: String,
                          locationName: String,
                          moduleId: String,
                          moduleName: String,
                          toolGroupId: String,
                          toolGroupName: String,
                          controlPlnId: String,
                          controlPlanName: String,
                          toolName: String,
                          indicatorName: String,
                          runId: String,
                          chamberName: String,
                          triggerAlarmCount: Int,
                          triggerRuleCount: Int,
                          triggerOcapCount: Int,
                          windowEndTime: Timestamp,
                          createTime: String
                         )