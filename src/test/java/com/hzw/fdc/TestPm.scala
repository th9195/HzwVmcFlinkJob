//package com.hzw.fdc
//
//
//
///**
// * @author ：gdj
// * @date ：Created in 2021/10/16 14:59
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//object TestPm {
//  def main(args: Array[String]): Unit = {
//
////    val data="{\"dataType\":\"rawData\",\"toolName\":\"TestT-API\",\"chamberName\":\"YMTC_chamber\",\"timestamp\":1634367449993,\"traceId\":\"TestT-API_YMTC_chamber--20211016145624907\",\"data\":[{\"svid\":\"2073\",\"sensorName\":\"YMTC_chamber_Timestamp\",\"sensorValue\":66,\"sensorAlias\":\"Timestamp\",\"unit\":\"s\"},{\"svid\":\"1383\",\"sensorName\":\"YMTC_chamber_StepID\",\"sensorValue\":10,\"sensorAlias\":\"StepID\",\"unit\":\"s\"},{\"svid\":\"1834\",\"sensorName\":\"YMTC_chamber_TestAliasAPI\",\"sensorValue\":125,\"sensorAlias\":\"TestAliasAPI\",\"unit\":\"s\"}]}"
//
//    val ss=List("d","a","c","a","c","a","c")
//    val dd0=ss(0)
//    val dd1=ss(1)
//
//    println("")
//
//    // 2022 3/1
//
//
//    //  活跃/激活
//
//    // dt uid action
//
//    //with  select dt, uid, action from table partition by dt='20220301' as tmp
//
//    select  (select count(distinct uid) as fenzi from tmp  where action='activate')/
//         (select count(distinct uid)  as fenmu from tmp where action='active')
//    from (
//      select dt, uid, action from table partition by dt='20220301'
//    ) as tmp
//
//
//    // top 5%
//
//    //
//
//
//    // 10秒   10毫秒
//
//    // map 耗时总量， count
//
//
//
//    // reduce  耗时总量 / count   中位数
//
//
//
//
//
//
//
//  }
//}
