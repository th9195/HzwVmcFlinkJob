package com.hzw.fdc.scalabean

/**
 * MainFabLogInfo
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/28 10:34
 * @since 1.0.0
 * @update 2022/11/28 10:34
 * */
case class MainFabLogInfo(mainFabDebugCode:String,    // 错误码
                    jobName:String,                   // Job Name
                    optionName:String,                // 算子name
                    functionName:String,              // function Name
                    logTimestamp:Long,                // 时间戳
                    logTime:String,                   // 时间戳格式化 yyyy-MM-dd hh:mm:ss
                    message:String,                   // 想打印信息主题
                    paramsInfo:Map[String,Any],       // 其它额外信息
                    dataInfo:String,                  // 数据信息
                    exception:String)                 // 抛出的异常信息



case class MainFabDebugInfo(logTime:String,                   // 时间戳格式化 yyyy-MM-dd hh:mm:ss
                            paramsInfo:Map[String,Any])       // 其它额外信息
