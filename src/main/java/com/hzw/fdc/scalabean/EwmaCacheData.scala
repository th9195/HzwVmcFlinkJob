package com.hzw.fdc.scalabean

case class EwmaCacheData(target:Option[Double],
                         ucl:Option[Double],
                         lcl:Option[Double],
                         runId:String,
                         lastIndicatorValue:Option[Double])

case class EwmaLimitJudgeResult(judgeStatus:Boolean,
                                ucl:Option[Double],
                                lcl:Option[Double],
                                errorType:String,
                                errorMessage :String)

case class EwmaRetargetJudgeResult(isRetarget:Boolean,
                                   retargetStatus:Boolean,
                                   errorType:String,
                                   errorMessage :String,
                                   cacheRunId:String,
                                   cacheTarget:Option[Double],
                                   cacheUcl:Option[Double],
                                   cacheLcl:Option[Double])











