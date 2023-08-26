package com.hzw.fdc.scalabean

import com.hzw.fdc.engine.alg.AlgorithmUtils.DataPoint

/**
 * AlarmEwmaCache
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/3 10:17
 * @since 1.0.0
 * @update 2022/8/3 10:17
 * */
case class RedisCache[T](dataType:String, datas:T)
case class AlarmEwmaCacheData( ewmaKey:String,
                               w2wKey:String,
                               target:Option[Double],
                               ucl:Option[Double],
                               lcl:Option[Double],
                               runId:String,
                               lastIndicatorValue:Option[Double],
                               dataVersion:String,
                               configVersion:String)


case class AdvancedIndicatorCacheData(baseIndicatorId:String,
                                      w2wKey:String,
                                      indicatorValueList:List[Double],
                                      dataVersion:String,
                                      configVersion:String)


case class AdvancedLinearFitIndicatorCacheData(baseIndicatorId:String,
                                               w2wKey:String,
                                               dataPointList:List[DataPoint],
                                               dataVersion:String,
                                               configVersion:String)



