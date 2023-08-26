package com.hzw.fdc.scalabean

/**
 * @author ：gdj
 * @date ：2021/12/29 17:02
 * @param $params
 * @return $returns
 */
case class OpenTSDBPoint(metric:String,
                         value:Double,
                         timestamp:Long,
                         tags:Map[String,String])

