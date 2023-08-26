package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.scalabean.{AutoLimitOneConfig, IndicatorResult}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector


/**
 * @author ：gdj
 * @date ：Created in 2021/6/27 0:59
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitByCountIndicatorProcessWindowFunction extends ProcessWindowFunction[ (AutoLimitOneConfig, IndicatorResult),List[(AutoLimitOneConfig, IndicatorResult)],String,GlobalWindow]{
  override def process(key: String, context: Context, elements: Iterable[(AutoLimitOneConfig, IndicatorResult)], out: Collector[List[(AutoLimitOneConfig, IndicatorResult)]]): Unit = {

    val results = elements.toList

    out.collect(results)
  }
}

