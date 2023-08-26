package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{AutoLimitOneConfig, AutoLimitResult, Condition, ErrorCode, IndicatorResult, SpecLimit}
import org.slf4j.Logger

import scala.math._

/**
 * @author lwt
 * @date 2021/6/10 20:42
 */
object AutoLimitCalculate {

  def map(t: (AutoLimitOneConfig, List[IndicatorResult], Long), logger: Logger): AutoLimitResult = {
    val config = t._1
    val indicatorResult = t._2
    if (indicatorResult == null || indicatorResult.isEmpty) {
      logger.warn(ErrorCode("007006b001C", System.currentTimeMillis(), Map("config" -> config, "indicatorResult" -> indicatorResult), "autoLimit计算所需indicatorReuslt列表为空").toJson)
    }

    var values = conditionFilter(config.condition, indicatorResult)
    if (values.isEmpty) {
      logger.warn(ErrorCode("007006c002C", System.currentTimeMillis(), Map("config" -> config, "oneValue" -> indicatorResult.head), "conditionFilter后数据列表为空").toJson)
    }
    //是否需要移除离群点
    if ("1".equals(config.removeOutlier)) {
      values = removeOutlier(config, values, logger)
    }

    try{
      logger.warn(s"autoLimitResult: ${config.controlPlanId}-${t._3} target: ${mean(values)}")
    }catch {
      case ex: Exception => logger.warn(s"${ex.toString}")
    }

    AutoLimitResult(
      "autoLimitResult",
      config.specId,
      //      UUID.randomUUID().toString.replaceAll("-", ""),
      //暂定为 controlPlanId +窗口开始时间
      s"${config.controlPlanId}-${t._3}",
      System.currentTimeMillis(),
      calculate(config, values, logger),
      config,
      runNum = values.size)
  }

  def conditionFilter(c: Condition, l: List[IndicatorResult]): List[Double] = {
    l.filter(i => {
      val toolList = c.tool.filter(x => x.nonEmpty)
      val chamberList = c.chamber.filter(x => x.nonEmpty)
      val recipeList = c.recipe.filter(x => x.nonEmpty)
      val productList = c.product.filter(x => x.nonEmpty)
      val stageList = c.stage.filter(x => x.nonEmpty)
      if (toolList.nonEmpty && !c.tool.contains(i.toolName)) {
        false
      } else if (chamberList.nonEmpty && !c.chamber.contains(i.chamberName)) {
        false
      } else if (recipeList.nonEmpty && !c.recipe.contains(i.recipeName)) {
        false
      } else if (productList.nonEmpty && !c.product.contains(i.product.head)) {
        false
      } else if (stageList.nonEmpty && !c.stage.contains(i.stage.head)) {
        false
      } else {
        true
      }
    }).flatMap(x => x.indicatorValue.split("\\|")).map(r => r.toDouble)
  }

  def calculate(d: AutoLimitOneConfig, l: List[Double], logger: Logger): SpecLimit = {
    val limitMethod = d.limitMethod
    // 修改target成实时计算的，不是后台传的配置
    val target = mean(l)
    var usl: BigDecimal = null
    var lsl: BigDecimal = null
    var ubl: BigDecimal = null
    var lbl: BigDecimal = null
    var ucl: BigDecimal = null
    var lcl: BigDecimal = null
    if (l.nonEmpty) {
      if ("sigma".equals(limitMethod)) {
        val n = d.limitValue.toDouble
        val avg = mean(l)
        val sigma = getSigma(l, avg)
        lcl = BigDecimal((target - n * sigma * 1.0).formatted("%.16f"))
        ucl = BigDecimal((target + n * sigma * 1.0).formatted("%.16f"))
        lbl = BigDecimal((target - n * sigma * 1.5).formatted("%.16f"))
        ubl = BigDecimal((target + n * sigma * 1.5).formatted("%.16f"))
        lsl = BigDecimal((target - n * sigma * 2.0).formatted("%.16f"))
        usl = BigDecimal((target + n * sigma * 2.0).formatted("%.16f"))
      } else if ("percent".equals(limitMethod)) {
        val percentValue = d.limitValue.toDouble
        val n = if (target > 0) percentValue / 100 else percentValue / -100
        lcl = BigDecimal((target * (1 - n * 1.0)).formatted("%.16f"))
        ucl = BigDecimal((target * (1 + n * 1.0)).formatted("%.16f"))
        lbl = BigDecimal((target * (1 - n * 1.5)).formatted("%.16f"))
        ubl = BigDecimal((target * (1 + n * 1.5)).formatted("%.16f"))
        lsl = BigDecimal((target * (1 - n * 2.0)).formatted("%.16f"))
        usl = BigDecimal((target * (1 + n * 2.0)).formatted("%.16f"))
      } else if ("iqr".equalsIgnoreCase(limitMethod)) {
        val n = d.limitValue.toDouble
        val sortList = l.sortWith(_ < _)
        val qu = quartile(sortList, 3)
        val ql = quartile(sortList, 1)
        val iqr = qu - ql
        lcl = BigDecimal((ql - n * iqr * 1.0).formatted("%.16f"))
        ucl = BigDecimal((qu + n * iqr * 1.0).formatted("%.16f"))
        lbl = BigDecimal((ql - n * iqr * 1.5).formatted("%.16f"))
        ubl = BigDecimal((qu + n * iqr * 1.5).formatted("%.16f"))
        lsl = BigDecimal((ql - n * iqr * 2.0).formatted("%.16f"))
        usl = BigDecimal((qu + n * iqr * 2.0).formatted("%.16f"))
      } else {
        logger.warn(ErrorCode("007006c004C", System.currentTimeMillis(), Map("config" -> d, "limitMethod" -> limitMethod), "不支持的limitMethod").toJson)
      }
    }
    SpecLimit(usl.toString(), lsl.toString(), ubl.toString(), lbl.toString(), ucl.toString(), lcl.toString())
  }

  def quartile(array: List[Double], quart: Int): Double = {
    val n = array.size
    val p = quart / 4.0
    val pos = 1 + (n - 1) * p
    val posStr = pos.toString
    val dotIndex = posStr.indexOf(".")
    val startIndex = if (dotIndex != -1) {
      posStr.substring(0, dotIndex).toInt
    } else {
      pos.toInt
    }
    val weight = pos - startIndex
    if (startIndex >= n) return array(n - 1)
    array(startIndex - 1) * (1 - weight) + array(startIndex) * weight
  }

  /**
   * 移除离群点
   */
  def removeOutlier(d: AutoLimitOneConfig, values: List[Double], logger: Logger): List[Double] = {
    if (values.isEmpty) {
      values
    } else {
      val limitMethod = d.limitMethod
      val limitValue = d.limitValue.toDouble
      val avg = mean(values)
      var max: Double = Double.NaN
      var min: Double = Double.NaN
      if ("sigma".equals(limitMethod)) {
        //Avg±N*sigma之外的点为离群点
        val sigma = getSigma(values, avg)
        max = avg + limitValue * sigma
        min = avg - limitValue * sigma
      } else if ("percent".equals(limitMethod)) {
        //Avg（1±m%）之外的点为离群点
        max = avg * (1 + limitValue / 100)
        min = avg * (1 - limitValue / 100)
      } else if ("iqr".equalsIgnoreCase(limitMethod)) {
        val n = d.limitValue.toDouble
        val sortList = values.sortWith(_ < _)
        val qu = quartile(sortList, 3)
        val ql = quartile(sortList, 1)
        val iqr = qu - ql
        max = qu + n * iqr
        min = ql - n * iqr
      } else {
        logger.warn(ErrorCode("007006c004C", System.currentTimeMillis(), Map("config" -> d, "limitMethod" -> limitMethod), "不支持的limitMethod").toJson)
      }
      val results = values.filter(r => {
        if (!Double.NaN.equals(max) && !Double.NaN.equals(min)) {
          r >= min && r <= max
        } else {
          true
        }
      })
      if (results.isEmpty) {
        logger.warn(ErrorCode("007006c003C", System.currentTimeMillis(), Map("config" -> d, "max" -> max, "min" -> min, "oneValue" -> values.head), "移除离群点后数据列表为空").toJson)
      }
      results
    }
  }

  /**
   *
   *  求方差
   * */
  def getSigma(l: List[Double], avg: Double): Double = {
    var dVar = 0.00000

    for(aDoube <- l){
      dVar = dVar + (aDoube - avg) * (aDoube - avg)
    }
    sqrt(dVar/ l.size)
  }


//  def getSigma(l: List[Double], avg: Double): Double = {
//    val n = if (l.size - 1 == 0) {
//      1
//    } else {
//      l.size - 1
//    }
//    sqrt(l.map(a => pow(a - avg, 2)).sum / n)
//  }

  def mean(l: List[Double]): Double = {
    l.sum / l.size
  }

}
