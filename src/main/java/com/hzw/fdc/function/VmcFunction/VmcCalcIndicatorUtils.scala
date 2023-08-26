package com.hzw.fdc.function.VmcFunction

import com.hzw.fdc.scalabean.sensorDataList
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcCalcIndicatorUtils
 */
object VmcCalcIndicatorUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 最大值
   * @param runId
   * @param data
   * @param n
   * @return
   */
  def MAXAlgorithm(runId:String,data: ListBuffer[sensorDataList], n: String): String = {
    try {
      val sensorValueList = for (sensorData <- data) yield {
        sensorData.sensorValue
      }
      sensorValueList.max.toString
    } catch {
      case ex: Exception => logger.error(s"MAXAlgorithm error: ${ex.printStackTrace()}\n " +
        s"runId == ${runId} ; \n" +
        s"data == ${data}" )

        "error"
    }
  }


  /**
   * 最小值
   * @param runId
   * @param data
   * @param n
   * @return
   */
  def MINAlgorithm(runId:String,data: ListBuffer[sensorDataList], n: String): String = {
    try {
      val sensorValueList = for (sensorData <- data) yield {
        sensorData.sensorValue
      }
      sensorValueList.min.toString
    } catch {
      case ex: Exception => logger.error(s"MINAlgorithm error: ${ex.printStackTrace()}\n " +
        s"runId == ${runId} ; \n" +
        s"data == ${data}" )

        "error"
    }
  }

  /**
   * 最小值
   * @param runId
   * @param data
   * @param n
   * @return
   */
  def AVGAlgorithm(runId:String,data: ListBuffer[sensorDataList], n: String): String = {
    try {
      val sensorValueList = for (sensorData <- data) yield {
        sensorData.sensorValue
      }
      val avg = sensorValueList.sum/sensorValueList.size
      avg.toString
    } catch {
      case ex: Exception => logger.error(s"AVGAlgorithm error: ${ex.printStackTrace()}\n " +
        s"runId == ${runId} ; \n" +
        s"data == ${data}" )

        "error"
    }
  }

  /**
   * 计算 STD
   * @param data
   * @return
   */
  def STDAlgorithm(runId:String,data: ListBuffer[sensorDataList]): String = {
    try {
      var sum1 = BigDecimal.valueOf(0d)
      for (n <- data) {
        sum1 += BigDecimal(n.sensorValue)
      }
      val avg = sum1 / data.size
      var sum = BigDecimal.valueOf(0d)

      for (d <- data) {

        sum += (BigDecimal(d.sensorValue) - avg).pow(2)

      }

      val variance = sum / data.size

      val STD = Math.sqrt(variance.toDouble)

      STD.toString
    } catch {
      case ex: Exception =>
        printTryCatchErrorInfo("STDAlgorithm",runId,ex,data)
        "error"
    }
  }

  /**
   * meanT
   * @param runId
   * @param dataPoints
   * @return
   */
  def meanT(runId:String,dataPoints: ListBuffer[sensorDataList]): String = {
    try {
      if (dataPoints.length < 2) throw new RuntimeException("Length Must Greater Than 2")
      var t_i = 0L
      var v_i = BigDecimal.valueOf(0d)
      var sum = BigDecimal.valueOf(0d)
      var firstTime = dataPoints.head.timestamp
      var firstValue = BigDecimal.valueOf(dataPoints.head.sensorValue)

      for(elem <- dataPoints.tail){
        t_i = elem.timestamp
        v_i = elem.sensorValue
        sum = sum + ((v_i + firstValue) * (t_i - firstTime)/1000.00) / 2

        firstTime = t_i
        firstValue = v_i
      }
      val res = sum / ((t_i - dataPoints.head.timestamp)/1000.00)
      res.toString
    }catch {
      case ex: Exception => printTryCatchErrorInfo("meanT",runId,ex,dataPoints)
        "error"
    }
  }


  def printTryCatchErrorInfo(calcType:String,runId:String,ex:Exception,data:ListBuffer[sensorDataList]) = {
    logger.error(s"${calcType} error: ${ex.printStackTrace()}\n " +
      s"runId == ${runId} ; \n" +
      s"data == ${data}" )
  }
}
