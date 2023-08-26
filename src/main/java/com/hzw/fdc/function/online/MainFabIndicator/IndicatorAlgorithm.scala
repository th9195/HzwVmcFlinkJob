package com.hzw.fdc.function.online.MainFabIndicator


import com.hzw.fdc.scalabean.{RawDataTimeValue, sensorDataList}
import com.hzw.fdc.util.ExceptionInfo
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * @author gdj
 * @create 2020-07-02-22:40
 *
 */
object IndicatorAlgorithm {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * @param data
   * @param n
   * @return
   */
  def MAXAlgorithm(data: ListBuffer[sensorDataList], n: String): String = {

    try {
      val res = for (m <- data) yield {
        m.sensorValue.toDouble
      }
//      val sorted = res.distinct.sorted.reverse
      val sorted = res.sorted.reverse
      val int = n.toInt
      sorted(int - 1).toString
    } catch {
      case ex: Exception => logger.error("MAXAlgorithm error:" + ex + "n:" + n + "data:" + data)
//        "0"
        "error"
    }


  }

  /**
   *
   * @param data
   * @return
   */
  def AVGAlgorithm(data: ListBuffer[sensorDataList]): String = {
    try {
      var sum = 0d
      for (n <- data) {
        sum += n.sensorValue.toDouble
      }
      (BigDecimal.valueOf(sum) / BigDecimal.valueOf(data.size)).formatted("%.11f")
    } catch {
      case ex: Exception => logger.error("AVGAlgorithm error:" + ex + "data:" + data)
        "0"
    }
  }

  /**
   *
   * @param data
   * @param n
   * @return
   */
  def MINAlgorithm(data: ListBuffer[sensorDataList], n: String): String = {
    try {
      val res = for (m <- data) yield {
        m.sensorValue.toDouble
      }
//      val sorted = res.distinct.sorted
      val sorted = res.sorted
      val int = n.toInt
      if(sorted.size>= int){
        sorted(int - 1).toString
      }else{
//        "0"
        "error"
      }
    } catch {
      case ex: Exception => logger.error("MINAlgorithm error:" + ex + "n:" + n + "data:" + data)
//        "0"
        "error"
    }
  }

  /**
   *
   * @param data
   * @return
   */
  def FIRSTPOINTAlgorithm(data: ListBuffer[sensorDataList]): String = {
    try {
      val sortWindowData = data.sortWith((d1, d2) => {
        d1.timestamp.toLong < d2.timestamp.toLong
      })
      sortWindowData.head.sensorValue.toString
    } catch {
      case ex: Exception => logger.error("FIRSTPOINTAlgorithm error:" + ex + "data:" + data)
        "0"
    }
  }

  /**
   *
   * @param data
   * @param RemoveMax
   * @param RemoveMin
   * @return
   */
  def RANGEAlgorithm(data: ListBuffer[sensorDataList], RemoveMax: String, RemoveMin: String): String = {

    try {
      val res = for (m <- data) yield {
        m.sensorValue.toDouble
      }
//      val sorted = res.distinct.sorted
      val sorted = res.sorted
      val size = sorted.size
      if (RemoveMax.toBoolean) {
        sorted.remove(size - 1)
      }
      if (RemoveMin.toBoolean) {
        sorted.remove(0)
      }
      if(sorted.size<1){
        "error"
      }else{
        (BigDecimal.valueOf(sorted.last) - BigDecimal.valueOf(sorted.head)).toString
      }
    } catch {
      case ex: Exception => logger.error("RANGEAlgorithm error:" + ex + "n:" + RemoveMax + RemoveMin + "data:" + data)
        "0"
    }


  }

  /**
   *
   * @param data
   * @return
   */
  def SUMAlgorithm(data: ListBuffer[sensorDataList]): String = {
    try {
      var sum = BigDecimal.valueOf(0d)

      for (n <- data) {
        sum += BigDecimal(n.sensorValue)
      }
      sum.toString()

    } catch {
      case ex: Exception => logger.error("SUMAlgorithm error:" + ex + "data:" + data)
        "0"
    }
  }

  /**
   *
   * @param
   * @return
   */
  def TimeRangeAlgorithm(windowStart: Long ,windowEnd: Long): String = {
    try {

      ((windowEnd - windowStart) / 1000.000).toString
    } catch {
      case ex: Exception => logger.error(s"TIMERANGEAlgorithm  windowStart: $windowStart windowEnd: $windowEnd  error:${ExceptionInfo.getExceptionInfo(ex)}")
        "0"
    }

  }

  /**
   *
   * @param data
   * @return
   */
  def STDAlgorithm(data: ListBuffer[sensorDataList]): String = {
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
      case ex: Exception => logger.error("STDAlgorithm error:" + ex + "data:" + data)
        "0"
    }
  }

  /**
   * 斜率
   * @param data
   * @return
   */
  def SlopeAlgorithm(data: ListBuffer[sensorDataList]): String = {

    try {

      if (data.size < 2) {
        "0"
      } else {
        var sumX = BigDecimal.valueOf(0d)
        var averageX = BigDecimal.valueOf(0d)
        var sumY = BigDecimal.valueOf(0d)
        var averageY = BigDecimal.valueOf(0d)
        var pointX = BigDecimal.valueOf(0d)
        var pointY = BigDecimal.valueOf(0d)
        val dataSize = data.size

        for (point <- data) {
          sumX += point.timestamp /1000.000
          sumY += point.sensorValue
        }
        averageX = sumX / dataSize
        averageY = sumY / dataSize
        var tempSum1 = BigDecimal.valueOf(0d)
        var tempSum2 = BigDecimal.valueOf(0d)
        for (point <- data) {
          pointX = point.timestamp /1000.000
          pointY = point.sensorValue
          tempSum1 += (pointX - averageX) * (pointY - averageY)
          val subtract = pointX - averageX

          tempSum2 += subtract.pow(2)
        }

        val slope = tempSum1 / tempSum2

        slope.toString
      }
    } catch {
      case ex: Exception => logger.error("SlopeAlgorithm error:" + ex + "data:" + data)
        "0"
    }
  }

  /**
   * 穿过limit次数
   * @param data
   * @param x
   * @param algoparam
   * @return
   */
  def HitCountAlgorithm(data: ListBuffer[sensorDataList], x: String, algoparam: String): String = {

    try {
      val threshold = x.toDouble
      var hitCount = 0

      if(algoparam.equals("x")){

        var judgeBuffer = new ListBuffer[Integer]()
        var lastJudgeValue:Integer = null;
        for (elem <- data){
          val i = elem.sensorValue
          var dv = i - threshold;
          if(dv>0){
            if(lastJudgeValue==null){
              judgeBuffer.append(1);
              lastJudgeValue = 1
            }else{
              if(lastJudgeValue<0){
                judgeBuffer.append(1);
                lastJudgeValue = 1;
              }
            }
          }else if(dv<0){
            if(lastJudgeValue == null){
              judgeBuffer.append(-1);
              lastJudgeValue = -1
            }else{
              if(lastJudgeValue>0){
                judgeBuffer.append(-1);
                lastJudgeValue = -1
              }
            }
          }
        }

        return (judgeBuffer.length - 1).toString;

      }else{
        var greater = true

        if (algoparam.equals("<")) {
          greater = false
        }
        var countOne = false
        for (elem <- data) {
          val i = elem.sensorValue

          if (greater) {
            if (i < threshold) {
              //表示出现了比limit更小的值
              countOne = true
            } else if ( countOne && i > threshold) {
              //表示在有比limit更小值的情况下又出现了比limit更大的值
              hitCount += 1
              countOne = false
            }

          }
          else {
            if (i > threshold) {
              //表示出现了比limit更小的值
              countOne = true
            } else if ( countOne && i < threshold) {
              //表示在有比limit更小值的情况下又出现了比limit更大的值
              hitCount += 1
              countOne = false
            }

          }

        }
      }



      hitCount.toString
    } catch {
      case ex: Exception => logger.error("hitCountAlgorithm error:" + ex + "x :" + x + algoparam + "data:" + data)
        "0"
    }

  }

  /**
   *穿过limit 的时间
   * @param data
   * @param x
   * @param algoparam
   * @return
   */
  def HitTimeAlgorithm(data: ListBuffer[sensorDataList], x: String, algoparam: String): String = {

    var firstPoint: sensorDataList = null
    var secondPoint: sensorDataList = null

    try {
      val threshold = x.toDouble
      var greater = true
      var hitCount = BigDecimal.valueOf(0d)
      if (algoparam.equals("<")) {
        greater = false
      }

      /**
       *  三角形公式 上坡： b1/a1 = b2/a2
       *        /
       *      /
       *    /
       *  /_ _ _ _
       */
      def triangleMath1(): Unit ={
        val a1 = (firstPoint.sensorValue - secondPoint.sensorValue).abs
        val a2 = (secondPoint.sensorValue - threshold).abs

        val b1 = (secondPoint.timestamp - firstPoint.timestamp) / 1000.000
        hitCount += (b1*a2)/a1
      }

      /**
       *  三角形公式，下坡
       *  | \
       *  |   \
       *  |    \
       *  | _ _ \
       */
      def triangleMath2(): Unit ={
        val a1 = (firstPoint.sensorValue - secondPoint.sensorValue).abs
        val a2 = (firstPoint.sensorValue - threshold).abs

        val b1 = (secondPoint.timestamp - firstPoint.timestamp) / 1000.000
        hitCount += (b1*a2)/a1
      }

      for (d <- data) {
        secondPoint = d
        if (greater) {
          if (secondPoint.sensorValue > threshold && firstPoint != null) {
            if(firstPoint.sensorValue > threshold) {
              hitCount += (secondPoint.timestamp - firstPoint.timestamp) / 1000.000
            }else{
              // 第一个点小于x， 第二个点大于x
              triangleMath1()
            }
          }

          // 第一个点大于x， 第二个点小于x
          if(secondPoint.sensorValue < threshold && firstPoint != null) {
            if(firstPoint.sensorValue > threshold) {
              triangleMath2()
            }
          }
        } else {
          if (secondPoint.sensorValue < threshold && firstPoint != null) {

            if(firstPoint.sensorValue < threshold){
              hitCount += (secondPoint.timestamp - firstPoint.timestamp) / 1000.000
            }else{
              triangleMath2()
            }
          }

          // 第一个点小于x， 第二个点大于x
          if(secondPoint.sensorValue > threshold && firstPoint != null) {
            if(firstPoint.sensorValue < threshold) {
              triangleMath1()
            }
          }

        }

        firstPoint = secondPoint
      }

      hitCount.toString
    } catch {
      case ex: Exception => logger.error("HitTimeAlgorithm error:" + ex + "x :" + x + algoparam + "data:" + data)
        "0"
    }

  }

  /**
   *
   * @param dataPoints
   * @return
   */
  def meanT(dataPoints: ListBuffer[sensorDataList]): String = {
    try {
      if (dataPoints.length < 2) throw new RuntimeException("Length Must Greater Than 2")
      var t_i = 0L
//      var t_i_1 = 0L
      var v_i = BigDecimal.valueOf(0d)
//      var v_i_1 = 0.0
      var sum = BigDecimal.valueOf(0d)
      var firstTime = dataPoints.head.timestamp
      var firstValue = BigDecimal.valueOf(dataPoints.head.sensorValue)

      for(elem <- dataPoints.tail){

        t_i = elem.timestamp
//        t_i_1 = dataPoints(i - 1).timestamp
        v_i = elem.sensorValue
//        v_i_1 = dataPoints(i - 1).sensorValue

        sum = sum + ((v_i + firstValue) * (t_i - firstTime)/1000.00) / 2

        firstTime = t_i
        firstValue = v_i
      }
      //now t_i is the last points' time, and v_i is the last points' value;
      val res = sum / ((t_i - dataPoints.head.timestamp)/1000.00)
      res.toString
    }catch {
      case ex: Exception => logger.error("meanT error:" + ex)
        "error"
    }
  }


  /**
   *
   * @param dataPoints
   * @return
   */
  def stdDevT(dataPoints: ListBuffer[sensorDataList], runId: String="", controlPlanName:String="",
              indicatorName:String=""): String = {
    try {
      if (dataPoints.length < 2) throw new RuntimeException("Length Must Greater Than 2")

      var part1 = BigDecimal.valueOf(0d)
      var part2 = BigDecimal.valueOf(0d)
      var dt = BigDecimal.valueOf(0d)
      var part3 = BigDecimal.valueOf(0d)

      {
        var t_i = 0L
//        var t_i_1 = 0L
        var v_i = BigDecimal.valueOf(0d)
//        var v_i_1 = 0.0
        var sum = BigDecimal.valueOf(0d)
        var sum1 = BigDecimal.valueOf(0d)
        var firstTime = dataPoints.head.timestamp
        var firstValue = BigDecimal.valueOf(dataPoints.head.sensorValue)

        for (elem <- dataPoints.tail) {
          t_i = elem.timestamp
          v_i = elem.sensorValue
          sum = sum + ((v_i + firstValue) * ((t_i - firstTime) / 1000.00)) / 2

          sum1 = sum1 + ((v_i * v_i + firstValue * firstValue) * ((t_i - firstTime)/1000.00)) / 2

          firstTime = t_i
          firstValue = v_i
        }


        dt = (t_i - dataPoints.head.timestamp) / 1000.00
        part1 = (sum * sum) / dt

        part2 = sum1
      }


//      {
//        var t_i = 0L
//        var t_i_1 = 0L
//        var v_i = 0.0
//        var v_i_1 = 0.0
//        var sum = 0.0
//
//        for (i <- 1 until dataPoints.length) {
//          t_i = dataPoints(i).timestamp
//          t_i_1 = dataPoints(i - 1).timestamp
//          v_i = dataPoints(i).sensorValue
//          v_i_1 = dataPoints(i - 1).sensorValue
//          sum = sum + ((v_i * v_i + v_i_1 * v_i_1) * ((t_i - t_i_1)/1000.00)) / 2
//        }
//        part2 = sum
//      }

      part3 = (part2 - part1) / dt
      var res = 0.0
      if(part3 > 0.0){
         res = Math.sqrt(part3.toDouble)
      }else{
        if(part3 < 0) {
          logger.warn(s"stdDevT Log runId:$runId \t controlPlanName:$controlPlanName\t indicatorName:$indicatorName\t "+
            s" dataPoints :$dataPoints \t part3:$part3 part1: $part1 part2: $part2 firstTime:" +
            s"${dataPoints.head.timestamp} firstValue: ${BigDecimal.valueOf(dataPoints.head.sensorValue)}")
        }
        res = 0
      }

      res.toString
    }catch {
      case ex: Exception => logger.error("stdDevT error:" + ex)
        "error"
    }
  }

  /**
   *
   * @param dataPoints
   * @return area
   */
  def area(dataPoints: ListBuffer[sensorDataList]) : String ={
    try{
      if (dataPoints == null) throw new Exception("wow=>[area算法参数异常][传入的参数为null]")
      if (dataPoints.length < 2) throw new Exception("wow=>[area算法参数异常][传入的数据点的数量小于2][length:" + dataPoints.length + "]")
      var dt    : Double = 0
      var dv    : Double = 0
      var dArea : Double = 0
      var area  : Double = 0
      var firstTime = dataPoints.head.timestamp
      var firstValue = dataPoints.head.sensorValue

      for (elem <- dataPoints.tail) {
        dt = (elem.timestamp - firstTime) / 1000.0
        dv = Math.abs(elem.sensorValue) + Math.abs(firstValue)
        dArea = (dv * dt) / 2
        area = area + dArea

        firstTime = elem.timestamp
        firstValue = elem.sensorValue
      }
      area.toString
    }catch {
      case ex: Exception => logger.warn(s"area error:${ex.getMessage}")
        "error"
    }
  }

}

