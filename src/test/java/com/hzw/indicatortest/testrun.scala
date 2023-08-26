package com.hzw.indicatortest

import com.greenpineyu.fel.FelEngineImpl
import com.hzw.fdc.engine.api.ApiControlWindow
import com.hzw.fdc.scalabean.ErrorCode

import collection.JavaConverters._
import scala.util.matching.Regex

/**
 * @author ：gdj
 * @date ：Created in 2021/10/31 16:19
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
object testrun {
  def main(args: Array[String]): Unit = {

    val felEngine = new FelEngineImpl

    val ctx = felEngine.getContext

    ctx.set(s"AR_CLEAN_MFC_Pressure", 52.69479390331807)

    val sensorValue = felEngine.eval("$('Math').min(AR_CLEAN_MFC_Pressure,55.0)", ctx)
    println(sensorValue)

    var numberPattern: Regex = "^[A-Za-z0-9.\\-\\_\\/]+$".r
    val tagsUnit: String = numberPattern.findFirstMatchIn("") match {
      case Some(_) => ""
      case None => stringToUnicode("%^&")
      case _ => ""
    }

    val startkeyTime1 = System.currentTimeMillis()
    var windowStart = "(offset(\"null\")&&((((((STN1_Occup==0.0)&&((STN2_Occup==1.0)&&((STN4_Occup==1.0)&&(STN3_Occup==1.0))))||((STN1_Occup==1.0)&&((STN2_Occup==0.0)&&((STN4_Occup==1.0)&&(STN3_Occup==1.0)))))||((STN1_Occup==1.0)&&((STN2_Occup==1.0)&&((STN4_Occup==0.0)&&(STN3_Occup==1.0)))))||((STN1_Occup==1.0)&&((STN2_Occup==1.0)&&((STN4_Occup==1.0)&&(STN3_Occup==0.0)))))))"
    var windowEnd  = "(offset(\"Te0S\")&&(_END))"
    try{

        windowStart = windowStart.replace("}", "").replace("{", "")
        windowEnd = windowEnd.replace("}", "").replace("{", "")
        val startSensorList = ApiControlWindow.utilParseSensors(windowStart).asScala
        val endSensorList = ApiControlWindow.utilParseSensors(windowEnd).asScala

        println(startSensorList)
      println(endSensorList)


    }catch {
      case ex: Exception => println("=====")
    }

    val startkeyTime2 = System.currentTimeMillis()
    println(s"============window==11==startkeyTime2:" + (startkeyTime2 - startkeyTime1))




  }

  /**
   * String转unicode
   *
   * @param unicode
   * @return
   */
  def stringToUnicode(unicode: String): String = {
    val chars = unicode.toCharArray
    val builder = new StringBuilder

    for (i <- 0 until chars.size) {
      val c = unicode.charAt(i)

      builder.append(String.format("\\u%04x", Integer.valueOf(c)))
    }
    builder.toString()
  }

}


