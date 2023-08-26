package com.hzw.fdc.util

import java.io.{PrintWriter, StringWriter}

/**
 * 打印异常详细详细
 * @author ：gdj
 * @date ：Created in 2021/6/26 17:06
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
object ExceptionInfo {
  def getExceptionInfo(exception: Exception): String = {

    try {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      exception.printStackTrace(pw)
      sw.toString()
    } catch {
      case exception: Exception =>
        s"bad getErrorInfoFromException ${exception.toString}"
    }

  }
}
