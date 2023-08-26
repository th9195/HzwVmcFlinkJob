package com.hzw.fdc.scalabean

import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：Created in 2021/11/20 16:40
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
case class UpDataMessage(
                          operationType: String,
                          version: String,
                          timestamp: Long
                        )


case class PiRunTool(tools: ListBuffer[String])
