package com.hzw.fdc.scalabean

import scala.beans.BeanProperty

/**
 * @author ：gdj
 * @date ：Created in 2021/9/29 22:18
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
case class PMData(toolName: String,
                  chamberName: String,
                  @BeanProperty var PMStatus: String,
                  timestamp: Long)
