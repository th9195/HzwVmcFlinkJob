package com.hzw.fdc.scalabean

/**
 * @author ：gdj
 * @date ：Created in 2021/5/22 2:04
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
case class OpenTSDBHttpResponse(metric: String,
                                tags: Map[String, String],
                                aggregateTags: List[String],
                                dps: Map[String, Double])

case class OpenTSDBHttpPutResponse(success: Int,
                                   failed: Int)
