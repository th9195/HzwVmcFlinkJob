package com.hzw.fdc.scalabean

import com.fasterxml.jackson.databind.JsonNode

/**
 *   同步策略 case class 类， 返回类型给后台
 */
case class CheckConfig(checkType: String,
                       message: JsonNode)
