
package com.hzw.fdc.function.online.MainFabWindow

import com.hzw.fdc.scalabean.{WindowConfig, WindowConfigAlias, WindowConfigData}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * <p>描述</p>
 *
 * @author liuwentao
 * @date 2021/4/23 15:03
 */
object WindowOracleConfig {

  def fetch(): WindowConfig = {
    val javaOracle = InitFetchWindowConfig.fetch()
    val javaIterator = javaOracle.getDatas.iterator()

    val windows = ListBuffer[WindowConfigData]()
    while (javaIterator.hasNext) {
      val javaWindow = javaIterator.next()
      val aliasIterator = javaWindow.getSensorAlias.iterator()
      val aliases = ListBuffer[WindowConfigAlias]()
      while (aliasIterator.hasNext) {
        val javaAlias = aliasIterator.next()
        val javaIndIds = javaAlias.getIndicatorId

        val indIds = ListBuffer[Long]()
        val indIter = javaIndIds.iterator()
        while (indIter.hasNext) {
          indIds.append(indIter.next())
        }
        aliases.append(WindowConfigAlias(
          javaAlias.getSensorAliasId,
          javaAlias.getSensorAliasName,
          "svid",
          indIds.toList,
          "",
          ""
        ))
      }
      windows.append(WindowConfigData(
        javaWindow.getContextId,
        javaWindow.getControlWindowId,
        javaWindow.getParentWindowId,
        javaWindow.getControlWindowType,
        javaWindow.getTopWindows,
        javaWindow.getConfiguredIndicator,
        javaWindow.getWindowStart,
        javaWindow.getWindowEnd,
        javaWindow.getControlPlanId,
        javaWindow.getControlPlanVersion,
        javaWindow.getCalculationTrigger,
        aliases.toList
      ))
    }

    WindowConfig("runwindow",
      status = true,
      windows.toList)
  }

}
