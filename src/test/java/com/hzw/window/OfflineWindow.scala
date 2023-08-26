//package com.hzw.window
//
//import com.hzw.fdc.engine.api.{ApiControlWindow, EngineFunction, IControlWindow}
//import com.hzw.fdc.json.MarshallableImplicits.Marshallable
//import com.hzw.fdc.scalabean._
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.concurrent
//
///**
// * @author ：gdj
// * @date ：Created in 2021/11/16 10:45
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//class OfflineWindow {
//
//  private val logger: Logger = LoggerFactory.getLogger(classOf[OfflineWindow])
//
//  def run(input: (OfflineTask, List[OfflineMainFabRawData])): Unit = {
//    val offlineTask = input._1
//    val rawData = input._2
//
//
//    val indicatorConfigs = getIndicatorConfigList(offlineTask.indicatorTree)
//
//
//
//    for (oneIndicatorConfig <- indicatorConfigs) {
//
//      if(oneIndicatorConfig.windowTree.nonEmpty){
//        val iControlWindow = createContainerWindowTree(oneIndicatorConfig.windowTree.get,
//          Option(
//            (oneIndicatorConfig.indicatorConfig.indicatorId,
//              oneIndicatorConfig.indicatorConfig.sensorAlias)
//          ))
//
//        val currentWindowConfig = oneIndicatorConfig.windowTree.get.current
//
//        val dataflow = EngineFunction.buildOfflineDataFlowData(rawData)
//
//        iControlWindow.attachDataFlow(dataflow)
//
//
//        val results = ApiControlWindow.calculate(iControlWindow).toList
//        val dataList = results.head.sensorDataList
//        val windowDatasList = results.map(x => OfflineWindowData(sensorName = x.sensorName,
//          sensorAlias = x.sensorAlias,
//          unit = x.unit,
//          indicatorId = x.indicatorId,
//          cycleUnitCount = x.cycleUnitCount,
//          cycleIndex = x.cycleIndex,
//          isCalculationSuccessful = true,
//          calculationInfo = "",
//          sensorDataList = x.sensorDataList.map(y => sensorDataList(
//            sensorValue = y.sensorValue,
//            timestamp = y.timestamp,
//            step = y.step))))
//
//        val   offlineIndicatorConfigList=   indicatorConfigs.map(x=>x.indicatorConfig)
//
//        val headData= rawData.head
//
//
//        val offlineWindowListData=  OfflineWindowListData(taskId = offlineTask.taskId,
//          toolName = headData.toolName,
//          chamberName = headData.chamberName,
//          runId = headData.runId,
//          dataMissingRatio = headData.dataMissingRatio,
//          controlWindowId = currentWindowConfig.controlWindowId,
//          windowStart = currentWindowConfig.windowStart,
//          windowStartTime = dataList.head.timestamp,
//          windowEnd = currentWindowConfig.windowEnd,
//          windowEndTime = dataList.last.timestamp,
//          windowTimeRange = dataList.last.timestamp-dataList.head.timestamp,
//          runStartTime = headData.runStart,
//          runEndTime = headData.runEnd,
//          windowEndDataCreateTime = System.currentTimeMillis(),
//          windowDatasList = windowDatasList,
//          offlineIndicatorConfigList = offlineIndicatorConfigList)
//
//        println(s"${offlineWindowListData.toJson}")
//      }
//
//
//
//    }
//  }
//
//
//  /**
//   * 获取 indicatorNode的 sensor list
//   * @param indicatorNode
//   * @return
//   */
//  def getIndicatorConfigList(indicatorTree:IndicatorTree): List[TreeConfig] ={
//
//    if(indicatorTree.nexts.nonEmpty) {
//
//      val list2=for (elem <- indicatorTree.nexts) yield {
//        getIndicatorConfigList(elem) :+ indicatorTree.current
//      }
//
//      val list= list2.reduceLeft(_ ++ _)
//      list
//    }else{
//      //      if(indicatorTree.current.windowTree.nonEmpty){
//      //        List(indicatorTree.current)
//      //      }
//      List(indicatorTree.current)
//    }
//
//  }
//
//
//  /**
//   * 构建windows树
//   *
//   * @param config
//   * @param map
//   * @return
//   */
//  def createContainerWindowTree(windowTree: WindowTree,indicatorList:Option[(Long,String)]): IControlWindow = {
//
//    //构建父window ICW
//    var iCW: IControlWindow = null
//    iCW= ApiControlWindow.parse(windowTree.current.controlWindowType,
//      windowTree.current.windowStart,
//      windowTree.current.windowEnd)
//    iCW.setWindowId( windowTree.current.controlWindowId)
//    for (oneAlias <- windowTree.current.sensorAlias) {
//
//      for (elem <- oneAlias.indicatorId if indicatorList.nonEmpty && indicatorList.get._1.equals(elem)
////        && indicatorList.get._2.equals(oneAlias.sensorAliasName)
//           ) {
//
//        iCW.addIndicatorConfig(elem,oneAlias.sensorAliasId,indicatorList.get._2)
//
//      }
//
//    }
//
//    //递归 添加 子window
//    if(windowTree.next.nonEmpty){
//
//      iCW.addSubWindow(createContainerWindowTree(windowTree.next.get,None))
//
//    }
//
//    iCW
//
//  }
//
//
//
//
//
//
//  /**
//   * 构建windows树
//   *
//   * @param config
//   * @param map
//   * @return
//   */
//  def createContainerWindow(config: WindowConfigData, map: concurrent.TrieMap[Long, WindowConfigData]): (WindowConfigData, IControlWindow) = {
//    var window: IControlWindow = null
//
//
//    window = ApiControlWindow.parse(config.controlWindowType, config.windowStart, config.windowEnd)
//
//    val aliases = config.sensorAlias.filter(_.sensorAliasName != null)
//
//    if(aliases.nonEmpty){
//
//      window.setWindowId(config.controlWindowId)
//      for (sensorAlias <- aliases) {
//        for (elem <- sensorAlias.indicatorId) {
//          window.addIndicatorConfig(elem, sensorAlias.sensorAliasId, sensorAlias.sensorAliasName)
//        }
//      }
//      //是否是最上层的window
//      if (config.isTopWindows) {
//        //递归结束
//        (config, window)
//      } else {
//        //获取父window的配置
//        if (map.contains(config.parentWindowId)) {
//          val parentWindowConfig = map.get(config.parentWindowId).get
//          //递归,返回父window
//          val parentWindow = createContainerWindow(parentWindowConfig, map)._2
//          //添加子window
//          parentWindow.addSubWindow(window)
//          //返回子window的配置和构建好子window 的父window 对象
//          (config, parentWindow)
//        } else {
//          logger.warn(ErrorCode("002003b005C", System.currentTimeMillis(), Map("WindowId" -> config.controlWindowId, "parentWindowId" -> config.parentWindowId, "context" -> config.contextId, "contextAllWindow" -> map.map(w => w._2.controlWindowId)), " window 配置错误,找不到父window，构建window结构失败").toJson)
//          (config, null)
//        }
//
//      }
//    }else{
//      logger.warn(ErrorCode("002003b005C", System.currentTimeMillis(), Map("WindowId" -> config.controlWindowId, "parentWindowId" -> config.parentWindowId, "context" -> config.contextId, "contextAllWindow" -> map.map(w => w._2.controlWindowId)), " window 配置错误,找不到父window，构建window结构失败").toJson)
//      (config, null)
//    }
//
//  }
//
//}
