package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.engine.api.{ApiControlWindow, EngineFunction, IControlWindow}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{OfflineTaskRunData, _}
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{concurrent, mutable}

/**
 * @author ：gdj
 * @date ：Created in 2021/5/20 15:30
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineWindowKeyedProcessFunction extends KeyedProcessFunction[String, (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), FdcData[OfflineWindowListData]] {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineWindowKeyedProcessFunction])


  /**
   *   数据流
   */
  override def processElement(input: (OfflineTask, List[OfflineMainFabRawData], OfflineTaskRunData),
                              ctx: KeyedProcessFunction[String, (OfflineTask, List[OfflineMainFabRawData],
                                OfflineTaskRunData), FdcData[OfflineWindowListData]]#Context,
                              collector: Collector[FdcData[OfflineWindowListData]]): Unit = {

    try {
      logger.warn(s"debug OfflineTask OfflineTaskId == ${input._1.taskId} \n " +
        s"List[OfflineMainFabRawData] Size: ${input._2.size} \n " +
        s"OfflineTaskRunData == ${input._3}")

      val offlineTask = input._1
      val rawData = input._2
      val offlineTaskRunData=input._3

      //用来取基础数据
      val headData = rawData.head

      val indicatorConfigs = getIndicatorConfigList(offlineTask.indicatorTree)

      for (oneIndicatorConfig <- indicatorConfigs) {


        if(oneIndicatorConfig.windowTree.nonEmpty) {
          val currentWindowConfig = oneIndicatorConfig.windowTree.get.current

          val sensorAliasNew = currentWindowConfig.sensorAlias.map(x => {
            WindowConfigAlias(chamberName=x.chamberName,
              toolName=x.toolName,
              sensorAliasId=x.sensorAliasId,
              sensorAliasName=x.sensorAliasName,
              svid=x.svid,
              indicatorId=List()
            )
          })


          val windowConfigDataMap = new concurrent.TrieMap[Long, WindowConfigData]()

          // 获取所有的WindowConfigData
          def getAllWindowConfigData(windowTree: WindowTree): Unit = {
            var currentNew = windowTree.current

            if(windowTree != oneIndicatorConfig.windowTree.get){
              val current = windowTree.current
              currentNew = current.copy(sensorAlias=sensorAliasNew)
            }

            windowConfigDataMap.put(windowTree.current.controlWindowId, currentNew)

            //递归
            if(windowTree.next.nonEmpty){
              getAllWindowConfigData(windowTree.next.get)
            }
          }

          getAllWindowConfigData(oneIndicatorConfig.windowTree.get)


          //获取raw data成功的才划分窗口计算
          if(offlineTaskRunData.isCalculationSuccessful){


           val res = createContainerWindow(oneIndicatorConfig.windowTree.get.current, windowConfigDataMap)
            val iControlWindow = res._2


          val dataflow = EngineFunction.buildOfflineDataFlowData(rawData)

          dataflow.setStartTime(headData.runStart)
          dataflow.setStopTime(headData.runEnd)

          iControlWindow.attachDataFlow(dataflow)


          val results = try {
            ApiControlWindow.calculate(iControlWindow).toList
          } catch {
            case e: Exception => logger.warn(s"Offline calculate error：taskId: ${input._1.taskId} OfflineTask: ${input._1.toJson}  List[OfflineMainFabRawData]: ${input._2.toJson} Exception :${ExceptionInfo.getExceptionInfo(e)}"  )
              Nil
          }

        val offlineWindowListData =  if (results.nonEmpty){
          val headResults = results.head
          val dataList = headResults.sensorDataList
          val windowDatasList = results.map(x => OfflineWindowData(sensorName = x.sensorName,
            sensorAlias = x.sensorAlias,
            unit = x.unit,
            indicatorId = x.indicatorId.distinct,
            cycleUnitCount = x.cycleUnitCount,
            cycleIndex = x.cycleIndex,
            windowStartTime = x.startTime,
            windowStopTime = x.stopTime,
            isCalculationSuccessful = true,
            calculationInfo = "",
            sensorDataList = x.sensorDataList.map(y => sensorDataList(
              sensorValue = y.sensorValue,
              timestamp = y.timestamp,
              step = y.step))))

          val offlineIndicatorConfigList = indicatorConfigs.map(x => x.indicatorConfig)


          OfflineWindowListData(batchId = Option(offlineTask.batchId),
            taskId = offlineTask.taskId,
            toolName = headData.toolName,
            chamberName = headData.chamberName,
            runId = headData.runId,
            dataMissingRatio = headData.dataMissingRatio,
            controlWindowId = currentWindowConfig.controlWindowId,
            windowStart = currentWindowConfig.windowStart,
            windowStartTime = headResults.startTime,
            windowEnd = currentWindowConfig.windowEnd,
            windowEndTime = headResults.stopTime,
            windowTimeRange = headResults.stopTime - headResults.startTime,
            runStartTime = headData.runStart,
            runEndTime = headData.runEnd,
            windowEndDataCreateTime = System.currentTimeMillis(),
            windowDatasList = windowDatasList,
            offlineIndicatorConfigList = offlineIndicatorConfigList)


          }else{


          val windowDatasList: List[OfflineWindowData] = results.map((x: WindowData) => OfflineWindowData(sensorName = x.sensorName,
            sensorAlias = x.sensorAlias,
            unit = x.unit,
            indicatorId = x.indicatorId,
            cycleUnitCount = x.cycleUnitCount,
            cycleIndex = x.cycleIndex,
            windowStartTime = x.startTime,
            windowStopTime = x.stopTime,
            isCalculationSuccessful = false,
            calculationInfo = "",
            sensorDataList = x.sensorDataList.map(y => sensorDataList(
              sensorValue = y.sensorValue,
              timestamp = y.timestamp,
              step = y.step))))

          val offlineIndicatorConfigList = indicatorConfigs.map(x => x.indicatorConfig)

          val headData = rawData.head


          OfflineWindowListData(batchId = Option(offlineTask.batchId),
            taskId = offlineTask.taskId,
            toolName = headData.toolName,
            chamberName = headData.chamberName,
            runId = headData.runId,
            dataMissingRatio = headData.dataMissingRatio,
            controlWindowId = currentWindowConfig.controlWindowId,
            windowStart = currentWindowConfig.windowStart,
            windowStartTime = 0L,
            windowEnd = currentWindowConfig.windowEnd,
            windowEndTime = 0L,
            windowTimeRange = 0L,
            runStartTime = headData.runStart,
            runEndTime = headData.runEnd,
            windowEndDataCreateTime = System.currentTimeMillis(),
            windowDatasList = windowDatasList,
            offlineIndicatorConfigList = offlineIndicatorConfigList)
          }
          collector.collect(FdcData("offlineFdcWindowDatas",offlineWindowListData))

        }else{
            //获取raw data 失败的数据
            val offlineIndicatorConfigList = indicatorConfigs.map(x => x.indicatorConfig)

           val windowList =OfflineWindowData(sensorName = oneIndicatorConfig.indicatorConfig.sensorAlias,
              sensorAlias = oneIndicatorConfig.indicatorConfig.sensorAlias,
              unit = "unit",
              indicatorId =  List(oneIndicatorConfig.indicatorConfig.indicatorId),
              cycleUnitCount = 0,
              cycleIndex = 0,
              windowStartTime = 0,
              windowStopTime = 0,
              isCalculationSuccessful = false,
              calculationInfo = offlineTaskRunData.calculationInfo,
              sensorDataList = Nil)

           val offlineWindowListData= OfflineWindowListData(batchId = Option(offlineTask.batchId),
             taskId = offlineTask.taskId,
              toolName = offlineTaskRunData.toolName,
              chamberName = offlineTaskRunData.chamberName,
              runId = offlineTaskRunData.runId,
              dataMissingRatio = offlineTaskRunData.dataMissingRatio,
              controlWindowId = currentWindowConfig.controlWindowId,
              windowStart = currentWindowConfig.windowStart,
              windowStartTime = 0L,
              windowEnd = currentWindowConfig.windowEnd,
              windowEndTime = 0L,
              windowTimeRange = 0L,
              runStartTime = offlineTaskRunData.runStart,
              runEndTime = offlineTaskRunData.runEnd,
              windowEndDataCreateTime = System.currentTimeMillis(),
              windowDatasList = List(windowList),
              offlineIndicatorConfigList = offlineIndicatorConfigList)

              collector.collect(FdcData("offlineFdcWindowDatas",offlineWindowListData))
        }

        }
      }


    } catch {
      case e: Exception => logger.warn(s"Offline processElement error：taskId: ${input._1.taskId}  List[OfflineMainFabRawData]: ${input._2.toJson} Exception :${ExceptionInfo.getExceptionInfo(e)}"  )
    }

  }

  /**
   * 获取 indicatorNode的 sensor list
   * @param
   * @return
   */
  def getIndicatorConfigList(indicatorTree:IndicatorTree): List[TreeConfig] ={

    if(indicatorTree.nexts.nonEmpty) {

      val list2=for (elem <- indicatorTree.nexts) yield {
        getIndicatorConfigList(elem) :+ indicatorTree.current
      }

      val list= list2.reduceLeft(_ ++ _)
      list.distinct
    }else{
//      if(indicatorTree.current.windowTree.nonEmpty){
//        List(indicatorTree.current)
//      }
      List(indicatorTree.current)
    }

  }


  /**
   * 构建windows树
   *
   */
  def createContainerWindowTree(windowTree: WindowTree,indicatorList:Option[(Long,String)]): IControlWindow = {

    //构建父window ICW
    var iCW: IControlWindow = null
    iCW= ApiControlWindow.parse(windowTree.current.controlWindowType,
     windowTree.current.windowStart,
     windowTree.current.windowEnd)
    iCW.setWindowId( windowTree.current.controlWindowId)

    for (oneAlias <- windowTree.current.sensorAlias.distinct) {

      for (elem <- oneAlias.indicatorId if indicatorList.nonEmpty &&
        indicatorList.get._1.equals(elem) && indicatorList.get._2.equals(oneAlias.sensorAliasName)) {

        iCW.addIndicatorConfig(elem,oneAlias.sensorAliasId,indicatorList.get._2)

      }

    }

    //递归 添加 子window
    if(windowTree.next.nonEmpty){

      iCW.addSubWindow(createContainerWindowTree(windowTree.next.get,None))

    }

    iCW

  }


  /**
   * 构建windows树
   *
   */
  def createContainerWindow(config: WindowConfigData, map: concurrent.TrieMap[Long, WindowConfigData]): (WindowConfigData, IControlWindow) = {
    var window: IControlWindow = null


    window = ApiControlWindow.parse(config.controlWindowType, config.windowStart, config.windowEnd)

    val aliases = config.sensorAlias.filter(_.sensorAliasName != null)

    if(aliases.nonEmpty){

      window.setWindowId(config.controlWindowId)
      for (sensorAlias <- aliases) {
        for (elem <- sensorAlias.indicatorId) {
          window.addIndicatorConfig(elem, sensorAlias.sensorAliasId, sensorAlias.sensorAliasName)
        }
      }
      //是否是最上层的window
      if (config.isTopWindows) {
        //递归结束
        (config, window)
      } else {
        //获取父window的配置
        if (map.contains(config.parentWindowId)) {
          val parentWindowConfig = map.get(config.parentWindowId).get
          //递归,返回父window
          val parentWindow = createContainerWindow(parentWindowConfig, map)._2
          //添加子window
          parentWindow.addSubWindow(window)
          //返回子window的配置和构建好子window 的父window 对象
          (config, parentWindow)
        } else {
          logger.warn(ErrorCode("002003b005C", System.currentTimeMillis(), Map("WindowId" -> config.controlWindowId, "parentWindowId" -> config.parentWindowId, "context" -> config.contextId, "contextAllWindow" -> map.map(w => w._2.controlWindowId)), " window 配置错误,找不到父window，构建window结构失败").toJson)
          (config, null)
        }

      }
    }else{
      logger.warn(ErrorCode("002003b005C", System.currentTimeMillis(), Map("WindowId" -> config.controlWindowId, "parentWindowId" -> config.parentWindowId, "context" -> config.contextId, "contextAllWindow" -> map.map(w => w._2.controlWindowId)), " window 配置错误,找不到父window，构建window结构失败").toJson)
      (config, null)
    }

  }

}