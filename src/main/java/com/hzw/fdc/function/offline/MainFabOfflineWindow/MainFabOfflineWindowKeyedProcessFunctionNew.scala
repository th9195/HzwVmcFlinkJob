package com.hzw.fdc.function.offline.MainFabOfflineWindow

import com.hzw.fdc.engine.api.{ApiControlWindow, EngineFunction, IControlWindow}
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResponse
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：Created in 2021/5/20 15:30
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabOfflineWindowKeyedProcessFunctionNew extends KeyedProcessFunction[String, (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), FdcData[OfflineWindowListData]] {


  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineWindowKeyedProcessFunctionNew])

  /**
   *   数据流
   */
  override def processElement(input: (OfflineTask, List[OfflineMainFabRawData], OfflineTaskRunData),
                              ctx: KeyedProcessFunction[String, (OfflineTask, List[OfflineMainFabRawData],OfflineTaskRunData), FdcData[OfflineWindowListData]]#Context,
                              collector: Collector[FdcData[OfflineWindowListData]]): Unit = {

    try {
      logger.warn(s"debug OfflineTask taskId: ${input._1.taskId}\t" +
        s"List[OfflineMainFabRawData] Size: ${input._2.size}\t OfflineTaskRunData:${input._3}")

      val offlineTask = input._1
      val rawDataList = input._2
      val offlineTaskRunData: OfflineTaskRunData =input._3

//      logger.warn(s"rawDataList == ${rawDataList}")

      //用来取基础数据
      val headData: OfflineMainFabRawData = rawDataList.head

      val indicatorConfigs: List[TreeConfig] = getIndicatorConfigList(offlineTask.indicatorTree)

      for (oneIndicatorConfig <- indicatorConfigs) {

        val currentSensorAlias = oneIndicatorConfig.indicatorConfig.sensorAlias

        val rawDataSensorDataList = getRawDataSensorData(rawDataList, currentSensorAlias)

        if(oneIndicatorConfig.windowTree.nonEmpty && rawDataSensorDataList.nonEmpty) {
          val currentWindowConfig: WindowConfigData = oneIndicatorConfig.windowTree.get.current
          val sensorAliasNew = currentWindowConfig.sensorAlias.map(x => {
            WindowConfigAlias(chamberName=x.chamberName,
              toolName=x.toolName,
              sensorAliasId=x.sensorAliasId,
              sensorAliasName=x.sensorAliasName,
              svid=x.svid,
              indicatorId=List()
            )
          })


          val windowConfigDataMap = new TrieMap[Long, WindowConfigData]()
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
            val dataflow = EngineFunction.buildOfflineDataFlowData(rawDataList)

            dataflow.setStartTime(offlineTaskRunData.runStart)
            dataflow.setStopTime(offlineTaskRunData.runEnd)

            iControlWindow.attachDataFlow(dataflow)

            val apiControlWindow = new ApiControlWindow()
            val clipWindowResult: ClipWindowResult = apiControlWindow.split(iControlWindow, true)

            if(null != clipWindowResult &&
              WindowClipResponse.SUCCESS_CODE.equals(clipWindowResult.msgCode) &&
              null != clipWindowResult.windowTimeRangeList){

              val windowTimeRangeList = clipWindowResult.windowTimeRangeList

              val currentWindowTimeRangeList = windowTimeRangeList.filter(windowTimeRange => {
                windowTimeRange.windowId == currentWindowConfig.controlWindowId
              })

              if(currentWindowTimeRangeList.nonEmpty){
                // 切窗口成功
                val offlineWindowListData = generateSuccessOfflineWindowListData(offlineTask,
                  offlineTaskRunData,
                  oneIndicatorConfig,
                  indicatorConfigs,
                  rawDataSensorDataList,
                  currentWindowConfig,
                  currentWindowTimeRangeList)

                collector.collect(FdcData("offlineFdcWindowDatas",offlineWindowListData))
              }else{
                // 没有 当前windowId 的结果
                logger.warn(s"切窗口成功，但是没有当前windowId的结果 : clipWindowResult == ${clipWindowResult} \n " +
                  s"offlineTaskRunData == ${offlineTaskRunData}")
                val offlineWindowListData = generateFailedOfflineWindowListData(offlineTask,indicatorConfigs,oneIndicatorConfig,offlineTaskRunData,currentWindowConfig)
                collector.collect(FdcData("offlineFdcWindowDatas",offlineWindowListData))
              }

            }else{
              // 切窗口失败
              logger.warn(s"切窗口失败 : clipWindowResult == ${clipWindowResult} \n " +
                s"offlineTaskRunData == ${offlineTaskRunData}")
              val offlineWindowListData = generateFailedOfflineWindowListData(offlineTask,indicatorConfigs,oneIndicatorConfig,offlineTaskRunData,currentWindowConfig)
              collector.collect(FdcData("offlineFdcWindowDatas",offlineWindowListData))
            }
          }else{
            //获取raw data 失败的数据
            logger.warn(s"获取raw data 失败的数据 : offlineTaskRunData == ${offlineTaskRunData}")
            val offlineWindowListData = generateFailedOfflineWindowListData(offlineTask,indicatorConfigs,oneIndicatorConfig,offlineTaskRunData,currentWindowConfig)
            collector.collect(FdcData("offlineFdcWindowDatas",offlineWindowListData))
          }

        }else {
          logger.warn(s"Offline processElement error: taskId: ${input._1.taskId} \n " +
            s"oneIndicatorConfig.windowTree is Empty or rawDataSensorDataList is Empty")
        }
      }

    } catch {
      case e: Exception => logger.warn(s"Offline processElement error：taskId: ${input._1.taskId}  List[OfflineMainFabRawData]: ${input._2.toJson} Exception :${ExceptionInfo.getExceptionInfo(e)}"  )
    }

  }


  /**
   *
   * @param rawDataList
   * @param currentSensorAlias
   * @return
   */
  private def getRawDataSensorData(rawDataList: List[OfflineMainFabRawData], currentSensorAlias: String) = {

    val rawDataSensorDataList = new ListBuffer[RawDataSensorData]
    rawDataList.foreach((rawData: OfflineMainFabRawData) => {

      val stepId = rawData.stepId
      val timestamp = rawData.timestamp
      rawData.data.distinct.foreach((sensor: OfflineSensorData) => {
        if (sensor.sensorAlias == currentSensorAlias) {

          val unit = sensor.unit
          val sensorValue = sensor.sensorValue

          val rawDataSensorData = RawDataSensorData(sensorValue = sensorValue,
            timestamp = timestamp,
            step = stepId,
            unit = unit)

          rawDataSensorDataList.append(rawDataSensorData)
        }
      })
    })

    rawDataSensorDataList
  }

  /**
   * 获取 indicatorNode的 sensor list
   *
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
      List(indicatorTree.current).distinct
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


  /**
   * 生成 切window失败 的 OfflineWindowListData
   * @param offlineTask
   * @param indicatorConfigs
   * @param oneIndicatorConfig
   * @param offlineTaskRunData
   * @param currentWindowConfig
   * @return
   */
  def generateFailedOfflineWindowListData(offlineTask: OfflineTask,
                                          indicatorConfigs: List[TreeConfig],
                                          oneIndicatorConfig: TreeConfig,
                                          offlineTaskRunData: OfflineTaskRunData,
                                          currentWindowConfig: WindowConfigData): OfflineWindowListData = {

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

    offlineWindowListData
  }

  /**
   * 生成 OfflineWindowData
   * @param currentWindowConfig
   * @param oneIndicatorConfig
   * @param rawDataSensorDataListResult
   * @param cycleCount
   * @param cycleIndex
   * @param startTime
   * @param stopTime
   * @return
   */
  def generateOfflineWindowData(currentWindowConfig: WindowConfigData,
                                oneIndicatorConfig: TreeConfig,
                                rawDataSensorDataListResult: ListBuffer[RawDataSensorData],
                                cycleCount: Int,
                                cycleIndex:Int,
                                startTime:Long,
                                stopTime:Long) = {

    val sensorAlias = oneIndicatorConfig.indicatorConfig.sensorAlias
    val indicatorId = oneIndicatorConfig.indicatorConfig.indicatorId

    // 数据结构转换成 sensorDataList
    val sensorDataLists = rawDataSensorDataListResult.map(sensorData => {
      sensorDataList(sensorValue = sensorData.sensorValue,
        timestamp = sensorData.timestamp,
        step = sensorData.step)
    }).toList

    OfflineWindowData(sensorName = null,
      sensorAlias = sensorAlias,
      unit = rawDataSensorDataListResult.head.unit,
      indicatorId = List(indicatorId),
      cycleUnitCount = cycleCount,
      cycleIndex = cycleIndex,
      windowStartTime = startTime,
      windowStopTime = stopTime,
      isCalculationSuccessful = true,
      calculationInfo = "",
      sensorDataList = sensorDataLists)
  }


  /**
   * 生成 成功切window 的 OfflineWindowListData
   * @param offlineTask
   * @param offlineTaskRunData
   * @param oneIndicatorConfig
   * @param indicatorConfigs
   * @param rawDataSensorDataList
   * @param currentWindowConfig
   * @param currentWindowTimeRangeList
   * @return
   */
  def generateSuccessOfflineWindowListData(offlineTask: OfflineTask,
                                           offlineTaskRunData: OfflineTaskRunData,
                                           oneIndicatorConfig: TreeConfig,
                                           indicatorConfigs: List[TreeConfig],
                                           rawDataSensorDataList: ListBuffer[RawDataSensorData],
                                           currentWindowConfig: WindowConfigData,
                                           currentWindowTimeRangeList: List[ClipWindowTimestampInfo]) = {

    var cycleIndex = 0
    val offlineWindowDataList = new ListBuffer[OfflineWindowData]

    val cycleCount = currentWindowTimeRangeList.size
    currentWindowTimeRangeList.foreach(windowTimeRange => {
      val startTime = windowTimeRange.startTime
      val endTime = windowTimeRange.endTime
      val startInclude = windowTimeRange.startInclude
      val endInclude = windowTimeRange.endInclude
      val rawDataSensorDataListResult = rawDataSensorDataList.filter(rawDataSensor => {
        val timestamp = rawDataSensor.timestamp

        // 根据 startInclude endInclude 判断是否包含起始时间或者结束时间
        if(startInclude && endInclude){
          startTime <= timestamp && timestamp <= endTime
        }else if(startInclude && !endInclude){
          startTime <= timestamp && timestamp < endTime
        }else if(!startInclude && endInclude){
          startTime < timestamp && timestamp <= endTime
        }else{
          startTime < timestamp && timestamp < endTime
        }
      })

      if(rawDataSensorDataListResult.nonEmpty){
        if(currentWindowConfig.controlWindowType == MainFabConstants.CycleWindowMaxType ||
          currentWindowConfig.controlWindowType == MainFabConstants.CycleWindowMinType){
          // 如果是cycle Window cycleIndix 从1 开始计数
          cycleIndex += 1
        }else{
          // 如果不是cycle Window cycleIndix = -1
          cycleIndex = -1
        }

        // 组装 WindowData 数据
        val offlineWindowData: OfflineWindowData = generateOfflineWindowData(currentWindowConfig,
          oneIndicatorConfig,
          rawDataSensorDataListResult,
          cycleCount,
          cycleIndex,
          startTime,
          endTime)
        offlineWindowDataList.append(offlineWindowData)
      }
    })

    val offlineIndicatorConfigList = indicatorConfigs.map(x => x.indicatorConfig)
    val windowStartTime = offlineWindowDataList.head.windowStartTime
    val windowEndTime = offlineWindowDataList.head.windowStopTime

    OfflineWindowListData(batchId = Option(offlineTask.batchId),
      taskId = offlineTask.taskId,
      toolName = offlineTaskRunData.toolName,
      chamberName = offlineTaskRunData.chamberName,
      runId = offlineTaskRunData.runId,
      dataMissingRatio = offlineTaskRunData.dataMissingRatio,
      controlWindowId = currentWindowConfig.controlWindowId,
      windowStart = currentWindowConfig.windowStart,
      windowStartTime = windowStartTime,
      windowEnd = currentWindowConfig.windowEnd,
      windowEndTime = windowEndTime,
      windowTimeRange = windowEndTime - windowStartTime,
      runStartTime = offlineTaskRunData.runStart,
      runEndTime = offlineTaskRunData.runEnd,
      windowEndDataCreateTime = System.currentTimeMillis(),
      windowDatasList = offlineWindowDataList.toList,
      offlineIndicatorConfigList = offlineIndicatorConfigList)

  }


}