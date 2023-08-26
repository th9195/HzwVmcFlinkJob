package com.hzw.fdc.function.online.MainFabAutoLimit

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.ExceptionInfo
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.TreeMap
import scala.collection.{concurrent, mutable}
import scala.collection.mutable.ListBuffer
/**
 * @author ：gdj
 * @date ：Created in 2021/6/27 23:05
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabAutoLimitByCountTaskProcessWindowFunction extends ProcessWindowFunction[ List[(AutoLimitOneConfig, IndicatorResult)],AutoLimitResult,String,GlobalWindow]{
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabAutoLimitByCountTaskProcessWindowFunction])
  lazy val outputAutoLimitTask = new OutputTag[AutoLimitTask]("AutoLimitTask")

  // 记录已经计算过的AutoLimit 记录信息: controlPlanId|version|indicatorId|specId
  private val haveCalcAutolimitMap = concurrent.TrieMap[String, concurrent.TrieMap[String, ListBuffer[String]]]()

  override def process(key: String,
                       context: Context,
                       elements: Iterable[List[(AutoLimitOneConfig, IndicatorResult)]],
                       out: Collector[AutoLimitResult]): Unit = {

    try {
      val nowTime = System.currentTimeMillis()
      val iterator = elements.iterator

      val map = iterator.flatMap(_.iterator)
        .aggregate(mutable.HashMap[String, (AutoLimitOneConfig,ListBuffer[IndicatorResult])]())({
          (map, y) =>{
            val key= y._1.indicatorId.toString +"," + y._1.specId.toString
            if (map.contains(key)){
              val value = map.get(key).get
              value._2.append(y._2)
            }else{
              map.put(key, (y._1,ListBuffer(y._2)))
            }
            map
          }
        }, _++=_ )

      logger.warn(s"auto limit count log -----------------------------start")

      val controlPlanIdSet: mutable.Set[Long] =  mutable.Set(-1L)

      for (key1 <- map.keySet) {
        try {

          val (autoLimitOneConfig,indicatorResultList) = map.get(key1).get

          val indicatorLog = indicatorResultList.map(x => (x.runId, x.indicatorValue))

          val runNum = indicatorResultList.map(_.runId).distinct.size

          logger.warn(s"by count task: ${autoLimitOneConfig.controlPlanId}-$nowTime indicatorName: ${indicatorResultList.head.indicatorName} " +
            s"\t runData: ${indicatorLog} autoLimitOneConfig: $autoLimitOneConfig")

          val controlPlanId = autoLimitOneConfig.controlPlanId
          val version = autoLimitOneConfig.version
          val indicatorId = autoLimitOneConfig.indicatorId
          val specId = autoLimitOneConfig.specId

          // 判断当前Autolimit是否已经计算过 ,如果没有计算过就正常计算，如果计算过，就不需要在计算
          if(is_calc(controlPlanId,version,indicatorId,specId)){

            // todo 组装 AutoLimitTask 数据
            if(!controlPlanIdSet.contains(autoLimitOneConfig.controlPlanId)){
              context.output(outputAutoLimitTask,
                AutoLimitTask("createTask",
                  s"${autoLimitOneConfig.controlPlanId}-$nowTime",
                  autoLimitOneConfig.controlPlanId,
                  nowTime,
                  runNum
                ))
              controlPlanIdSet.add(autoLimitOneConfig.controlPlanId)
            }

            // todo 计算 AutoLimit
            val result: AutoLimitResult = AutoLimitCalculate.map((autoLimitOneConfig, indicatorResultList.toList, nowTime), logger)

            // todo 记录该 AutoLimit 已经计算过
            addHaveCalcAutoLimitMap(controlPlanId, version, indicatorId, specId)

//            printHaveCalaMap()

            Thread.sleep(200)
            out.collect(result)
          }


        } catch {
          case e: Exception =>
            logger.warn(ErrorCode("007006b005C", System.currentTimeMillis(), Map("AutoLimitOneConfig" -> "")
              , ExceptionInfo.getExceptionInfo(e)).toJson)
        }
      }
      logger.warn(s"auto limit count log -----------------------------end")
    }catch {
      case e: Exception =>
        logger.warn(ErrorCode("007006b005C", System.currentTimeMillis(), Map("auto limit AutoLimitOneConfig" -> "")
          , ExceptionInfo.getExceptionInfo(e)).toJson)
    }
  }


  /**
   * 判断是否需要计算
   * @param controlPlanId
   * @param version
   * @param indicatorId
   * @param specId
   * @return
   */
  def is_calc(controlPlanId: Long, version: Long, indicatorId: Long, specId: Long) = {

    var is_calc = false

    if(!haveCalcAutolimitMap.contains(controlPlanId.toString)){
      // todo 如果没有记录 该controlPlanId
      is_calc = true
    }else{
      val versionMap = haveCalcAutolimitMap.get(controlPlanId.toString).get
      if(!versionMap.contains(version.toString)){
        // todo 如果记录 该controlPlanId 但没有记录该 version
        is_calc = true
      }else{
        val indicator_spec_list = versionMap.get(version.toString).get
        if(!indicator_spec_list.contains(indicatorId + "|" + specId)){
          // todo 如果记录 该controlPlanId 和 version 但是没有记录该 indicatorId|specId
          is_calc = true
        }
      }
    }

    is_calc
  }


  /**
   * 记录已经计算的 AutoLimit
   * @param controlPlanId
   * @param version
   * @param indicatorId
   * @param specId
   */
  def addHaveCalcAutoLimitMap(controlPlanId: Long, version: Long, indicatorId: Long, specId: Long) ={

    val indicator_spec_list: ListBuffer[String] = ListBuffer(indicatorId + "|" + specId)
    if(!haveCalcAutolimitMap.contains(controlPlanId.toString)){
      // todo 如果记录中没有该 controlPlanId
      val versionMap = new concurrent.TrieMap[String, ListBuffer[String]]()
      versionMap.put(version.toString,indicator_spec_list)
      haveCalcAutolimitMap.put(controlPlanId.toString,versionMap)
    }else {
      val versionMap = haveCalcAutolimitMap.get(controlPlanId.toString).get
      if (!versionMap.contains(version.toString)){
        // todo 如果记录中有 controlPlanId 但是没有该 version
        versionMap.put(version.toString,indicator_spec_list)
        haveCalcAutolimitMap.put(controlPlanId.toString,versionMap)
      }else {
        var currentList = versionMap.get(version.toString).get
        if(!currentList.contains(indicatorId + "|" + specId)){
          // todo 如果记录中有 该controlPlanId 、 version 但是没有该 indicatorId|specId
          currentList += indicatorId + "|" + specId
          versionMap.put(version.toString,currentList)
          haveCalcAutolimitMap.put(controlPlanId.toString,versionMap)
        }
      }

      // todo haveCalcAutolimitMap 每个controlPlanId 只需要保留一个版本
      val versionList = versionMap.keys.map(_.toLong)
      if(versionList.size > 1){
        val minVserion = versionList.min
        versionMap.remove(minVserion.toString)
        haveCalcAutolimitMap.put(controlPlanId.toString,versionMap)
      }
    }
  }


  /**
   * 打印调试
   */
  def printHaveCalaMap() = {
    logger.error(s"--------------------by Count------------------")
    haveCalcAutolimitMap.foreach((map1: (String, TrieMap[String, ListBuffer[String]])) => {
      logger.error(s"controlPlanId == ${map1._1}")
      map1._2.foreach((map2: (String, ListBuffer[String])) => {
        logger.error(s"\tversion == ${map2._1}")
        map2._2.foreach(value => {
          logger.error(s"\t\tvalue == ${value}")
        })
      })
    })
  }


}
