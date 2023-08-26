package com.hzw.fdc.function.online.MainFabLogisticIndicator

import com.hzw.fdc.function.online.MainFabIndicator.InitIndicatorOracle
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{ConfigData, ErrorCode, FdcData, IndicatorConfig, IndicatorResult}
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.mutable.ListBuffer

/**
 * @author ：gdj
 * @date ：Created in 2021/8/29 16:13
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
class MainFabLogisticIndicatorProcessFunction extends KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig], (IndicatorConfig, IndicatorResult)] {

  // {"controlPlanId": {"version" : {"参与计算的indicatorId": {"LogisticIndicator结果 的indicatorId"": "IndicatorConfig"}}}}
  val indicatorConfigByAll = new concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[Long, IndicatorConfig]]]]()

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabLogisticIndicatorProcessFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    val initConfig:ListBuffer[ConfigData[IndicatorConfig]] = readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

//    val initConfig: ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList

    initConfig.filter(i => i.datas.algoClass.equals("logisticIndicator")).foreach(config => addIndicatorConfigToTCSP(config))

//    logger.warn(s"LogisticIndicatorJob Config indicatorConfigByAll: ${indicatorConfigByAll.toJson}")
  }

  override def processElement(fdcDataIndicatorResult: FdcData[IndicatorResult],
                              readOnlyContext: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig], (IndicatorConfig, IndicatorResult)]#ReadOnlyContext,
                              collector: Collector[(IndicatorConfig, IndicatorResult)]): Unit = {
    try {
      val indicatorResult = fdcDataIndicatorResult.datas
      if (indicatorConfigByAll.contains(indicatorResult.controlPlanId)) {
        val controlPlanMap = indicatorConfigByAll(indicatorResult.controlPlanId)
        if (controlPlanMap.contains(indicatorResult.controlPlanVersion)) {
          val versionMap = controlPlanMap(indicatorResult.controlPlanVersion)

          if (versionMap.contains(indicatorResult.indicatorId)) {
            val logisticIndicatorList = versionMap(indicatorResult.indicatorId)

            for (elem <- logisticIndicatorList) {

              logger.warn(s"debug processElement data : ${(elem._2, indicatorResult)}")
              collector.collect((elem._2, indicatorResult))
            }

          }
        }
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("LogisticIndicator", System.currentTimeMillis(),
        Map("function"->"processElement", "message" -> fdcDataIndicatorResult), ex.toString).toJson)
    }

  }

  override def processBroadcastElement(indicatorConfig: ConfigData[IndicatorConfig],
                                       context: KeyedBroadcastProcessFunction[String, FdcData[IndicatorResult], ConfigData[IndicatorConfig], (IndicatorConfig, IndicatorResult)]#Context,
                                       collector: Collector[(IndicatorConfig, IndicatorResult)]): Unit = {

    if(indicatorConfig.datas.algoClass=="logisticIndicator"){
      addIndicatorConfigToTCSP(indicatorConfig)
    }
  }


  def addIndicatorConfigToTCSP(FdcIndicatorConfig: ConfigData[IndicatorConfig]): Unit = {

    try {


      val indicatorConfig = FdcIndicatorConfig.datas

      val algoParamList: List[String] = indicatorConfig.algoParam.split("\\^_hzw_\\^").toList
      if (algoParamList.size >= 2) {
        val indicatorList = algoParamList.drop(1)


        for (one <- indicatorList) {

          val indicatorId = one.toLong

          val controlPlanKey = indicatorConfig.controlPlanId

          val version = indicatorConfig.controlPlanVersion

          val logisticIndicatorId = indicatorConfig.indicatorId



          if (!FdcIndicatorConfig.status) {

            // 删除indicatorConfig逻辑
            if (this.indicatorConfigByAll.contains(controlPlanKey)) {


              val versionMap = this.indicatorConfigByAll(controlPlanKey)
              //版本
              if (versionMap.contains(version)) {
                val indicatorMap = versionMap(version)
                //参与计算indicatorMap
                if (indicatorMap.contains(indicatorId)) {
                  val logisticIndicatorMap = indicatorMap(indicatorId)
                  //logisticIndicatorMap
                  if (logisticIndicatorMap.contains(logisticIndicatorId)) {
                    logisticIndicatorMap.remove(logisticIndicatorId)

                  }else{
                    logger.warn(s"LogisticIndicatorJob Config logisticIndicator no exist: " + indicatorConfig)
                  }

                }else{
                  logger.warn(s"LogisticIndicatorJob Config 参与计算的 logisticIndicator no exist: " + indicatorConfig)
                }

              }else{
                logger.warn(s"LogisticIndicatorJob Config version no exist: " + indicatorConfig)
              }
              this.indicatorConfigByAll += (controlPlanKey -> versionMap)

            }else{
              logger.warn(s"LogisticIndicatorJob Config controlPlan no exist: " + indicatorConfig)
            }

          } else {

            logger.warn(s"add Config logisticIndicator controlPlanKey: $controlPlanKey --> version:$version --> indicatorId:$indicatorId --> logisticIndicatorId:$logisticIndicatorId --> indicatorConfig ${indicatorConfig.toJson}")
            // 新增逻辑
            if (this.indicatorConfigByAll.contains(controlPlanKey)) {
              val versionMap = this.indicatorConfigByAll(controlPlanKey)
              //版本
              if (versionMap.contains(version)) {
                val indicatorMap = versionMap(version)
                //参与计算的indicator
                if (indicatorMap.contains(indicatorId)) {
                  val logisticIndicatorMap = indicatorMap(indicatorId)
                  logisticIndicatorMap.put(logisticIndicatorId, indicatorConfig)
                  indicatorMap += (indicatorId -> logisticIndicatorMap)
                  versionMap += (version -> indicatorMap)

                  //新logisticIndicatorId
                } else {
                  val logisticIndicatorMap = concurrent.TrieMap[Long, IndicatorConfig](logisticIndicatorId -> indicatorConfig)
                  indicatorMap += (indicatorId -> logisticIndicatorMap)
                  versionMap += (version -> indicatorMap)

                }

                //新版本
              } else {
                val logisticIndicatorMap = concurrent.TrieMap[Long, IndicatorConfig](logisticIndicatorId -> indicatorConfig)
                val indicatorMap = concurrent.TrieMap[Long, concurrent.TrieMap[Long, IndicatorConfig]](indicatorId -> logisticIndicatorMap)
                versionMap += (version -> indicatorMap)
                // 更新版本
                val k = versionMap.keys
                if (k.size > 2) {
                  val minVersion = k.toList.min
                  versionMap.remove(minVersion)
                  logger.warn(s"addIndicatorConfigToTCSP LogisticIndicatorJob: 删除旧版本 " + minVersion + "\tindicatorConfig: " + indicatorConfig)
                }
              }


              this.indicatorConfigByAll += (controlPlanKey -> versionMap)


            } else {
              // 新 control Plan
              val logisticIndicatorMap = concurrent.TrieMap[Long, IndicatorConfig](logisticIndicatorId -> indicatorConfig)
              val indicatorMap = concurrent.TrieMap[Long, concurrent.TrieMap[Long, IndicatorConfig]](indicatorId -> logisticIndicatorMap)
              val versionMap = concurrent.TrieMap[Long, concurrent.TrieMap[Long, concurrent.TrieMap[Long, IndicatorConfig]]](version -> indicatorMap)

              this.indicatorConfigByAll += (controlPlanKey -> versionMap)
            }
          }
        }


      } else {
        logger.warn(s"addIndicatorConfigToTCSP LogisticIndicatorJob: 配置错误 algoParam " + FdcIndicatorConfig)
      }


    } catch {
      case ex: Exception => logger.error(s"LogisticIndicatorJob_addIndicatorConfigToTCSP ERROR:  ${ExceptionInfo.getExceptionInfo(ex)} FdcIndicatorConfig：$FdcIndicatorConfig")
    }

  }

}
