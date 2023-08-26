package com.hzw.fdc.function.online.MainFabIndicator

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.ALGO.ALGO
import com.hzw.fdc.scalabean._
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.util.NoSuchElementException

import com.hzw.fdc.util.InitFlinkFullConfigHbase.{IndicatorDataType, readHbaseAllConfig}

import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}


/**
 * @author gdj
 * @create 2020-07-01-13:53
 *
 */
@SerialVersionUID(1L)
class FdcIndicatorConfigBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, fdcWindowData, JsonNode, (ListBuffer[(ALGO, IndicatorConfig)], ListBuffer[RawData])] {

  lazy private val logger: Logger = LoggerFactory.getLogger(classOf[FdcIndicatorConfigBroadcastProcessFunction])


  //key: {"controlPlanId": {"controlPlanVersion": {"indicatorId": "IndicatorConfigScala"}}}
  var indicatorConfigByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String,
    concurrent.TrieMap[Long, IndicatorConfig]]]()


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

//    val initConfig: ListBuffer[ConfigData[IndicatorConfig]] = InitFlinkFullConfigHbase.IndicatorConfigList
    val initConfig: ListBuffer[ConfigData[IndicatorConfig]] =  readHbaseAllConfig[IndicatorConfig](ProjectConfig.HBASE_SYNC_INDICATOR_TABLE, IndicatorDataType)

    logger.warn("indicatorConfig SIZE: " + initConfig.size)

    initConfig.foreach(config => {
      config.`dataType` match {
        case "indicatorconfig" => addIndicatorConfigToTCSP(config)

        case _ => logger.warn(s"FdcIndicator job open no mach type: " + config.`dataType`)
      }
    })
  }

  override def processElement(windowData: fdcWindowData, ctx: KeyedBroadcastProcessFunction[String, fdcWindowData,
    JsonNode, (ListBuffer[(ALGO, IndicatorConfig)], ListBuffer[RawData])]#ReadOnlyContext,
                              out: Collector[(ListBuffer[(ALGO, IndicatorConfig)], ListBuffer[RawData])]): Unit = {
    try {
//      record.get("dataType").asText() match {
//
//        case "fdcWindowDatas" =>
          try {
            val recordStarTiIme = System.currentTimeMillis()

//            val windowData = toBean[FdcData[windowListData]](record)
            val myDatas = windowData.datas
            val windowDataList = myDatas.windowDatasList

            val controlPlanId = myDatas.controlPlanId.toString
            val controlPlanVersion = myDatas.controlPlanVersion.toString

            if (!this.indicatorConfigByAll.contains(controlPlanId)) {
              logger.warn(ErrorCode("003008d001C", System.currentTimeMillis(),
                Map("controlPlanId" -> ""), "indicator job indicatorConfigByAll mo match: ").toJson)
              return
            }

            val cycleMap = new mutable.HashMap[String, mutable.HashMap[Int, RawData]]()


            val versionMap = indicatorConfigByAll(controlPlanId)

            if (!versionMap.contains(controlPlanVersion)) {
              logger.warn(ErrorCode("003008d001C", System.currentTimeMillis(), Map("controlPlanId" -> controlPlanId, "exit version"
                -> versionMap.keys, "match version" -> controlPlanVersion), "indicator_contains version no exist").toJson)
              return
            }

            val indicatorMap = versionMap(controlPlanVersion)
            val baseMessage = myDatas.copy(windowDatasList=List())

            for (windowDataScala <- windowDataList) {
              val listdata = windowDataScala.sensorDataList
              val indicatorIds = windowDataScala.indicatorId

              val cycleIndex = windowDataScala.cycleIndex

              val rawData = parseRawData(baseMessage, listdata, windowDataScala, cycleIndex.toString)


              // 保存cycle window 的数据
              if (cycleIndex != -1) {
                for (indicator <- indicatorIds) {
                  val key = indicator.toString + "|" + windowDataScala.sensorAlias
                  if (cycleMap.contains(key)) {
                    val value = cycleMap(key)
                    value += (cycleIndex -> rawData)
                    cycleMap.put(key, value)
                  } else {
                    val rawMap = mutable.HashMap[Int, RawData](cycleIndex -> rawData)
                    cycleMap += (key -> rawMap)
                  }
                }
              } else {
                if (indicatorIds.nonEmpty) {
                  val indicatorConfigList = new ListBuffer[(ALGO, IndicatorConfig)]()
                  for (elem <- indicatorIds) {
                    if (indicatorMap.contains(elem)) {
                      val indicatorConfigScala = indicatorMap(elem)

                      indicatorConfigList.append(Tuple2(parseAlgo(indicatorConfigScala), indicatorConfigScala))

                    } else {
                      logger.warn(s"indicatorMap not contains  $elem")
                    }
                  }

                  if(indicatorConfigList.nonEmpty){
                    val collectElem = (indicatorConfigList, ListBuffer(rawData))
                    out.collect(collectElem)
                  }
                } else {
                  logger.warn(s"indicatorIds is Empty: " + indicatorIds)
                }
              }
            }

            if(cycleMap.nonEmpty) {
              for ((k, v) <- cycleMap) {
                val indicatorId = k.split("\\|").head.toLong
                if (indicatorMap.contains(indicatorId)) {
                  val indicatorConfigScala = indicatorMap(indicatorId)

                  val x = scala.collection.immutable.ListMap(v.toSeq.sortBy(_._1): _*).values.to[ListBuffer]
                  val collectElem = (ListBuffer((parseAlgo(indicatorConfigScala), indicatorConfigScala)), x)

                  out.collect(collectElem)

                } else {
                  logger.warn(s"cycleMap not contains  $indicatorId")
                }
              }
            }
            val recordEndTime = System.currentTimeMillis()
            if(recordEndTime - recordStarTiIme > 5000){
              logger.warn(s"message: ${myDatas.toolName}|${myDatas.chamberName} \t total time: ${recordEndTime - recordStarTiIme}")
            }
          } catch {
            case ex: Exception => logger.warn(ErrorCode("003008d011D", System.currentTimeMillis(),
              Map("msg" -> "indicator和配置匹配过程失败"), s"indicator job error $ex").toJson)
          }
//        case _ => logger.warn(s"indicator job Kafka no mach type")
//      }
    }catch {
      case ex: Exception => logger.warn(s"processElement_error: $ex")
    }
  }

  override def processBroadcastElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String,
    fdcWindowData, JsonNode, (ListBuffer[(ALGO, IndicatorConfig)], ListBuffer[RawData])]#Context,
                                       out: Collector[(ListBuffer[(ALGO, IndicatorConfig)], ListBuffer[RawData])]): Unit = {
    try {
      val contextConfig = toBean[ConfigData[IndicatorConfig]](value)

      contextConfig.`dataType` match {

        case "indicatorconfig" => addIndicatorConfigToTCSP(contextConfig)

        case _ => logger.warn(s"indicator job Hbase no mach type: " + contextConfig)
      }
    }catch {
      case ex: Exception => logger.warn(ErrorCode("003008d001D", System.currentTimeMillis(),
        Map("msg" -> "indicator配置加载失败", "record" -> value), ExceptionInfo.getExceptionInfo(ex)).toJson)
    }
  }


  def addIndicatorConfigToTCSP(kafkaConfigData: ConfigData[IndicatorConfig]): Unit = {
    try {
      val indicatorConfig = kafkaConfigData.datas
      val key = indicatorConfig.controlPlanId.toString

      val version = indicatorConfig.controlPlanVersion.toString

      if (!indicatorConfig.algoType.equals("1")) {
        return
      }

      logger.warn(s"addToolWindowConfig_step1: " + kafkaConfigData)

      val indicatorId = indicatorConfig.indicatorId.toLong

      if (!kafkaConfigData.status) {

        //删除indicatorConfig逻辑
        if (this.indicatorConfigByAll.contains(key)) {

          //有一样的key
          val versionAndIndicator = this.indicatorConfigByAll(key)

          if (versionAndIndicator.contains(version)) {
            val algoesAndIndicator = versionAndIndicator(version)

            algoesAndIndicator.remove(indicatorId)
            versionAndIndicator += (version -> algoesAndIndicator)

            this.indicatorConfigByAll += (key -> versionAndIndicator)
          }

          if (versionAndIndicator.isEmpty) {
            this.indicatorConfigByAll.remove(key)
          }
        }
      } else {
        //新增逻辑
        if (this.indicatorConfigByAll.contains(key)) {
          val versionAndIndicator = this.indicatorConfigByAll(key)

          if (versionAndIndicator.contains(version)) {
            val algoMap = versionAndIndicator(version)
            algoMap += (indicatorId -> indicatorConfig)
            versionAndIndicator += (version -> algoMap)
          } else {
            val algoToScala = concurrent.TrieMap[Long, IndicatorConfig](indicatorId -> indicatorConfig)
            versionAndIndicator += (version -> algoToScala)
          }

          // 更新版本
          val k = versionAndIndicator.keys.map(_.toLong)
          if (k.size > 2) {
            val minVersion = k.toList.min
            versionAndIndicator.remove(minVersion.toString)
            logger.warn(s"addToolWindowConfig_step4: " + minVersion + "\tindicatorConfig: " + indicatorConfig)
          }

          this.indicatorConfigByAll.put(key, versionAndIndicator)
        } else {
          //没有一样的key
          val algoToScala = concurrent.TrieMap[Long, IndicatorConfig](indicatorId -> indicatorConfig)
          val versionScala = concurrent.TrieMap[String, concurrent.TrieMap[Long, IndicatorConfig]](version -> algoToScala)
          this.indicatorConfigByAll.put(key, versionScala)
        }
      }
    } catch {
      case ex: Exception => logger.warn(s"addIndicatorConfigToTCSP_error: $ex")
    }
  }


  /**
   * 解析rawdata
   */
  def parseRawData(myDatas: windowListData, listdata: List[sensorDataList], windowDataScala: WindowData, cycleIndex: String): RawData = {
    val product = myDatas.productName
    val stage = myDatas.stage
    val rawData = RawData(
      myDatas.locationId
      , myDatas.locationName
      , myDatas.moduleId
      , myDatas.moduleName
      , myDatas.toolGroupId
      , myDatas.toolGroupName
      , myDatas.chamberGroupId
      , myDatas.chamberGroupName
      , myDatas.recipeGroupName
      , myDatas.limitStatus
      , myDatas.toolName
      , myDatas.toolId
      , myDatas.chamberName
      , myDatas.chamberId
      , myDatas.recipeName
      , myDatas.recipeId
      , myDatas.contextId
      , product
      , stage
      , listdata
      , myDatas.runId
      , myDatas.runStartTime
      , myDatas.runEndTime
      , windowStartTime = windowDataScala.startTime
      , windowEndTime = windowDataScala.stopTime
      , myDatas.windowEndDataCreateTime
      , myDatas.dataMissingRatio
      , myDatas.controlWindowId
      , myDatas.materialName
      , pmStatus = myDatas.pmStatus
      , pmTimestamp = myDatas.pmTimestamp
      , area = if( myDatas.area != null) myDatas.area else ""
      , section = if( myDatas.section != null) myDatas.section else ""
      , mesChamberName = if( myDatas.mesChamberName != null) myDatas.mesChamberName else ""
      , lotMESInfo = myDatas.lotMESInfo
      , unit = windowDataScala.unit
      , dataVersion = myDatas.dataVersion
      , cycleIndex
    )
    rawData
  }

  def parseAlgo(indicatorConfig: IndicatorConfig): ALGO = {
    var algo: ALGO = null
    try {
      algo = ALGO.withName(indicatorConfig.algoClass)
    } catch {
      case ex: NoSuchElementException =>
        algo = ALGO.UNKNOWN
        logger.warn(s" ALGO error indicatorConfig.ALGO_CLASS")
    }
    algo
  }

  /**
   * 获取key
   */
  def generateKey(toolGroup: String, chamberGroupId: String, sensorAliasId: String, subRecipelistId: String): String = {
    s"$toolGroup#$chamberGroupId#$sensorAliasId#$subRecipelistId"
  }

  /**
   * 获取key
   */
  def mapData(toolGroup: String, chamberGroupId: String, sensorAliasId: String, subRecipelistId: String): String = {
    s"$toolGroup#$chamberGroupId#$sensorAliasId#$subRecipelistId"
  }

  /**
   * 获取indicatorkey
   */
  def indicatorKey(toolid: String, chamberid: String, recipeName: String, productName: String, stage: String): String = {
    s"$toolid#$chamberid#$recipeName#$productName#$stage"
  }
}

