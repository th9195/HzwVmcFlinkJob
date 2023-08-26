package com.hzw.fdc.function.online.MainFabVirtualSensor

import com.fasterxml.jackson.databind.JsonNode
import com.greenpineyu.fel.{FelEngine, FelEngineImpl}
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, fromJson, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.util.MainFabConstants.IS_DEBUG
import com.hzw.fdc.scalabean.{ConfigData, ErrorCode, MainFabLogInfo, MainFabPTRawData, PTSensorData, ParamConfig, Point, VirtualSensorAlgoParam, VirtualSensorConfigData}
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.InitFlinkFullConfigHbase.{VirtualSensorDataType, readHbaseAllConfig}
import com.hzw.fdc.util.{ExceptionInfo, InitFlinkFullConfigHbase, MainFabConstants, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent
import scala.collection.mutable.ListBuffer

/**
 * MainFabAddVirtualSensorKeyedBroadcastProcessFunction
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/8 9:11
 * @since 1.0.0
 * @update 2022/11/8 9:11
 * */
class MainFabAddVirtualSensorKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabVirtualSensorKeyedBroadcastProcessFunction])

  // {"toolName + chamberName" :{"svid": "ParamConfig"}}}
  val virtualSensorConfigByAll = new concurrent.TrieMap[String, concurrent.TrieMap[String, ParamConfig]]()

  // {"toolName + chamberName" :{ "paramSvid + vitrualSvid": List[Point}
  val cacheSensorData = new concurrent.TrieMap[String, concurrent.TrieMap[String, List[Point]]]()


  lazy val mainFabLogInfoOutput = new OutputTag[MainFabLogInfo]("mainFabLogInfo")
  val job_fun_DebugCode:String = "022002"
  val jobName:String = "MainFabDataTransformWindowService"
  val optionName : String = "MainFabAddVirtualSensorKeyedBroadcastProcessFunction"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    if(!IS_DEBUG){

      try {
        val initConfig1: ListBuffer[ConfigData[VirtualSensorConfigData]] =
          readHbaseAllConfig[VirtualSensorConfigData](ProjectConfig.HBASE_SYNC_VIRTUAL_SENSOR_TABLE,
            VirtualSensorDataType)

        initConfig1.foreach(addVirtualSensorConfig1)
      }catch {
        case ex: Exception => logger.warn(s"readHbaseAllConfig ${ex.toString}")
      }

      //val initConfig:ListBuffer[ConfigData[VirtualSensorConfigData]] = InitFlinkFullConfigHbase.VirtualSensorConfigList
      val initConfig:ListBuffer[ConfigData[List[VirtualSensorConfigData]]] =
        readHbaseAllConfig[List[VirtualSensorConfigData]](ProjectConfig.HBASE_SYNC_VIRTUAL_SENSOR_TABLE,
          VirtualSensorDataType)

      initConfig.foreach(addVirtualSensorConfig)

      logger.warn("virtualSensorConfigByAll SIZE: " + virtualSensorConfigByAll.size)

    }

  }



//  override def processElement(in1: JsonNode, readOnlyContext: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, collector: Collector[JsonNode]): Unit = ???

  override def processElement(value: JsonNode, ctx: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#ReadOnlyContext, out: Collector[JsonNode]): Unit = {
    try {

      val option = value.path(MainFabConstants.dataType).asText()

      // 只对rawdata类型的数据做处理
      if (option == MainFabConstants.rawData) {

        val ptRawData: MainFabPTRawData =toBean[MainFabPTRawData](value)

        val key = s"${ptRawData.toolName}|${ptRawData.chamberName}"
        if(!virtualSensorConfigByAll.contains(key)){
          out.collect(value)
          return
        }

        val virtualSensorResList: ListBuffer[PTSensorData] = ListBuffer()

        val virtualSensorList = virtualSensorConfigByAll(key)
        var sensorMap: Map[String, PTSensorData] = ptRawData.data.map(x => {
          (x.svid, x)
        }).toMap

        val virtualConfigList = virtualSensorList.values.to[ListBuffer].distinct.sortBy(_.virtualLevel)

        for(virtualConfig <- virtualConfigList){

          val config = virtualConfig
          val vitrualSvid = virtualConfig.virtualSvid
          val virtualSensorAliasName = virtualConfig.virtualSensorAliasName
          try {
            // 根据paramIndex计算参数  排序
            val paramListSort = config.param.sortWith(_.paramIndex < _.paramIndex)

            // calculated计算
            if (config.algoClass == "arithmetic") {
              // 解析配置
              val parseTmp = parseParam(virtualConfig.param)
              val parameter = parseTmp._1
              val operator = parseTmp._2


              val parameter_list = parameter.split(",")
              var isContainAllSensor = true
              for(value <- parameter_list) {
                if (!value.contains("Number(")) {
                  if(!sensorMap.contains(value)){
                    isContainAllSensor = false
                  }
                }
              }

              if(isContainAllSensor) {
                // 基础的计算
                val ptSensor: PTSensorData = baseMath(parameter, operator, sensorMap, virtualSensorAliasName, vitrualSvid)
                if (ptSensor.sensorValue != "error") {
                  virtualSensorResList.append(ptSensor)
                  sensorMap += (vitrualSvid -> ptSensor)
                }
              }
            }

            if(config.algoClass == "customer"){
              var expression = ""
              val sensorSvidConfigList = new ListBuffer[(Int, String, String)]()
              for(param <- paramListSort){
                if(param.paramName == "expression"){
                  expression = param.paramValue.replace("Math.", "$('Math').")
                }

                if(param.paramDataType == "SENSOR_ALIAS_ID"){
                  sensorSvidConfigList.append((param.paramName.length, param.svid, param.paramName))
                }
              }

              var isContainAllSensor = true
              for(elem <- sensorSvidConfigList.sortWith(_._1 > _._1)){
                val paramSvid = elem._2
                val sensorAlias = elem._3
                if(!sensorMap.contains(paramSvid)){
                  isContainAllSensor = false
                }else{
                  val sensorValue = sensorMap(paramSvid).sensorValue.asInstanceOf[Number].doubleValue.toString
                  expression  = expression.replace(s"${sensorAlias}", sensorValue)

//                  expression  = expression.replace(s"${sensorAlias}", sensorMap(paramSvid).sensorValue.toString)
                }
              }

              // 是否包含所有的paramSvid
              if(isContainAllSensor) {
                try {
                  val sensorValue = FelEngine.instance.eval(expression)

                  val ptSensor = PTSensorData(
                    svid = vitrualSvid,
                    sensorName = virtualSensorAliasName,
                    sensorAlias = virtualSensorAliasName,
                    isVirtualSensor = true,
                    sensorValue = sensorValue,
                    unit = Option(""))
                  virtualSensorResList.append(ptSensor)
                  sensorMap += (vitrualSvid -> ptSensor)
                }catch {
                  case exception: Exception => ctx.output(mainFabLogInfoOutput,
                    generateMainFabLogInfo("022004d002C",
                      "virtual sensor customer error",
                      ExceptionInfo.getExceptionInfo(exception),
                      Map[String,Any]("algoClass" -> config.algoClass, "expression" -> expression,
                        "traceId" -> ptRawData.traceId,
                        "timeStamp" -> ptRawData.timestamp, "config" -> config, "sensorMap" -> sensorMap)))
                }
              }
            }


            // abs 求绝对值计算
            if (config.algoClass == "abs") {
              val paramSvid = paramListSort.head.svid

              if (sensorMap.contains(paramSvid)) {
                val ptSensorData = sensorMap(paramSvid)
                val sensorValue =  ptSensorData.sensorValue.toString.toDouble
                val absValue = sensorValue.abs
                val ptSensor = PTSensorData(
                  svid = vitrualSvid,
                  sensorName = virtualSensorAliasName,
                  sensorAlias = virtualSensorAliasName,
                  isVirtualSensor = true,
                  sensorValue = absValue,
                  unit = ptSensorData.unit)
                virtualSensorResList.append(ptSensor)
                sensorMap += (vitrualSvid -> ptSensor)
              }else{
//                logger.warn(s"abs not exist: " + config)
                ctx.output(mainFabLogInfoOutput,
                  generateMainFabLogInfo("0001A",
                    "processElement",
                    "abs not exist",
                    Map[String,Any]("config"->config,"dataInfo"->value)))

              }
            }

            // repout 计算
            if(config.algoClass == "repOut") {
              val paramSvid = paramListSort.head.svid

              if(sensorMap.contains(paramSvid)) {
                val ptSensorData = sensorMap(paramSvid)
                val sensorValue = ptSensorData.sensorValue.asInstanceOf[Number].doubleValue

                // NA 不触发计算
                var upperLimitStatus = true
                val upperLimit = paramListSort(1).paramValue
                if(upperLimit.toString != "NA"){
                  if(sensorValue > upperLimit.toDouble){
                    upperLimitStatus = false
                  }
                }
                var lowerLimitStatus = true
                val lowerLimit = paramListSort(2).paramValue
                if(lowerLimit.toString != "NA"){
                  if(sensorValue < lowerLimit.toDouble){
                    lowerLimitStatus = false
                  }
                }

                if(upperLimitStatus && lowerLimitStatus){
                  val ptSensor = PTSensorData(
                    svid = vitrualSvid,
                    sensorName = virtualSensorAliasName,
                    sensorAlias = virtualSensorAliasName,
                    isVirtualSensor = true,
                    sensorValue = ptSensorData.sensorValue,
                    unit = ptSensorData.unit)
                  virtualSensorResList.append(ptSensor)
                  sensorMap += (vitrualSvid -> ptSensor)
                }
              }
            }

            // movAvg 计算
            if(config.algoClass == "movAvg"){
              val paramSvid = paramListSort.head.svid
              val avgX = paramListSort(1).paramValue.toInt
              val cacheKey = s"${paramSvid}|${vitrualSvid}"

              // 缓存sensorAlias
              if(sensorMap.contains(paramSvid)){
                val ptSensorData = sensorMap(paramSvid)
                val sensorValue = ptSensorData.sensorValue.asInstanceOf[Number].doubleValue
                cacheFunction(paramSvid, avgX, sensorValue, cacheKey, ptRawData.timestamp, key)

                // 开始计算
                val cacheMap = cacheSensorData(key)
                val nowAvgList = cacheMap(cacheKey)
                val max = avgX.toInt
                if(max > nowAvgList.size){
//                  logger.warn(s"AvgFunction AvgXInt > nowAvgList.size config:$config  cacheSensorData:$cacheSensorData")

                  ctx.output(mainFabLogInfoOutput,
                    generateMainFabLogInfo("0002A",
                      "processElement",
                      "AvgFunction AvgXInt > nowAvgList.size",
                      Map[String,Any]("config" -> config,
                        "cacheSensorData" -> cacheSensorData,
                        "dataInfo" -> value)))

                }else{
                  //提取列表的后n个元素
                  val mathList: List[Point] = nowAvgList.takeRight(max)

                  val Result: Double = mathList.map(_.value).sum / max.toDouble
                  //                      logger.warn(s"movAvg mathList:$mathList  \t $cacheKey  \tResult:$Result \t" )
                  val ptSensor = PTSensorData(
                    svid = vitrualSvid,
                    sensorName = virtualSensorAliasName,
                    sensorAlias = virtualSensorAliasName,
                    isVirtualSensor = true,
                    sensorValue = Result,
                    unit = ptSensorData.unit)


                  virtualSensorResList.append(ptSensor)
                  sensorMap += (vitrualSvid -> ptSensor)
                }
              }
            }

            // slope 斜率计算
            if(config.algoClass == "slope") {
              val paramSvid = paramListSort.head.svid
              val avgX = paramListSort(1).paramValue.toInt
              val cacheKey = s"${paramSvid}|${vitrualSvid}"

              // 缓存sensorAlias
              if (sensorMap.contains(paramSvid)) {
                val ptSensorData = sensorMap(paramSvid)
                val sensorValue = ptSensorData.sensorValue.asInstanceOf[Number].doubleValue
                cacheFunction(paramSvid, avgX, sensorValue, cacheKey, ptRawData.timestamp, key)


                // 开始计算
                val cacheMap = cacheSensorData(key)
                val nowAvgList = cacheMap(cacheKey)
                val max = avgX.toInt + 1
                if (max > nowAvgList.size) {
//                  logger.warn(s"AvgFunction AvgXInt > nowAvgList.size config:$config  sensorMap:$sensorMap")
                  ctx.output(mainFabLogInfoOutput,
                    generateMainFabLogInfo("0003A",
                      "processElement",
                      "AvgFunction AvgXInt > nowAvgList.size",
                      Map[String,Any]("config"->config,"sensorMap"->sensorMap,"dataInfo"->value)))

                } else {
                  //提取列表的后n个元素
                  val mathList: List[Point] = nowAvgList.takeRight(max)

                  // 公式: Yn=(Xn-Xn-i)/(Tn-Tn-i)   Tn:秒数时间戳
                  val Result: Double = (mathList.head.value - mathList.last.value) /
                    ((mathList.head.timestamp - mathList.last.timestamp) / 1000.000)

                  val ptSensor = PTSensorData(
                    svid = vitrualSvid,
                    sensorName = virtualSensorAliasName,
                    sensorAlias = virtualSensorAliasName,
                    isVirtualSensor = true,
                    sensorValue = Result,
                    unit = ptSensorData.unit)
                  virtualSensorResList.append(ptSensor)
                  sensorMap += (vitrualSvid -> ptSensor)
                }
              }
            }
          }catch {
            case exception: Exception => ctx.output(mainFabLogInfoOutput,
              generateMainFabLogInfo("022002d001C",
                "virtual sensor math error",
                ExceptionInfo.getExceptionInfo(exception),
                Map[String,Any]("algoClass" -> config.algoClass, "traceId" -> ptRawData.traceId,
                  "timeStamp" -> ptRawData.timestamp, "config" -> config, "sensorMap" -> sensorMap)))

//            case exception: Exception =>logger.error(ErrorCode("022002d001C", System.currentTimeMillis(),
//              Map("algoClass" -> config.algoClass, "traceId" -> ptRawData.traceId, "timeStamp" -> ptRawData.timestamp,
//                "config" -> config, "sensorMap" -> sensorMap), ExceptionInfo.getExceptionInfo(exception)).toJson)
            //out.collect(value)
          }
        }

        // 添加虚拟sensor
        if(virtualSensorResList.isEmpty){
          //            logger.warn("virtualSensorResList_step1: " + ptRawData.toolName)
          out.collect(value)
        }else{
          //            logger.warn("virtualSensorResList_step2: " + virtualSensorResList + "toolName: " + ptRawData.toolName)

          ptRawData.data ++= virtualSensorResList.filter(_.svid != null)
          val node = beanToJsonNode[MainFabPTRawData](ptRawData)
          out.collect(node)
        }
      }else{
        //删除缓存  修改bug: FAB3-1086 【home】在线虚拟sensor计算mov有误，当前mov的算法是以连续的， 不是按run
        val toolName = value.path("toolName").asText()
        val chamberName = value.path("chamberName").asText()
        val key = s"${toolName}|${chamberName}"
        cacheSensorData.remove(key)

        out.collect(value)
      }
    } catch {
      case exception: Exception =>logger.error(ErrorCode("022002d002C", System.currentTimeMillis(), Map(), exception.toString).toJson)
        out.collect(value)
    }
  }


  override def processBroadcastElement(value: JsonNode, context: KeyedBroadcastProcessFunction[String, JsonNode, JsonNode, JsonNode]#Context, out: Collector[JsonNode]): Unit = {
    try {
      val config = toBean[ConfigData[List[VirtualSensorConfigData]]](value)

      addVirtualSensorConfig(config)
    } catch {
      case e: Exception => logger.error(ErrorCode("022002d003C", System.currentTimeMillis(), Map("processBroadcastElement" -> value), e.toString).toJson)
    }

    try {
      val config = toBean[ConfigData[VirtualSensorConfigData]](value)

      addVirtualSensorConfig1(config)
    } catch {
      case e: Exception => logger.error(ErrorCode("022002d003C", System.currentTimeMillis(), Map("processBroadcastElement" -> value), e.toString).toJson)
    }
  }


  /**
   * 缓存sensorAlias
   */
  def cacheFunction(sensorAlias: String, avgX: Int, sensorValue: Double, cacheKey: String, timestamp:Long,
                    key:String): Unit = {
    if (cacheSensorData.contains(key)) {
      val cacheMap = cacheSensorData(key)

      if(cacheMap.contains(cacheKey)){
        val dataList: List[Point] = cacheMap(cacheKey)
        val newList = dataList :+ Point(timestamp, sensorValue)
        val dropList = if (newList.size > avgX + 1) {
          newList.drop(1)
        } else {
          newList
        }
        cacheMap.put(cacheKey, dropList.distinct)
      }else{
        cacheMap.put(cacheKey, List(Point(timestamp, sensorValue)))
      }

      cacheSensorData.put(key, cacheMap)
    } else {
      val cacheScala = concurrent.TrieMap[String, List[Point]](cacheKey -> List(Point(timestamp, sensorValue)))
      cacheSensorData.put(key, cacheScala)
    }
  }



  def parseParam(paramList: List[VirtualSensorAlgoParam]): (String, String) = {
    //根据paramIndex排序
    val paramListSort = paramList.sortWith(_.paramIndex < _.paramIndex)
    var parameter = ""
    var operator = ""
    for (param <- paramListSort){
      if(param.paramDataType == "OPERATOR"){
        if(operator.isEmpty)
          operator = param.paramValue
        else
          operator = operator + "," + param.paramValue
      }

      if(param.paramDataType == "SENSOR_ALIAS_ID"){
        if(parameter.isEmpty)
          parameter = param.svid
        else
          parameter = parameter + "," + param.svid
      }

      if(param.paramDataType == "NUMERIC" || param.paramDataType == "FLOAT"){
        if(parameter.isEmpty)
          parameter = "Number(" + param.paramValue + ")"
        else
          parameter = parameter + ",Number(" + param.paramValue + ")"
      }
    }
    (parameter, operator)
  }


  /**
   *  arithmetic的参数计算
   */
  def baseMath(parameter: String, operator: String, sensorAliasDataMap: Map[String, PTSensorData],
               virtualSensorAliasName: String, svid: String): PTSensorData = {
    var res_indicator_value = ""
    var sensorName = ""
    var unit = ""
    try {
      var indicator_value: scala.math.BigDecimal = 0.0
      var parameter1: scala.math.BigDecimal = 0.00
      var parameter2: scala.math.BigDecimal= 0.00
      val operator_list = operator.split(",")
      val parameter_list = parameter.split(",")

      def parameter_value(value: String): String = {
        if(value.contains("Number(")){
          value.replace("Number(", "").replace(")", "")
        }else{


          sensorName= sensorAliasDataMap(value).sensorName
          unit =  sensorAliasDataMap(value).unit.getOrElse("")
          sensorAliasDataMap(value).sensorValue.toString
        }
      }

      for (i <- 1 until parameter_list.length) {

        if (i == 1) {
          parameter1 = BigDecimal(parameter_value(parameter_list(0)))
          parameter2 = BigDecimal(parameter_value(parameter_list(1)))
        } else {
          parameter1 = indicator_value
          parameter2 = BigDecimal(parameter_value(parameter_list(i)))
        }
        // 匹配计算 +-*/
        indicator_value = operator_list(i-1) match {
          case "+" => parameter1 + parameter2
          case "-" => parameter1 - parameter2
          case "*" => parameter1 * parameter2
          case "/" => if (parameter1 != 0) parameter1 / parameter2 else 0
        }
      }
      res_indicator_value = indicator_value.toString
    }catch {
      case ex: Exception => logger.error(ErrorCode("022002d004C", System.currentTimeMillis(), Map("function"->"baseMath"), ex.toString).toJson)
        res_indicator_value = "error"
    }

    PTSensorData(
      svid = svid,
      sensorName = virtualSensorAliasName,
      sensorAlias = virtualSensorAliasName,
      isVirtualSensor = true,
      sensorValue = res_indicator_value,
      unit = Option(""))
  }


  def addVirtualSensorConfig(config: ConfigData[List[VirtualSensorConfigData]]): Unit = {
    try {
      println(s"${DateTimeUtil.getCurrentTime()} addVirtualSensorConfig_step1: " + config)

      // 第一步: 先删除全量的 虚拟sensor数据
      val virtualConfigList = config.datas
      for(virtualConfig <- virtualConfigList){
        val toolMsgList = virtualConfig.toolMsgList
        for (elem <- toolMsgList) {
          val key = s"${elem.toolName}|${elem.chamberName}"
          this.virtualSensorConfigByAll.remove(key)
        }
      }

      // 第二步: 添加
      for(virtualConfig <- virtualConfigList) {

        val toolMsgList = virtualConfig.toolMsgList


        for (toolMsg <- toolMsgList) {

          val key = s"${toolMsg.toolName}|${toolMsg.chamberName}"
          val svid = virtualConfig.svid

          val paramConfig = ParamConfig(
            virtualConfig.svid,
            virtualConfig.virtualSensorAliasName,
            virtualConfig.virtualLevel,
            virtualConfig.algoName,
            virtualConfig.algoClass,
            virtualConfig.param
          )

          if (!config.status) {
            // 删除indicatorConfig逻辑
            if (this.virtualSensorConfigByAll.contains(key)) {

              //有一样的key
              val virtualSensorMap = this.virtualSensorConfigByAll(key)

              if (virtualSensorMap.contains(svid)) {
                virtualSensorMap.remove(svid)
                this.virtualSensorConfigByAll.put(key, virtualSensorMap)
              } else {
                logger.warn(s"CalculatedIndicator Config version no exist: " + key)
              }
            }
          } else {
            // 新增逻辑
            if (this.virtualSensorConfigByAll.contains(key)) {
              val virtualSensorMap = this.virtualSensorConfigByAll(key)

              virtualSensorMap += (svid -> paramConfig)
              this.virtualSensorConfigByAll.put(key, virtualSensorMap)

            } else {
              val virtualSensorScala = concurrent.TrieMap[String, ParamConfig](svid -> paramConfig)
              this.virtualSensorConfigByAll.put(key, virtualSensorScala)
            }
          }
        }
      }
    }catch {
      case ex: Exception => logger.error(ErrorCode("022002d005C", System.currentTimeMillis(),
        Map("function"->"addVirtualSensorConfig", "message" -> config), ex.toString).toJson)
    }
  }


  def addVirtualSensorConfig1(config: ConfigData[VirtualSensorConfigData]): Unit = {
    try {
      val virtualConfig = config.datas

      val toolMsgList = virtualConfig.toolMsgList

      for (toolMsg <- toolMsgList) {
        val key = s"${toolMsg.toolName}|${toolMsg.chamberName}"
        val svid = virtualConfig.svid


        val paramConfig = ParamConfig(
          virtualConfig.svid,
          virtualConfig.virtualSensorAliasName,
          virtualConfig.virtualLevel,
          virtualConfig.algoName,
          virtualConfig.algoClass,
          virtualConfig.param
        )

        if (!config.status) {
          // 删除indicatorConfig逻辑
          if (this.virtualSensorConfigByAll.contains(key)) {


            //有一样的key
            val virtualSensorMap = this.virtualSensorConfigByAll(key)


            if (virtualSensorMap.contains(svid)) {
              virtualSensorMap.remove(svid)
              this.virtualSensorConfigByAll.put(key, virtualSensorMap)
            } else {
              logger.warn(s"CalculatedIndicator Config version no exist: " + key)
            }
          }
        } else {
          // 新增逻辑
          if (this.virtualSensorConfigByAll.contains(key)) {
            val virtualSensorMap = this.virtualSensorConfigByAll(key)


            virtualSensorMap += (svid -> paramConfig)
            this.virtualSensorConfigByAll.put(key, virtualSensorMap)


          } else {
            val virtualSensorScala = concurrent.TrieMap[String, ParamConfig](svid -> paramConfig)
            this.virtualSensorConfigByAll.put(key, virtualSensorScala)
          }
        }
      }
    } catch {
      case ex: Exception => logger.error(ErrorCode("022002d005C", System.currentTimeMillis(),
        Map("function" -> "addVirtualSensorConfig1", "message" -> config), ex.toString).toJson)
    }
  }



  /**
   * 生成日志信息
   * @param debugCode
   * @param message
   * @param paramsInfo
   * @param dataInfo
   * @param exception
   * @return
   */
  def generateMainFabLogInfo(debugCode:String,funName:String,message:String,paramsInfo:Map[String,Any]=null,dataInfo:String="",exception:String = ""): MainFabLogInfo = {
    MainFabLogInfo(mainFabDebugCode = job_fun_DebugCode + debugCode,
      jobName = jobName,
      optionName = optionName,
      functionName = funName,
      logTimestamp = DateTimeUtil.getCurrentTimestamp,
      logTime = DateTimeUtil.getCurrentTime(),
      message = message,
      paramsInfo = paramsInfo,
      dataInfo = dataInfo,
      exception = exception)
  }
}
