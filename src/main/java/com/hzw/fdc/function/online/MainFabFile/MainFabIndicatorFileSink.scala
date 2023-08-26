package com.hzw.fdc.function.online.MainFabFile


import java.text.SimpleDateFormat
import com.hzw.fdc.json.MarshallableImplicits._
import com.hzw.fdc.scalabean.{ErrorCode, IndicatorResultFileScala}
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

class MainFabIndicatorFileSink extends RichSinkFunction[IndicatorResultFileScala] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabIndicatorFileSink])

  var conf: org.apache.hadoop.conf.Configuration = _
  var fs: FileSystem = _
  var path: String = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)


    conf = new org.apache.hadoop.conf.Configuration()
    // 禁用 hdfs 的fs等缓存
    conf.set("fs.defaultFS", ProjectConfig.FS_URL)
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    fs = FileSystem.get(conf)

    path = ProjectConfig.INDICATOR_FILE_PATH
    val pathDir = new Path(path)

    if(!fs.exists(pathDir)){
      fs.mkdirs(pathDir)
    }
  }


  override def invoke(value: IndicatorResultFileScala, context: SinkFunction.Context): Unit = {
    try {
      val nt = new SimpleDateFormat("yyyyMMdd")
      val nTime = nt.format(System.currentTimeMillis())

//      val nowPath = path + "/" + nTime

      /**
       * 新需求: 写入hdfs的12个目录, 增加并发
       **/
      val i = scala.util.Random.nextInt(ProjectConfig.FILE_PATH_NUM) + 1

      val nowPath = path + "/" + nTime + "/" + s"${i}"
      val nowPathDir = new Path(nowPath)

      if(!fs.exists(nowPathDir)){
        fs.mkdirs(nowPathDir)
      }

      val toolName = value.toolName
      val chamberName = value.chamberName

      val ft = new SimpleDateFormat("yyyyMMddHHmmssSSS")
      val ftime = ft.format(value.runStartTime)
      // 文件名
      val fileName = "HFDC_" + toolName + "_" + chamberName + "_" + ftime + ".fdc"
      val filePath = nowPathDir + "/" + fileName

      val nowTime = System.currentTimeMillis()
      val nfm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      val nowTimeStr = nfm.format(nowTime)

      var stage = ""
      try{
        if(value.lotMESInfo.head.nonEmpty) {
          stage = value.lotMESInfo.head.get.stage.get
        }
      }catch {
        case ex: Exception => None
      }


      val IND = "IND_%s_%s_%s_%s".format(stage, value.toolGroupName, value.chamberGroupName, value.recipeName)

      //      val NA = "NA"
      // 输出内容
      var content = "<data-file>\n\n"
      content += "API Version\t3.4\tFDC DATA\tCurrent Time\t" + nowTimeStr + "\n\n"
      content += "<header-section>\n"
      content += "ProgClass\t24\n"
      content += "ProgGroup\tYA-FDC\n"
      content += "ProgName\t%s\n".format(IND)
      content += "Equipment\t%s\n".format(value.toolName)
      content += "Equipment Type\t%s\n".format(value.toolGroupName)
      content += "Material Name\t%s\n".format(value.materialName)
      content += s"Recipe\t%s\n".format(value.recipeName)
      content += s"Physical Recipe\tN.A.\n"
      content += s"Logical Recipe\t%s\n".format(value.recipeGroupName)

      val startTimeStr = nfm.format(value.runStartTime)
      val endTimeStr = nfm.format(value.runEndTime)

      content += "StartTime\t%s\n".format(startTimeStr)
      content += "EndTime\t%s\n".format(endTimeStr)
      content += "FPVersion\t%s\n".format(value.controlPlanVersion)
      content += s"Strategy\t%s\n".format(value.controlPlanName)
      content += "StrategyUUID\tN.A.\n"
      content += "Model\tN.A.\n"
      content += "FdcContextId\t%s\n".format(value.runId)
      content += "DcContextId\t%s\n".format(value.runId)
      content += s"Chamber\t%s\n".format(value.chamberName)
      content += "FilterPlanKey\t0\n"


      content += "AggregationPlanKey\tN.A.\n"
      content += "ControlJobId\tN.A.\n"
      content += "ProcessJobId\tN.A.\n"
      content += "ProcessJobName\tN.A.\n"
      content += s"EquipmentId\t%s\n".format(value.toolId)
      content += s"ChamberGroup\t%s\n".format(value.chamberGroupName)
      content += s"ChamberId\t%s\n".format(value.chamberId)
      content += s"RecipeId\t%s\n".format(value.recipeId)
      content += s"ControlPlan\t%s\n".format(value.controlPlanName)
      content += s"CPVersion\t%s\n".format(value.controlPlanVersion)
      content += s"CPKey\t%s\n".format(value.controlPlanId)
      content += s"RunId\t%s\n".format(value.runId)
      content += s"DCQV\t%s\n".format(value.dataMissingRatio.replace("-1.0", "3.33"))

      content += "</header-section>\n\n"
      content += "<wafer-section>\n"
      content += "LOT_NAME\tWAFER_NAME\tAUX1\tAUX2\tTECHNOLOGY\tPRODUCT\tPROCESS\tSTAGE\tSTEP\tSlotNumber\tRoute\tProductFamily\tCarrier\tLotType\tSubRoute\tInputLoadPort\tReticleName\n"
      val lotMESInfo = value.lotMESInfo
      for (lotMes <- lotMESInfo if lotMes.nonEmpty) {
        val wafers = lotMes.get.wafers
        for (wafer <- wafers) {
          content += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n".
            format(lotMes.get.lotName.get, wafer.get.waferName.get, "N.A.", "N.A.", lotMes.get.technology.get, lotMes.get.product.get, "N.A.", lotMes.get.stage.get,
              "N.A.", "N.A.", lotMes.get.route.get, "N.A.", lotMes.get.carrier.get, lotMes.get.lotType.get, "N.A.", "N.A.", "N.A.")
        }
      }
      content += "</wafer-section>\n\n"
      content += "<indicator-section>\n"
      content += "INDICATOR_NAME\tVALUE\tLSL\tHSL\tLPL\tHPL\tLOL\tHOL\tIndicatorId\tAlarmLevel\n"


      for (indicatorScala <- value.indicatorList) {
        var LCL = "N.A."
        var UCL = "N.A."
        var LBL = "N.A."
        var UBL = "N.A."
        var LSL = "N.A."
        var USL = "N.A."
        try{
          val limit = indicatorScala.limit.replace("N", "N.A.").split("/", 6)
          LCL = if(limit(0) == ""){"N.A."} else limit(0)
          UCL = if(limit(1) == ""){"N.A."} else limit(1)
          LBL = if(limit(2) == ""){"N.A."} else limit(2)
          UBL = if(limit(3) == ""){"N.A."} else limit(3)
          LSL = if(limit(4) == ""){"N.A."} else limit(4)
          USL = if(limit(5) == ""){"N.A."} else limit(5)
        }catch {
          case ex: Exception => logger.warn(("011008d001C", System.currentTimeMillis(),
            Map("msg" -> "limit indicator error !!", "indicatorScala" -> indicatorScala), ex.toString).toJson)
        }

        content += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n".format(indicatorScala.indicatorName, indicatorScala.indicatorValue,
          LSL, USL, LBL, UBL, LCL, UCL, indicatorScala.indicatorId, indicatorScala.alarmLevel)
      }

      content += "</indicator-section>\n\n"
      content += "</data-file>\n"

      writeFile(fs, filePath, content)
    }catch {
      case e: Exception => logger.warn(ErrorCode("011007d001C", System.currentTimeMillis(),
        Map("msg" -> "write indicator file error !!", "value" -> value), ExceptionInfo.getExceptionInfo(e)).toJson)
    }
  }

  override def close(): Unit = {
    try {
      fs.close()
    } catch {
      case e: Exception => None
    }
  }


  /**
   * 写入
   * */
  def writeFile(hdfs: FileSystem, path: String, content: String): Boolean = {
    if(path.isEmpty || content == null) {
      return false
    }

    try {
      val output = hdfs.create(new Path(path))
      output.write(content.getBytes("UTF-8"))
      output.close()
      true
    } catch {
      case e: Exception =>
        logger.error(s"append file exception, path{$path}, content{$content}", e)
        false
    }
  }

}
