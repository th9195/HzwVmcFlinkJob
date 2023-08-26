package com.hzw.fdc.function.online.MainFabFile

import java.text.SimpleDateFormat

import com.hzw.fdc.scalabean.RawTrace
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

class MainFabRawTraceFileSink extends RichSinkFunction[(List[String], String, String, Long)] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabRawTraceFileSink])

  private var conf: org.apache.hadoop.conf.Configuration = _
  private var fs: FileSystem = _
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

    path = ProjectConfig.RAWTRACE_FILE_PATH
    val pathDir = new Path(path)

    if(!fs.exists(pathDir)){
      fs.mkdirs(pathDir)
    }
  }


  override def invoke(value: (List[String], String, String, Long), context: SinkFunction.Context): Unit = {
    try {
      val content = value._1
      val toolName = value._2
      val chamberName = value._3
      val runStartTime = value._4
      val nt = new SimpleDateFormat("yyyyMMdd")
      val nTime = nt.format(System.currentTimeMillis())

      /**
       * 新需求: 写入hdfs的12个目录, 增加并发
       **/
      val i = scala.util.Random.nextInt(ProjectConfig.FILE_PATH_NUM) + 1

      val nowPath = path + "/" + nTime + "/" + s"${i}"
      val nowPathDir = new Path(nowPath)

      if(!fs.exists(nowPathDir)){
        fs.mkdirs(nowPathDir)
      }


      val ft = new SimpleDateFormat("yyyyMMddHHmmssSSS")
      val ftime = ft.format(runStartTime)
      // 文件名
      val runFileName = toolName + "_" + chamberName + "_" + ftime + ".exntrace.running"
      val runFilePath = nowPathDir + "/" + runFileName

      val finishFileName = toolName + "_" + chamberName + "_" + ftime + ".exntrace"
      val finishFilePath = nowPathDir + "/" + finishFileName

      val runId = toolName + "--" + chamberName + "--" + runStartTime

      if(!fs.exists(new Path(finishFilePath))) {
        writeFile(fs, runFilePath, content)

        // 修改文件名称格式, 代表已经写入成功
        fs.rename(new Path(runFilePath), new Path(finishFilePath))
      }

    }catch {
      case e: Exception => logger.warn(s"write rawTrace file error $e")
    }
  }


  //  override def invoke(value: RawTrace, context: SinkFunction.Context): Unit = {
  //    try {
  //      val nt = new SimpleDateFormat("yyyyMMdd")
  //      val nTime = nt.format(System.currentTimeMillis())
  //
  //      val nowPath = path + "/" + nTime
  //      val nowPathDir = new Path(nowPath)
  //
  //      if(!fs.exists(nowPathDir)){
  //        fs.mkdirs(nowPathDir)
  //      }
  //
  //      val toolName = value.toolName
  //      val chamberName = value.chamberName
  //      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  //      val timestampStr = fm.format(value.runEndTime).replace(" ", "T")
  //
  //      val ft = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  //      val ftime = ft.format(value.runStartTime)
  //      // 文件名
  //      val runFileName = toolName + "_" + chamberName + "_" + ftime + ".exntrace.running"
  //      val runFilePath = nowPathDir + "/" + runFileName
  //
  //      val finishFileName = toolName + "_" + chamberName + "_" + ftime + ".exntrace"
  //      val finishFilePath = nowPathDir + "/" + finishFileName
  //
  //      val runId = toolName + "--" + chamberName + "--" + value.runStartTime
  //
  //      val NA = "N.A."
  //      // 输出内容
  //      var content = "<ProcessContext>\n  <Location>\n"
  //      content += "    <Factory name=\"%s\"/>\n".format(value.locationName)
  //      content += "    <Line/>\n"
  //      content += "    <Area name=\"%s\"/>\n".format(value.moduleName)
  //      content += "    <Department/>\n"
  //      content += "    <EquipmentType name=\"%s\"/>\n".format(value.toolGroupName)
  //      content += "    <Equipment name=\"%s\"/>\n".format(value.toolName)
  //      content += "    <ModuleType name=\"%s\"/>\n".format(value.chamberGroupName)
  //      content += "    <Module name=\"%s\"/>\n".format(value.chamberName)
  //      content += s"    <SubSystems/>\n"
  //      content += s"  </Location>\n"
  //
  //      content += s"  <Process>\n"
  //      content += "    <Recipe name=\"%s\"/>\n".format(value.recipeName)
  //      content += "    <RecipeGroup name=\"%s\"/>\n".format(value.recipeGroupName)
  //      content += "    <ControlJobName/>\n"
  //      content += "    <ProcessJobName name=\"%s\"/>\n".format(NA)
  //      content += s"    <ProcessTags>\n"
  //      content += "      <Tag name=\"EquipmentID\" value=\"" + value.toolId + "\"/>\n"
  //      content += "      <Tag name=\"ModuleID\" value=\"" + value.chamberId + "\"/>\n"
  //      content += "      <Tag name=\"RecipeID\" value=\"" + value.recipeId + "\"/>\n"
  //      content += "      <Tag name=\"DCContextID\" value=\"" + runId + "\"/>\n"
  //      val startTime = fm.format(value.runStartTime).replace(" ", "T")
  //      content += s"    </ProcessTags>\n"
  //      content += "    <ProcessTime start=\"" + startTime + "\" stop=\"" + timestampStr + "\" DCQV=\"%s\"/>\n".format(value.dataMissingRatio.replace("-1.0", "3.33"))
  //      content += s"  </Process>\n"
  //
  //
  //      content += "  <Material materialName=\"%s\">\n".format(value.materialName)
  //      val lotMESInfo = value.lotMESInfo
  //      for (lotMes <- lotMESInfo if lotMes.nonEmpty) {
  //
  //        content += "    <Lot name=\"%s\" carrier=\"%s\" layer=\"%s\" operation=\"%s\" product=\"%s\" route=\"%s\" stage=\"%s\" technology=\"%s\" type=\"%s\">\n".
  //          format(lotMes.get.lotName.get, lotMes.get.carrier.get, lotMes.get.layer.get, lotMes.get.operation.get, lotMes.get.product.get, lotMes.get.route.get, lotMes.get.stage.get, lotMes.get.technology.get, lotMes.get.lotType.get)
  //        content += s"      <LotProcessTags/>\n"
  //        content += s"      <LotTags/>\n"
  //        content += s"      <Wafers>\n"
  //        val wafers = lotMes.get.wafers
  //        for (wafer <- wafers) {
  //          content += "        <Wafer name=\"%s\" carrierSlot=\"%s\">\n".format(wafer.waferName.get, wafer.carrierSlot.get)
  //          content += s"          <WaferTags>\n"
  //          content += "            <Tag name=\"AeWaferKey\" value=\"" + NA + "\"/>\n"
  //          content += s"          </WaferTags>\n"
  //          content += s"          <WaferProcessTags/>\n"
  //          content += s"        </Wafer>\n"
  //        }
  //        content += s"      </Wafers>\n"
  //        content += s"    </Lot>\n"
  //
  //      }
  //
  //      content += s"  </Material>\n"
  //      content += s"</ProcessContext>\n"
  //
  //      var headLine = s"Timestamp\t"
  //      val sensorNameSet = value.ptSensor.flatMap(x => {
  //        x.data.map(_.sensorName)
  //      }).distinct
  //
  //      for (senorName <- sensorNameSet) {
  //        headLine += s"$senorName\t"
  //      }
  //      content += headLine + "\n"
  //
  //      // 按时间排序
  //      val ptSensor = value.ptSensor.sortWith(_.timestamp < _.timestamp)
  //
  //      for (sensor <- ptSensor) {
  //        content += sensor.timestamp + "\t"
  //        val map = sensor.data.map(x => {
  //          (x.sensorName, x.sensorValue)
  //        }).toMap
  //        for (sensorName <- sensorNameSet) {
  //          val sensorValue = if (map.contains(sensorName)) {
  //            map(sensorName)
  //          } else {
  //            s"$NA"
  //          }
  //          content += sensorValue + "\t"
  //        }
  //        content += "\n"
  //      }
  //
  //      writeFile(fs, runFilePath, content)
  //
  //      // 修改文件名称格式, 代表已经写入成功
  //      fs.rename(new Path(runFilePath), new Path(finishFilePath))
  //
  //    }catch {
  //      case e: Exception => logger.warn(s"write rawTrace file error $e")
  //    }
  //  }

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
  def writeFile(hdfs: FileSystem, path: String, contentList: List[String]): Boolean = {
    if(path.isEmpty || contentList.isEmpty) {
      return false
    }

    try {
      val output = hdfs.create(new Path(path))

      contentList.foreach(content => {
        try {
          output.write(content.getBytes("UTF-8"))
        }catch {
          case e: Exception =>
            logger.error(s"append file exception, path{$path}, content{$content}", e)
        }
      })

      output.close()

      true
    } catch {
      case e: Exception =>
        logger.error(s"append file exception, path{$path}, contentList{$contentList}", e)
        false
    }
  }

}
