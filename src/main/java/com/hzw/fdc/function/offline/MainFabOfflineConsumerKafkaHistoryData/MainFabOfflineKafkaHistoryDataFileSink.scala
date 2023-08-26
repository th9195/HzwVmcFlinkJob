package com.hzw.fdc.function.offline.MainFabOfflineConsumerKafkaHistoryData

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat

/**
 * MainFabKafkaHistoryDataFileSink
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/24 13:16
 * @since 1.0.0
 * @update 2022/11/24 13:16
 * */
class MainFabOfflineKafkaHistoryDataFileSink extends RichSinkFunction[JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineKafkaHistoryDataFileSink])

  private var fileSystem : FileSystem = _
  private var path : Path = _
  private var filePath : String = _
  private var writer : FSDataOutputStream = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)


    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("fs.defaultFS", ProjectConfig.FS_URL)
    fileSystem = FileSystem.get(conf)

    val dirPath = ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_FILE_PATH + ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_TOPIC
    path = new Path(dirPath)

    if(!fileSystem.exists(path)){
      fileSystem.mkdirs(path)
    }

    val dateTime = DateTimeUtil.getCurrentTime("yyyyMMddHHmmss")
    filePath = dirPath + "/" + dateTime  // fileName == /kafkaHistoryData/topicName/时间戳

    writer = fileSystem.create(new Path(filePath))
  }

  override def invoke(value: JsonNode, context: SinkFunction.Context): Unit = {
    logger.warn(s"value == ${value}")
    writeFile(value.toString)
  }


  /**
   * 写文件
   * @param message
   */
  def writeFile(message: String) = {

    if(StringUtils.isNotEmpty(message)){
      try {
        writer.write(message.getBytes("UTF-8"))
      } catch {
        case ex: Exception =>
          logger.error(s"write file exception; path{$filePath}, content{$message} ; ex == ${ExceptionInfo.getExceptionInfo(ex).toJson}")
      }
    }else{
      logger.warn(s"message is null!")
    }

  }


  override def close(): Unit = {
    try {

      if(null != writer){
        writer.close()
      }
      if(null != fileSystem){
        fileSystem.close()
      }
    } catch {
      case ex: Exception => {
        logger.error(s"ex == ${ExceptionInfo.getExceptionInfo(ex).toJson}")
      }
    }
  }

}
