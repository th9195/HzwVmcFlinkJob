package com.hzw.fdc.function.offline.MainFabOfflineConsumerKafkaHistoryData

import com.hzw.fdc.util.ProjectConfig
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks.{break, breakable}

/**
 * MainFabOfflineKafkaHistoryDataFilter
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/6 13:52
 * @since 1.0.0
 * @update 2022/12/6 13:52
 * */
class MainFabOfflineKafkaHistoryDataFilter extends KeyedProcessFunction[String,JsonNode,JsonNode]{
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabOfflineKafkaHistoryDataFilter])

  private var filterInfos:String = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // todo 根据条件过滤
    filterInfos = ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO
      .replace("(","")
      .replace(")","")


    logger.warn(s"time == ${ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP}")
    val time_format = DateTimeUtil.getTimeByTimestamp13(ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP,"yyyy-MM-dd HH:mm:ss")
    logger.warn(s"time_format == ${time_format}")
    logger.warn(s"topic == ${ProjectConfig.CONSUMER_KAFKA_HISTORY_DATA_TOPIC}")
    logger.warn(s"filterInfos == ${filterInfos}")

  }

  override def processElement(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {

    if(StringUtils.isNotEmpty(filterInfos)){
      val filterInfoAndList: List[String] = filterInfos.split("&&").toList

      if(filterAnd(inputValue,filterInfoAndList)){
        logger.warn(s"inputValue == ${inputValue}")
        collector.collect(inputValue)
      }

    }else{
      collector.collect(inputValue)
    }

  }



  /**
   * 过滤  or: ||  的信息
   * 只要满足一个返回true
   * @param inputValue
   * @param filterInfoAnd
   * @return
   */
  def filterOr(inputValue: JsonNode, filterInfoAnd: String): Boolean = {
    var res : Boolean = false
    val filterInfoOrList = filterInfoAnd.split("\\|\\|")
    breakable{
      for(filterInfo <- filterInfoOrList){
        if(inputValue.toString.contains(filterInfo)){
          res = true
          break()
        }
      }
    }
    res
  }

  /**
   * 过滤  and: &&  的信息
   * 必须满足所有的信息
   * @param inputValue
   * @param filterInfoAndList
   * @return
   */
  def filterAnd(inputValue: JsonNode, filterInfoAndList: List[String]): Boolean = {
    var resOut: Boolean = true
    breakable {
      for (filterinfoAnd <- filterInfoAndList) {
        if (!filterOr(inputValue,filterinfoAnd)) {
          resOut = false
          break()
        }
      }
    }
    resOut
  }
}
