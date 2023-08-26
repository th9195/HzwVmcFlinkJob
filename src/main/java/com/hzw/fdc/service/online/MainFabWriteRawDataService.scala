package com.hzw.fdc.service.online
import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.MainFabRawDataDao
import com.hzw.fdc.function.online.MainFabRawData.{AsyncRawDataToOpentsdb, RawDataToOpentsdbSink}
import com.hzw.fdc.util.{ExceptionInfo, MainFabConstants, ProjectConfig}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit

/**
 * @author Liuwt
 * @date 2021/11/2111:58
 */
class MainFabWriteRawDataService extends TService{
  private val logger: Logger = LoggerFactory.getLogger(classOf[MainFabWriteRawDataService])

  override def getDao(): TDao = {
    new MainFabRawDataDao()
  }

  override def analyses(): Any = {
    val allData =
      if(ProjectConfig.GET_KAFKA_DATA_BY_TIMESTAMP){
        getDao.getKafkaJsonSourceByTimestamp(
          ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC,
          ProjectConfig.KAFKA_QUORUM,
          ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RAW_DATA_JOB,
          MainFabConstants.latest,
          MainFabConstants.MAIN_FAB_WRITE_RAW_DARA_JOB_KAFKA_SOURCE_UID,
          MainFabConstants.MAIN_FAB_WRITE_RAW_DARA_JOB_KAFKA_SOURCE_UID,
          ProjectConfig.KAFKA_MAINFAB_RAW_DATA_JOB_FROM_TIMESTAMP)
      }else {
        getDao().getKafkaJsonSource(
          ProjectConfig.KAFKA_MAINFAB_RAWDATA_TOPIC,
          ProjectConfig.KAFKA_QUORUM,
          ProjectConfig.KAFKA_CONSUMER_GROUP_WRITE_RAW_DATA_JOB,
          MainFabConstants.latest,
          MainFabConstants.MAIN_FAB_WRITE_RAW_DARA_JOB_KAFKA_SOURCE_UID,
          MainFabConstants.MAIN_FAB_WRITE_RAW_DARA_JOB_KAFKA_SOURCE_UID
        )
      }

   val rawData: DataStream[JsonNode] = allData
     .keyBy(data => {
       try {
         val toolName = data.findPath(MainFabConstants.toolName).asText()
         val chamberName = data.findPath(MainFabConstants.chamberName).asText()
         s"$toolName|$chamberName"
       } catch {
         case e:Exception => logger.warn(s"traceId Exception: ${e.toString}")
           ""
       }
     })
     .filter(data=>
       try {
         data.path(MainFabConstants.dataType).asText().equals(MainFabConstants.rawData)
       } catch {
         case e:Exception => logger.warn(s"rawData Exception:${ExceptionInfo.getExceptionInfo(e)} data: $data")
           false
       }
     )

//    rawData
//      .rebalance
//      .addSink(new RawDataToOpentsdbSink())

    rawData
      .addSink(new AsyncRawDataToOpentsdb())
      .name(MainFabConstants.OpenTSDBSink)
      .uid(MainFabConstants.OpenTSDBSink)

//    AsyncDataStream
//      .unorderedWait(rawData,
//        new AsyncRawDataToOpentsdb(),
//        12000,
//        TimeUnit.MILLISECONDS,
//        100)
//      .name(MainFabConstants.OpenTSDBSink)
//      .uid(MainFabConstants.OpenTSDBSink)
  }
}
