package com.hzw.fdc.controller

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.offline.MainFabOfflineConsumerKafkaHistoryDataService
import com.hzw.fdc.service.online.MainFabWindowService

/**
 * MainFabOfflineConsumerKafkaHistoryDataController
 *
 * @desc:
 * @author tobytang
 * @date 2022/11/24 10:23
 * @since 1.0.0
 * @update 2022/11/24 10:23
 * */
class MainFabOfflineConsumerKafkaHistoryDataController extends TController{
  private val service = new MainFabOfflineConsumerKafkaHistoryDataService

  override def execute(): Unit = service.analyses()


}
