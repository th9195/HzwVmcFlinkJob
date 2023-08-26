package com.hzw.fdc.function.offline.MainFabOfflineAutoLimit

import com.hzw.fdc.function.PublicFunction.limitCondition.{ConditionEntity, FieldOrder}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.{AutoLimitIndicatorResult, ErrorCode, OfflineAutoLimitConfig, OfflineAutoLimitControlPlanInfo, OfflineAutoLimitIndicatorConfig, OfflineAutoLimitOneConfig, OfflineAutoLimitResult, OfflineAutoLimitRunData, OfflineCondition, OfflineDeploymentInfo, QueryRunDataConfig, SpecLimit}
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.ProjectConfig
import com.hzw.fdc.util.hbaseUtils.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import java.lang.reflect.Field
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.math.{BigDecimal, pow, sqrt, toRadians}
import scala.util.control.Breaks

/**
 * OfflineAutoLimitUtil
 * 离线计算 AutoLimit 所有的方法 集合
 * @desc:
 * @author tobytang
 * @date 2022/8/23 11:24
 * @since 1.0.0
 * @update 2022/8/23 11:24
 * */
object OfflineAutoLimitUtil {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val indicatorTableName = "mainfab_indicatordata_table"
  val runDataTableName = "mainfab_rundata_table"

  val indicatorColumns = List[String](
    "INDICATOR_ID",
    "INDICATOR_VALUE",
    "TOOL_ID",
    "CHAMBER_ID",
    "RECIPE",
    "PRODUCT_ID",
    "STAGE")

  val runDataColumns = List[String](
    "RUN_START_TIME",
    "RUN_END_TIME",
    "RECIPE",
    "PRODUCT",
    "STAGE")


  val declaredFields = classOf[ConditionEntity].getDeclaredFields.filter(_.isAnnotationPresent(classOf[FieldOrder]))

  val fields = new Array[Field](declaredFields.size)

  declaredFields.foreach(field => {
    val num = field.getAnnotation(classOf[FieldOrder]).value()
    fields(num) = field
  })

  /**
   * 解析捞RunData的配置信息
   * @param offlineAutoLimitConfig
   * @return
   */
  def parseRunDataConfig(offlineAutoLimitConfig: OfflineAutoLimitConfig): QueryRunDataConfig = {
    QueryRunDataConfig(
      controlPlanId = offlineAutoLimitConfig.controlPlanId,
      version = offlineAutoLimitConfig.version,
      toolList = offlineAutoLimitConfig.toolList,
      recipeList = offlineAutoLimitConfig.recipeList,
      controlPlanList = offlineAutoLimitConfig.controlPlanList,
      endTimestamp = offlineAutoLimitConfig.endTimestamp,
      maxRunNumber = offlineAutoLimitConfig.maxRunNumber,
      maxTimeRange = offlineAutoLimitConfig.maxTimeRange
    )
  }


  /**
   * 判断当前run是否属于需要计算 AutoLimit的controlplan
   * 判断思想： 积分算法
   * stage积分 (包含 stage 积2分 ，为空积 0 分  , 不包含 -1)
   * 1- 每个ctrolPlan 都有个score字段， 默认值为 -1;
   * 2- 如果controlPlan 中的stages 为空 score = 0;
   * 3- 如果controlPlan 中的stages 不为空， 并且 与 当前的runData中的stages 有交集 score = 2;
   * 4- 如果controlPlan 中的stages 不为空， 并且 与 当前的runData中的stages 没有交集 score = -1;
   * product 积分 (包含 product 积 1分 ，为空积 0 分  , 不包含 -1)
   * 1- 如果controlPlan 中的products 为空 score = score + 0;
   * 2- 如果controlPlan 中的products 不为空 并且 与 当前的runData中的products 有交集 score = score + 1;
   * 3- 如果controlPlan 中的products 不为空 并且 与 当前的runData中的products 没有有交集 score = -1;
   * 最后取 score最大值
   * @param currentProducts
   * @param currentStages
   * @param destProducts
   * @param destStages
   * @param otherProductsList
   * @param otherStagesList
   * @return
   */
  def isDestControlPlan(destControlPlanId :Long,
                        currentProducts: List[String],
                        currentStages: List[String],
                        controlPlanList:List[OfflineAutoLimitControlPlanInfo]): Boolean = {

    val matchControlPlanList = controlPlanList.map(controlPlan => {
      val stages = controlPlan.stages
      if(stages.isEmpty){
        controlPlan.setScore(0)
      }else if(stages.intersect(currentStages).nonEmpty){
        controlPlan.setScore(2)
      }else{
        controlPlan.setScore(-1)
      }
      controlPlan
    }).filter(controlPlan => {
      controlPlan.getScore >= 0
    }).map(controlPlan => {
      val products = controlPlan.products
      if(products.isEmpty){
        controlPlan.setScore(controlPlan.getScore + 0)
      }else if(products.intersect(currentProducts).isEmpty){
        controlPlan.setScore(-1)
      }else {
        controlPlan.setScore(controlPlan.getScore + 1)
      }
      controlPlan
    }).filter(controlPlan => {
      controlPlan.getScore >= 0
    })

    if(matchControlPlanList.nonEmpty){
      val maxScore = matchControlPlanList.sortBy(controlPlan => {
        controlPlan.score
      }).reverse.head.score

      val matchControlPlanIds = matchControlPlanList.filter(controlPlan => {
        maxScore == controlPlan.score
      }).map(controlPlan => {
        controlPlan.controlPlanId
      })

      if(matchControlPlanIds.contains(destControlPlanId)){
        true
      }else{
        logger.warn(s"该Run命中了其它controlplan: " +
          s"matchControlPlanList==${matchControlPlanList} ; " +
          s"matchControlPlanIds==${matchControlPlanIds} ; " +
          s"destControlPlanId==${destControlPlanId} ; " +
          s"currentProducts==${currentProducts} ; " +
          s"currentStages==${currentStages} ; " +
          s"controlPlans==${controlPlanList.toJson}")
        false
      }
    }else{
      logger.error(s"该Run没有命中任何一个controlplan:" +
        s"destControlPlanId==${destControlPlanId} ; " +
        s"currentProducts==${currentProducts} ; " +
        s"currentStages==${currentStages} ; " +
        s"controlPlans==${controlPlanList.toJson} ; ")
      false
    }

  }

  /**
   * 分片查询Run数据
   * 捞Run的数据获取 开始时间
 *
   * @param queryRunDataConfig
   * @return
   */
  def getRunTimeRange(queryRunDataConfig: QueryRunDataConfig) = {
    val toolList = queryRunDataConfig.toolList
    val recipeList = queryRunDataConfig.recipeList
    val maxTimeRange = queryRunDataConfig.maxTimeRange
    val endTimestamp = queryRunDataConfig.endTimestamp
    val maxRunNumber = queryRunDataConfig.maxRunNumber.toInt
    val destControlPlanId = queryRunDataConfig.controlPlanId
    val controlPlanList = queryRunDataConfig.controlPlanList

//    val everyTimestamp: Long = ProjectConfig.OFFLINE_AUTOLIMIT_EVERY_SHARD_TIMES  // 分片查询，每次查询2天数据
    // 如果时间范围 timeRange > 1天(86400000)  分片查询按照配置 每次查询1天 ;
    // 如果 时间范围 timeRange < 1天(86400000) 分片查询:每次查询6小时
    // 或者 maxRunNumber 个数 < 50 每次查询6小时数据
    val everyTimestamp: Long = if(maxTimeRange < 24 * 60 * 60 * 1000l || maxRunNumber < 50){
        6 * 60 * 60 * 1000l
      }else{
        ProjectConfig.OFFLINE_AUTOLIMIT_EVERY_SHARD_TIMES  // 分片查询，每次查询1天数据
      }

    val queryTimes: Long = maxTimeRange/everyTimestamp + 1

    val totalOfflineAutoLimitRunDataList = new ListBuffer[OfflineAutoLimitRunData]()

    logger.warn(s"queryRunDataConfig == ${queryRunDataConfig.toJson}")
    if(toolList.nonEmpty){
      val loop = new Breaks

      loop.breakable {
        // todo 分片查询
        for (i <- Range(0,queryTimes.toInt)) {
          val currentEndTimestamp = endTimestamp - everyTimestamp * i
          var currentStartTimestamp = endTimestamp - everyTimestamp * (i+1)

          currentStartTimestamp = if(currentStartTimestamp < endTimestamp - maxTimeRange)
            {endTimestamp - maxTimeRange}else{currentStartTimestamp}

          if(currentEndTimestamp > currentStartTimestamp){
            toolList.foreach(toolInfo => {
              val toolName = toolInfo.toolName
              val chamberNameList = toolInfo.chamberNameList

              if (chamberNameList.nonEmpty) {
                chamberNameList.foreach(chamberName => {

                  val offlineAutoLimitRunDataList: ListBuffer[OfflineAutoLimitRunData] =
                    HbaseUtil.getRunDataFromHbaseByParams(toolName = toolName,
                      chamberName = chamberName,
                      startTimestamp = currentStartTimestamp,
                      endTimestamp = currentEndTimestamp,
                      columns = runDataColumns,
                      tableName = runDataTableName)
                      .filter((offlineAutoLimitRunData: OfflineAutoLimitRunData) => {
                        // todo 该Run是否属于本controlPlan
                        val currentRecipe = offlineAutoLimitRunData.recipe
                        val currentProducts = offlineAutoLimitRunData.products.trim.split(",").toList
                        val currentStages = offlineAutoLimitRunData.stages.trim.split(",").toList

                        recipeList.contains(currentRecipe) &&
                          isDestControlPlan(destControlPlanId,currentProducts,currentStages,controlPlanList)
                      })

                  if (offlineAutoLimitRunDataList.nonEmpty) {
                    totalOfflineAutoLimitRunDataList ++= offlineAutoLimitRunDataList
                  }

                  //                offlineAutoLimitRunDataList.foreach(println(_))
                  val startTimeFormat = DateTimeUtil.getTimeByTimestamp13(currentStartTimestamp,"yyyy-MM-dd HH:mm:ss")
                  val endTimeFormat = DateTimeUtil.getTimeByTimestamp13(currentEndTimestamp,"yyyy-MM-dd HH:mm:ss")
                  logger.warn(s" toolName = ${toolName} ,chamberName= ${chamberName},recipeList = ${recipeList},startTimeFormat = ${startTimeFormat},endTimeFormat = ${endTimeFormat} : length = ${offlineAutoLimitRunDataList.length}")
                })
              }

            })
          }

          // todo 是否已经获取到足够的数据
          if(totalOfflineAutoLimitRunDataList.length >= maxRunNumber){
            logger.warn(s"${toolList.toJson}:已经查出足够Run个数 maxRunNumber == ${maxRunNumber} ; query RunData length == ${totalOfflineAutoLimitRunDataList.length}")
            loop.break()
          }else{
            logger.warn(s"${toolList.toJson}:当前查询RunData length == ${totalOfflineAutoLimitRunDataList.length} ; maxRunNumber == ${maxRunNumber} ")
          }
        }
      }
    }

    // todo 获取这批RunData的 开始时间
    if(totalOfflineAutoLimitRunDataList.nonEmpty){

      val queryRunDataList = totalOfflineAutoLimitRunDataList.sortBy(elem => {
        elem.run_start_time
      }).reverse.slice(0, maxRunNumber)

      val lastRunData: OfflineAutoLimitRunData = queryRunDataList.last
      logger.warn(s"${toolList.toJson}:offlineAutoLimit query RunData length == ${queryRunDataList.length},lastRunData==${lastRunData}")

      lastRunData
    }else{

      logger.error(s"${queryRunDataConfig.controlPlanId}-${queryRunDataConfig.version} lastRunData == null : Don't query RunData ; ")

      OfflineAutoLimitRunData(
        rowkey = "",
        run_start_time = queryRunDataConfig.endTimestamp,
        run_end_time = queryRunDataConfig.endTimestamp
      )
    }
  }

  /**
   * 解析 OfflineAutoLimitIndicatorConfig
   * @param offlineAutoLimitConfig
   * @return
   */
  def parseOfflineAutoLimitIndicatorConfig(offlineAutoLimitConfig:OfflineAutoLimitConfig,lastRunData:OfflineAutoLimitRunData) = {
    // key: indicatorId   value: OfflineAutoLimitIndicatorConfig
    val offlineAutoLimitIndicatorConfigMap = TrieMap[String,OfflineAutoLimitIndicatorConfig]()

    try{
      val deploymentInfoList = offlineAutoLimitConfig.deploymentInfo
      if(deploymentInfoList.nonEmpty){
        deploymentInfoList.foreach(deploymentInfo => {
          val indicatorId = deploymentInfo.indicatorId.toString
          val specId = deploymentInfo.specId
          if(null != specId && 0 != specId){
            if(offlineAutoLimitIndicatorConfigMap.contains(indicatorId)){

              val offlineAutoLimitIndicatorConfig = offlineAutoLimitIndicatorConfigMap.get(indicatorId).get
              offlineAutoLimitIndicatorConfig.deploymentInfo.append(deploymentInfo)
              offlineAutoLimitIndicatorConfigMap.put(indicatorId,offlineAutoLimitIndicatorConfig)

            }else{

              val offlineAutoLimitIndicatorConfig = initOfflineAutoLimitIndicatorConfig(offlineAutoLimitConfig,deploymentInfo,lastRunData)
              offlineAutoLimitIndicatorConfigMap.put(indicatorId,offlineAutoLimitIndicatorConfig)

            }
          }else{
            logger.error(s"This specId is null ; deploymentInfo = ${deploymentInfo}")
          }
        })
      }
    }catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"parse OfflineAutoLimitIndicatorConfig error")
      }
    }

    offlineAutoLimitIndicatorConfigMap
  }


  /**
   *
   * @param offlineAutoLimitConfig
   * @param deploymentInfo
   * @param startTimestamp
   * @return
   */
  def initOfflineAutoLimitIndicatorConfig(offlineAutoLimitConfig:OfflineAutoLimitConfig,
                                          deploymentInfo:OfflineDeploymentInfo,
                                          lastRunData:OfflineAutoLimitRunData): OfflineAutoLimitIndicatorConfig = {

    val run_end_time = lastRunData.run_end_time
    val run_start_time = lastRunData.run_start_time
    val run_range_time = if(run_end_time>run_start_time){run_end_time - run_start_time}else 0l

    OfflineAutoLimitIndicatorConfig(controlPlanId = offlineAutoLimitConfig.controlPlanId,
      version = offlineAutoLimitConfig.version,
      toolList = offlineAutoLimitConfig.toolList,
      recipeList = offlineAutoLimitConfig.recipeList,
      indicatorId = deploymentInfo.indicatorId.toString,
      startTimestamp = lastRunData.run_start_time,
      runRangeTime = run_range_time,
      endTimestamp = offlineAutoLimitConfig.endTimestamp,
      maxTimeRange = offlineAutoLimitConfig.maxTimeRange,
      removeOutlier = offlineAutoLimitConfig.removeOutlier,
      maxRunNumber = offlineAutoLimitConfig.maxRunNumber,
      every = offlineAutoLimitConfig.every,
      triggerMethodName = offlineAutoLimitConfig.triggerMethodName,
      triggerMethodValue = offlineAutoLimitConfig.triggerMethodValue,
      active = offlineAutoLimitConfig.active,
      deploymentInfo = ListBuffer[OfflineDeploymentInfo](deploymentInfo))
  }



  /**
   * 解析出所有的ConditionEntity
   * @param offlineAutoLimitIndicatorConfig
   * @return
   */
  def parseConfigConditionEntity(offlineAutoLimitIndicatorConfig:OfflineAutoLimitIndicatorConfig) = {
    val deploymentInfoList = offlineAutoLimitIndicatorConfig.deploymentInfo

    // key:specId  ; value:ListBuffer[ConditionEntity]
//    var conditionEntityMap: TrieMap[String,ListBuffer[ConditionEntity]] = new TrieMap[String, ListBuffer[ConditionEntity]]()
    var conditionEntityMap: TrieMap[String,ConditionEntity] = new TrieMap[String, ConditionEntity]()

    deploymentInfoList.foreach(deploymentInfo => {
      val specId = deploymentInfo.specId.toString
      val condition: OfflineCondition = deploymentInfo.condition
      val toolNameList = condition.tool
      val chamberNameList = condition.chamber
      val recipeNameList = condition.recipe
      val productNameList = condition.product
      val stageNameList = condition.stage

      var toolNames = ""
      var chamberNames = ""
      var recipeNames = ""
      var productNames = ""
      var stageNames = ""

      if(toolNameList != null && toolNameList.nonEmpty ){
        toolNames = toolNameList.mkString(",")
      }

      if(null != chamberNameList && chamberNameList.nonEmpty){
        chamberNames = chamberNameList.mkString(",")
      }

      if(null != recipeNameList && recipeNameList.nonEmpty){
        recipeNames = recipeNameList.mkString(",")
      }

      if(null != productNameList && productNameList.nonEmpty){
        productNames = productNameList.mkString(",")
      }

      if(null != stageNameList && stageNameList.nonEmpty){
        stageNames = stageNameList.mkString(",")
      }

      conditionEntityMap.put(specId,new ConditionEntity(toolNames,chamberNames,recipeNames,productNames,stageNames))
//      if(toolNameList.nonEmpty){
//        toolNameList.foreach(toolName => {
//          if(chamberNameList.nonEmpty){
//            chamberNameList.foreach(chamberName => {
//              if(recipeNameList.nonEmpty){
//                recipeNameList.foreach( recipeName => {
//                  if(productNameList.nonEmpty){
//                    productNameList.foreach(productName => {
//                      if(stageNameList.nonEmpty){
//                        stageNameList.foreach(stageName => {
//                          conditionEntityMap = addConditionEntity(conditionEntityMap,specId,
//                            new ConditionEntity(toolName,chamberName,recipeName,productName,stageName))
//                        })
//                      }else{
//                        conditionEntityMap = addConditionEntity(conditionEntityMap,specId,
//                          new ConditionEntity(toolName,chamberName,recipeName,productName,""))
//                      }
//                    })
//                  }else{
//                    conditionEntityMap = addConditionEntity(conditionEntityMap,specId,
//                      new ConditionEntity(toolName,chamberName,recipeName,"",""))
//                  }
//                })
//              }else{
//                conditionEntityMap = addConditionEntity(conditionEntityMap,specId,
//                  new ConditionEntity(toolName,chamberName,"","",""))
//              }
//            })
//          }else{
//            conditionEntityMap = addConditionEntity(conditionEntityMap,specId,
//              new ConditionEntity(toolName,"","","",""))
//          }
//        })
//      }else{
//        conditionEntityMap = addConditionEntity(conditionEntityMap,specId,
//          new ConditionEntity("","","","",""))
//      }
    })

    conditionEntityMap
  }


  /**
   * 添加 ConditionEntity
   * @param conditionEntityMap
   * @param indicatorId
   * @param specId
   * @param conditionEntity
   * @return
   */
  def addConditionEntity(conditionEntityMap: TrieMap[String,  ListBuffer[ConditionEntity]],
                         specId:String,
                         conditionEntity:ConditionEntity) = {

    if(conditionEntityMap.contains(specId)){
      val conditionEntityList = conditionEntityMap.get(specId).get
      conditionEntityList.append(conditionEntity)
      conditionEntityMap.put(specId,conditionEntityList)
    }else{
      val conditionEntityList = ListBuffer[ConditionEntity](conditionEntity)
      conditionEntityMap.put(specId,conditionEntityList)
    }

    conditionEntityMap
  }

  /**
   * 解析出每个Spec的配置 isCalcLimit == true
   * @param offlineAutoLimitIndicatorConfig
   */
  def paseOffLineAutoLimitOneConfig(offlineAutoLimitIndicatorConfig: OfflineAutoLimitIndicatorConfig) = {
    val offlineAutoLimitOneConfigMap = new TrieMap[String, OfflineAutoLimitOneConfig]()

    val deploymentInfoList = offlineAutoLimitIndicatorConfig.deploymentInfo

    if(deploymentInfoList.nonEmpty){

      deploymentInfoList.foreach((deploymentInfo: OfflineDeploymentInfo) => {
        val key = deploymentInfo.specId.toString
        if (!deploymentInfo.equals("selfDefined") && deploymentInfo.isCalcLimit) {
          val offlineAutoLimitOneConfig = OfflineAutoLimitOneConfig(
            controlPlanId = offlineAutoLimitIndicatorConfig.controlPlanId,
            version = offlineAutoLimitIndicatorConfig.version,
            toolList = offlineAutoLimitIndicatorConfig.toolList,
            recipeList = offlineAutoLimitIndicatorConfig.recipeList,
            endTimestamp = offlineAutoLimitIndicatorConfig.endTimestamp,
            maxTimeRange = offlineAutoLimitIndicatorConfig.maxTimeRange,
            removeOutlier = offlineAutoLimitIndicatorConfig.removeOutlier,
            maxRunNumber = offlineAutoLimitIndicatorConfig.maxRunNumber,
            every = offlineAutoLimitIndicatorConfig.every,
            triggerMethodName = offlineAutoLimitIndicatorConfig.triggerMethodName,
            triggerMethodValue = offlineAutoLimitIndicatorConfig.triggerMethodValue,
            active = offlineAutoLimitIndicatorConfig.active,
            indicatorId = deploymentInfo.indicatorId,
            specId = deploymentInfo.specId,
            isCalcLimit = deploymentInfo.isCalcLimit,
            condition = deploymentInfo.condition,
            limitMethod = deploymentInfo.limitMethod,
            limitValue = deploymentInfo.limitValue,
            cpk = deploymentInfo.cpk,
            target = deploymentInfo.target
          )

          offlineAutoLimitOneConfigMap.put(key,offlineAutoLimitOneConfig)
        }
      })
    }

    offlineAutoLimitOneConfigMap
  }

  /**
   * 分片查询 IndicatorData
   * @param offlineAutoLimitIndicatorConfig
   * @return
   */
  def getAutoLimitIndicatorResultData(offlineAutoLimitIndicatorConfig: OfflineAutoLimitIndicatorConfig) = {

    val toolList = offlineAutoLimitIndicatorConfig.toolList
    val runRangeTime = if( 0 != offlineAutoLimitIndicatorConfig.runRangeTime){
      offlineAutoLimitIndicatorConfig.runRangeTime
    }else{
      12000
    }
    val startTimestamp = offlineAutoLimitIndicatorConfig.startTimestamp - runRangeTime*10
    val endTimestamp = offlineAutoLimitIndicatorConfig.endTimestamp + runRangeTime
    val every = offlineAutoLimitIndicatorConfig.every.toInt
    val indicatorId = offlineAutoLimitIndicatorConfig.indicatorId

    val maxRunNumber = offlineAutoLimitIndicatorConfig.maxRunNumber.toInt

    val timeRange = endTimestamp - startTimestamp

    val queryAutoLimitIndicatorResult: ListBuffer[AutoLimitIndicatorResult] = ListBuffer[AutoLimitIndicatorResult]()
    val sampleAutoLimitIndicatorResult: ListBuffer[AutoLimitIndicatorResult] = ListBuffer[AutoLimitIndicatorResult]()

    // 如果时间范围 timeRange > 1天(86400000)  分片查询按照配置 每次查询1天 ; 如果 时间范围 timeRange < 1天(86400000) 分片查询:每次查询一小时
    val everyTimestamp: Long = if(timeRange > 24 * 60 * 60 * 1000l){
        ProjectConfig.OFFLINE_AUTOLIMIT_EVERY_SHARD_TIMES  // 分片查询，每次查询1天数据
      }else{
        1 * 60 * 60 * 1000l
      }

    val queryTimes: Long = timeRange/everyTimestamp + 1

    // todo 分片查询
    if(toolList.nonEmpty){
      for (i <- Range(0,queryTimes.toInt)){
        val currentEndTimestamp = endTimestamp - everyTimestamp * i
        var currentStartTimestamp = endTimestamp - everyTimestamp * (i+1)

        currentStartTimestamp = if(startTimestamp > currentStartTimestamp) startTimestamp else currentStartTimestamp

        if(currentEndTimestamp > currentStartTimestamp ){
          toolList.foreach(toolInfo => {
            val toolName = toolInfo.toolName
            val chamberNameList = toolInfo.chamberNameList
            if(chamberNameList.nonEmpty){
              chamberNameList.foreach(chamberName => {
                val queryShardResultList: ListBuffer[AutoLimitIndicatorResult] = HbaseUtil.getIndicatorDataFromHbaseByParams(toolName,
                  chamberName,
                  indicatorId,
                  currentStartTimestamp,
                  currentEndTimestamp,
                  indicatorColumns,
                  indicatorTableName)

                if(queryShardResultList.nonEmpty){
                  queryAutoLimitIndicatorResult ++= queryShardResultList
                }

                val startTimeFormat = DateTimeUtil.getTimeByTimestamp13(currentStartTimestamp,"yyyy-MM-dd HH:mm:ss")
                val endTimeFormat = DateTimeUtil.getTimeByTimestamp13(currentEndTimestamp,"yyyy-MM-dd HH:mm:ss")
                logger.warn(s"indicatorId = ${indicatorId} ,toolName = ${toolName} ,chamberName= ${chamberName},startTimeFormat = ${startTimeFormat},endTimeFormat = ${endTimeFormat} : length = ${queryShardResultList.length}")

              })
            }
          })
        }
      }
    }

    // todo 采样
    if(queryAutoLimitIndicatorResult.nonEmpty){
      val maxAutoLimitIndicatorResult = queryAutoLimitIndicatorResult.slice(0, maxRunNumber)

      val length = maxAutoLimitIndicatorResult.length
      var count = 0
      while (count < length){
        sampleAutoLimitIndicatorResult.append(maxAutoLimitIndicatorResult(count))
        count = count + every
      }
    }

    logger.warn(s"indicatorId == ${offlineAutoLimitIndicatorConfig.indicatorId},endTimestamp == ${offlineAutoLimitIndicatorConfig.endTimestamp},total length == ${queryAutoLimitIndicatorResult.length}")

    logger.warn(s"indicatorId == ${offlineAutoLimitIndicatorConfig.indicatorId},endTimestamp == ${offlineAutoLimitIndicatorConfig.endTimestamp},sample length == ${sampleAutoLimitIndicatorResult.length}")

    sampleAutoLimitIndicatorResult
  }


  /**
   * 根据indicator的 Product 和 Stage 生成ConditionEntity
   * @param autoLimitIndicatorResult
   * @return
   */
  def getIndicatorConditionEntity(autoLimitIndicatorResult: AutoLimitIndicatorResult) = {
    val productList: List[String] = autoLimitIndicatorResult.product
    val stageList = autoLimitIndicatorResult.stage
    val indicatorConditionEntityList = new ListBuffer[ConditionEntity]()
    if (productList == null || stageList == null || productList.size == 0 || stageList.size == 0) {
      logger.error(s"IndicatorResult product or stage is null:${autoLimitIndicatorResult.toString}")
      indicatorConditionEntityList.append(new ConditionEntity(autoLimitIndicatorResult.toolName,
        autoLimitIndicatorResult.chamberName,
        autoLimitIndicatorResult.recipeName,
        "",
        ""))
    }else{
      for (product <- productList) {
        for (stage <- stageList) {
          indicatorConditionEntityList.append(new ConditionEntity(autoLimitIndicatorResult.toolName,
            autoLimitIndicatorResult.chamberName,
            autoLimitIndicatorResult.recipeName,
            product,
            stage))
        }
      }
    }
    indicatorConditionEntityList
  }


  /**
   * 匹配条件
   *
   * @param configConditionEntity
   * @param indicatorConditionEntity
   * @return
   */
  def satisfied(configConditionEntity: ConditionEntity, indicatorConditionEntity: ConditionEntity): Int = {
    try {
      var value=0
      for (i <- 0 until fields.size reverse) {

        val field: Field = fields(i)

        val current = field.get(configConditionEntity)
        val configConditionValue = field.get(configConditionEntity).asInstanceOf[String]
        val indicatorConditionValue = field.get(indicatorConditionEntity).asInstanceOf[String]

        logger.warn(s"configConditionValue == ${configConditionValue} ; indicatorConditionValue == ${indicatorConditionValue}")
        if (StringUtils.isNotBlank(configConditionValue) ) {

          if (configConditionValue.split(",").contains(indicatorConditionValue))
            value+=pow(2.toDouble,i.toDouble).toInt
          else
            return -1

        }
      }
      value
    } catch {
      case e: Exception => e.printStackTrace()
        -1
    }
  }


  /**
   *
   * @param get
   */
  def getEmptyOfflineAutoLimitResult(offlineAutoLimitOneConfig: OfflineAutoLimitOneConfig) = {
    OfflineAutoLimitResult(
      "offlineAutoLimitResult",
      offlineAutoLimitOneConfig.specId,
      s"${offlineAutoLimitOneConfig.controlPlanId}-${offlineAutoLimitOneConfig.version}-${offlineAutoLimitOneConfig.endTimestamp}",
      System.currentTimeMillis(),
      SpecLimit(null, null, null, null, null, null),
      offlineAutoLimitOneConfig,
      runNum = 0)
  }

  /**
   * 获取空的SpeckLimit
   * @return
   */
  def getEmptySpecLimit(): SpecLimit = {
    SpecLimit(null, null, null, null, null, null)
  }

  /**
   * 开始计算AutoLimit
   * @param offlineAutoLimitOneConfig
   * @param autoLimitIndicatorResultList
   * @return
   */
  def calcAutoLimit(offlineAutoLimitOneConfig: OfflineAutoLimitOneConfig, autoLimitIndicatorResultList: ListBuffer[AutoLimitIndicatorResult]) = {

    if (autoLimitIndicatorResultList == null || autoLimitIndicatorResultList.isEmpty) {
      logger.warn(ErrorCode("007006b001C", System.currentTimeMillis(), Map("config" -> offlineAutoLimitOneConfig, "indicatorResult" -> autoLimitIndicatorResultList), "autoLimit计算所需indicatorReuslt列表为空").toJson)
    }

    var values = getValues(autoLimitIndicatorResultList)
    if (values.isEmpty) {
      logger.warn(s"----------getValues-----------")
      logger.warn(ErrorCode("007006c002C", System.currentTimeMillis(), Map("config" -> offlineAutoLimitOneConfig, "oneValue" -> autoLimitIndicatorResultList.head), "conditionFilter后数据列表为空").toJson)
    }
    //是否需要移除离群点
    if ("1".equals(offlineAutoLimitOneConfig.removeOutlier)) {
      values = removeOutlier(offlineAutoLimitOneConfig, values)
    }
    if (values.isEmpty) {
      logger.warn(ErrorCode("007006c003C", System.currentTimeMillis(), Map("config" -> offlineAutoLimitOneConfig, "oneValue" -> autoLimitIndicatorResultList.head), "removeOutlier 移除离群点后数据列表为空").toJson)
    }

    try{
      logger.warn(s"offlineAutoLimitResult: ${offlineAutoLimitOneConfig.controlPlanId}-${offlineAutoLimitOneConfig.version}-${offlineAutoLimitOneConfig.specId} target: ${mean(values.toList)}")
    }catch {
      case ex: Exception => logger.warn(s"${ex.toString}")
    }

    val specLimit = calculate(offlineAutoLimitOneConfig, values.toList)
//    logger.warn(s"specLimit == ${specLimit}")
    // todo 计算
    OfflineAutoLimitResult(
      "offlineAutoLimitResult",
      offlineAutoLimitOneConfig.specId,
      //  UUID.randomUUID().toString.replaceAll("-", ""),
      //  暂定为 controlPlanId +窗口开始时间
      s"${offlineAutoLimitOneConfig.controlPlanId}-${offlineAutoLimitOneConfig.version}-${offlineAutoLimitOneConfig.endTimestamp}",
      System.currentTimeMillis(),
      specLimit,
      offlineAutoLimitOneConfig,
      runNum = autoLimitIndicatorResultList.length)
  }



  /**
   * 计算 AutoLimit
   * @param offlineAutoLimitOneConfig
   * @param indicatorValueList
   * @return
   */
  def calculate(offlineAutoLimitOneConfig: OfflineAutoLimitOneConfig, indicatorValueList: List[Double]): SpecLimit = {
    try{
      val limitMethod = offlineAutoLimitOneConfig.limitMethod
      // 修改target成实时计算的，不是后台传的配置
      val target = mean(indicatorValueList)
      var usl: BigDecimal = null
      var lsl: BigDecimal = null
      var ubl: BigDecimal = null
      var lbl: BigDecimal = null
      var ucl: BigDecimal = null
      var lcl: BigDecimal = null
      if (indicatorValueList.nonEmpty) {
        if ("sigma".equals(limitMethod)) {
          val n = offlineAutoLimitOneConfig.limitValue.toDouble
          val avg = mean(indicatorValueList)
          val sigma = getSigma(indicatorValueList, avg)
          lcl = BigDecimal((target - n * sigma * 1.0).formatted("%.16f"))
          ucl = BigDecimal((target + n * sigma * 1.0).formatted("%.16f"))
          lbl = BigDecimal((target - n * sigma * 1.5).formatted("%.16f"))
          ubl = BigDecimal((target + n * sigma * 1.5).formatted("%.16f"))
          lsl = BigDecimal((target - n * sigma * 2.0).formatted("%.16f"))
          usl = BigDecimal((target + n * sigma * 2.0).formatted("%.16f"))
        } else if ("percent".equals(limitMethod)) {
          val percentValue = offlineAutoLimitOneConfig.limitValue.toDouble
          val n = if (target > 0) percentValue / 100 else percentValue / -100
          lcl = BigDecimal((target * (1 - n * 1.0)).formatted("%.16f"))
          ucl = BigDecimal((target * (1 + n * 1.0)).formatted("%.16f"))
          lbl = BigDecimal((target * (1 - n * 1.5)).formatted("%.16f"))
          ubl = BigDecimal((target * (1 + n * 1.5)).formatted("%.16f"))
          lsl = BigDecimal((target * (1 - n * 2.0)).formatted("%.16f"))
          usl = BigDecimal((target * (1 + n * 2.0)).formatted("%.16f"))
        } else if ("iqr".equalsIgnoreCase(limitMethod)) {
          val n = offlineAutoLimitOneConfig.limitValue.toDouble
          val sortList = indicatorValueList.sortWith(_ < _)
          val qu = quartile(sortList, 3)
          val ql = quartile(sortList, 1)
          val iqr = qu - ql
          lcl = BigDecimal((ql - n * iqr * 1.0).formatted("%.16f"))
          ucl = BigDecimal((qu + n * iqr * 1.0).formatted("%.16f"))
          lbl = BigDecimal((ql - n * iqr * 1.5).formatted("%.16f"))
          ubl = BigDecimal((qu + n * iqr * 1.5).formatted("%.16f"))
          lsl = BigDecimal((ql - n * iqr * 2.0).formatted("%.16f"))
          usl = BigDecimal((qu + n * iqr * 2.0).formatted("%.16f"))
        } else {
          logger.warn(ErrorCode("007006c004C", System.currentTimeMillis(), Map("config" -> offlineAutoLimitOneConfig, "limitMethod" -> limitMethod), "不支持的limitMethod").toJson)
        }
      }
      SpecLimit(usl.toString(), lsl.toString(), ubl.toString(), lbl.toString(), ucl.toString(), lcl.toString())
    }catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error("inner calculate error ")
      }
      getEmptySpecLimit()
    }

  }

  /**
   *
   * @param data
   * @return
   */
  def getValues(data: ListBuffer[AutoLimitIndicatorResult]): ListBuffer[Double] = {
    data.flatMap(elem => {
//      logger.warn(s"elem.indicatorValue == ${elem.indicatorValue}")
      elem.indicatorValue.split("\\|").map(value => {
//        logger.warn(s"value == ${value}")
        value.toDouble
      })
    })
  }

  /**
   * 求平均值
   * @param l
   * @return
   */
  def mean(l: List[Double]): Double = {
    l.sum / l.size
  }

  /**
   *
   *  求方差
   * */
  def getSigma(l: List[Double], avg: Double): Double = {
    var dVar = 0.00000

    for(aDoube <- l){
      dVar = dVar + (aDoube - avg) * (aDoube - avg)
    }
    sqrt(dVar/ l.size)
  }

  /**
   * 求四分位
   * @param array
   * @param quart
   * @return
   */
  def quartile(array: List[Double], quart: Int): Double = {
    val n = array.size
    val p = quart / 4.0
    val pos = 1 + (n - 1) * p
    val posStr = pos.toString
    val dotIndex = posStr.indexOf(".")
    val startIndex = if (dotIndex != -1) {
      posStr.substring(0, dotIndex).toInt
    } else {
      pos.toInt
    }
    val weight = pos - startIndex
    if (startIndex >= n) return array(n - 1)
    array(startIndex - 1) * (1 - weight) + array(startIndex) * weight
  }

  /**
   * 移除离群点
   */
  def removeOutlier(d: OfflineAutoLimitOneConfig, values: ListBuffer[Double]) = {
    if (values.isEmpty) {
      values
    } else {
      val limitMethod = d.limitMethod
      val limitValue = d.limitValue.toDouble
      val avg = mean(values.toList)
      var max: Double = Double.NaN
      var min: Double = Double.NaN
      if ("sigma".equals(limitMethod)) {
        //Avg±N*sigma之外的点为离群点
        val sigma = getSigma(values.toList, avg)
        max = avg + limitValue * sigma
        min = avg - limitValue * sigma
      } else if ("percent".equals(limitMethod)) {
        //Avg（1±m%）之外的点为离群点
        max = avg * (1 + limitValue / 100)
        min = avg * (1 - limitValue / 100)
      } else if ("iqr".equalsIgnoreCase(limitMethod)) {
        val n = d.limitValue.toDouble
        val sortList = values.sortWith(_ < _)
        val qu = quartile(sortList.toList, 3)
        val ql = quartile(sortList.toList, 1)
        val iqr = qu - ql
        max = qu + n * iqr
        min = ql - n * iqr
      } else {
        logger.warn(ErrorCode("007006c004C", System.currentTimeMillis(), Map("config" -> d, "limitMethod" -> limitMethod), "不支持的limitMethod").toJson)
      }
      val results = values.filter(r => {
        if (!Double.NaN.equals(max) && !Double.NaN.equals(min)) {
          r >= min && r <= max
        } else {
          true
        }
      })
      if (results.isEmpty) {
        logger.warn(ErrorCode("007006c003C", System.currentTimeMillis(), Map("config" -> d, "max" -> max, "min" -> min, "oneValue" -> values.head), "移除离群点后数据列表为空").toJson)
      }
      results
    }
  }
}
