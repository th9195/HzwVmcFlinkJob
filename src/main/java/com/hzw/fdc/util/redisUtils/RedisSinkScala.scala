package com.hzw.fdc.util.redisUtils

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.{AdvancedIndicatorCacheData, AdvancedLinearFitIndicatorCacheData, AlarmEwmaCacheData, ErrorCode, RedisCache}
import com.hzw.fdc.util.ProjectConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, JedisSentinelPool}

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * RedisSinkScala
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/4 11:15
 * @since 1.0.0
 * @update 2022/8/4 11:15
 * */
class RedisSinkScala extends RichSinkFunction[JsonNode]{

//  var jedisPool: JedisPool = _
  var jedisSentinelPool : JedisSentinelPool= _
  var jedis :Jedis = _
  val logger: Logger = LoggerFactory.getLogger(classOf[RedisSinkScala])

  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    // 初始化连接redis
    getConnection()
  }

  override def close(): Unit = {
    closeResource()
    try jedisSentinelPool.close()
    catch {
      case e: Exception => logger.error("redis sink error {}", e)
    }
  }

  def closeResource(): Unit = {
    if (jedis != null) {
      try {
        jedis.close()
      }
      catch {
        case e: Exception => logger.error(s"closeResource error ${e.toString}")
      }
    }
  }

  /**
   * 获取Redis连接
   */
  def getConnection(): Unit = {
    val config =  new JedisPoolConfig
    config.setMaxTotal(ProjectConfig.REDIS_MAX_TOTAL)
    config.setMaxIdle(ProjectConfig.REDIS_MAX_IDLE)
    config.setMinIdle(ProjectConfig.REDIS_MIN_IDLE)

    //哨兵模式
    val redismaster = ProjectConfig.REDIS_MASTER
    val redis_sentinel_nodes = ProjectConfig.REDIS_SENTINEL_NODES
    val redisconnectiontimeout = ProjectConfig.REDIS_CONNECTION_TIMEOUT
    val sentinel_set = redis_sentinel_nodes.split(",").toSet

    jedisSentinelPool = new JedisSentinelPool(redismaster,sentinel_set,config,redisconnectiontimeout)

    //    //构建Redis线程池
    //    jedisPool = new JedisPool(config, ProjectConfig.REDIS_HOST, ProjectConfig.REDIS_PORT)

    try{
      jedis = jedisSentinelPool.getResource

      //    //从线程池中获取连接
      //    jedis = jedisPool.getResource
    }catch {
      case e:Exception => {
        logger.error(s"--- 获取连接失败 : ${e.toString} -----")
      }
    }finally {
      if(jedis != null){
        jedis.close()
      }
    }
  }

  // 获取单个连接
  def getJedis() = {
    if (jedisSentinelPool == null){
      getConnection()
    }
    jedisSentinelPool.getResource
  }


  override def invoke(sinkRedisData: JsonNode, context: SinkFunction.Context): Unit = {
    try {
      sinkRedis(sinkRedisData)
    } catch {
      case exception: Exception => logger.warn(ErrorCode("012008d001C", System.currentTimeMillis(),
        Map("invoke" -> "结果匹配失败!!"), exception.toString).toString)
    }
  }

  /**
   *   写入redis
   */
  def alarmEwmaCacheSinkRedis(sinkRedisData: JsonNode): Unit = {
    try{
      val redisCache = toBean[RedisCache[AlarmEwmaCacheData]](sinkRedisData)
      val alarmEwmaCacheData: AlarmEwmaCacheData = redisCache.datas
      val prefixKey = redisCache.dataType

      val key = prefixKey + "|" + alarmEwmaCacheData.ewmaKey + "|" + alarmEwmaCacheData.w2wKey

      if(null == jedis){
        jedis = getJedis()
      }
      jedis.set(key, sinkRedisData.toString)
    }catch {
      case exception: Exception => {
        logger.error(ErrorCode("012008d002C", System.currentTimeMillis(),
          Map("msg" -> "ewma的缓存 redis写入失败!!", "sinkRedisData" -> sinkRedisData), exception.toString).toString)

        // todo 重试 连接redis;
        logger.warn(s"retry connect redis")
        getConnection()
      }
    }
  }


  def sinkRedis(sinkRedisData: JsonNode) = {

    try{
      val dataType = sinkRedisData.findPath("dataType").asText()

      if(null == jedis){
        jedis = getJedis()
      }

      val key: String = dataType match {
        case "alarmEwmaCache" =>
          getAlarmEwmaCacheKey(sinkRedisData)
        case "advancedIndicator_avg" =>
          getAdvancedIndicatorCacheKey(sinkRedisData)
        case "advancedIndicator_movAvg" =>
          getAdvancedIndicatorCacheKey(sinkRedisData)
        case "advancedIndicator_drift" =>
          getAdvancedIndicatorCacheKey(sinkRedisData)
        case "advancedIndicator_driftPercent" =>
          getAdvancedIndicatorCacheKey(sinkRedisData)
        case "advancedIndicator_linearFit" =>
          getLinearFitAdvancedIndicatorCacheKey(sinkRedisData)
        case _ => ""
      }
      if(key.nonEmpty){
        jedis.set(key, sinkRedisData.toString)
        jedis.expire(key,ProjectConfig.REDIS_DATA_TTL)
      }
    }catch {
      case exception: Exception => {
        logger.error(ErrorCode("012008d002C", System.currentTimeMillis(),
          Map("msg" -> "advancedIndicator的缓存 redis写入失败!!", "sinkRedisData" -> sinkRedisData), exception.toString).toString)

        // todo 重试 连接redis;
        logger.warn(s"retry connect redis")
        getConnection()
      }
    }

  }

  // 获取 AdvancedIndicatorCacheData Key
  private def getAdvancedIndicatorCacheKey(sinkRedisData: JsonNode) = {
    val redisCache = toBean[RedisCache[AdvancedIndicatorCacheData]](sinkRedisData)
    val advancedIndicatorCacheData: AdvancedIndicatorCacheData = redisCache.datas
    val prefixKey = redisCache.dataType
    prefixKey + "|" + advancedIndicatorCacheData.baseIndicatorId + "|" + advancedIndicatorCacheData.w2wKey
  }

  private def getLinearFitAdvancedIndicatorCacheKey(sinkRedisData: JsonNode) = {
    val redisCache = toBean[RedisCache[AdvancedLinearFitIndicatorCacheData]](sinkRedisData)
    val linearFitAdvancedIndicatorCacheData = redisCache.datas
    val prefixKey = redisCache.dataType
    prefixKey + "|" + linearFitAdvancedIndicatorCacheData.baseIndicatorId + "|" + linearFitAdvancedIndicatorCacheData.w2wKey
  }

  // 获取AlarmewmaCache key
  private def getAlarmEwmaCacheKey(sinkRedisData: JsonNode) = {
    val redisCache = toBean[RedisCache[AlarmEwmaCacheData]](sinkRedisData)
    val alarmEwmaCacheData: AlarmEwmaCacheData = redisCache.datas
    val prefixKey = redisCache.dataType
    prefixKey + "|" + alarmEwmaCacheData.ewmaKey + "|" + alarmEwmaCacheData.w2wKey
  }


}
