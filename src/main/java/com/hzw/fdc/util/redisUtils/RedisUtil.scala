package com.hzw.fdc.util.redisUtils

import com.hzw.fdc.json.MarshallableImplicits.Unmarshallable
import com.hzw.fdc.scalabean.{AdvancedIndicatorCacheData, AlarmEwmaCacheData, RedisCache}
import com.hzw.fdc.util.ProjectConfig
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, JedisSentinelPool}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
 * RedisUtil
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/4 13:34
 * @since 1.0.0
 * @update 2022/8/4 13:34
 * */
object RedisUtil {

//  var jedisPool: JedisPool = null
  var jedisSentinelPool : JedisSentinelPool= null

  val logger: Logger = LoggerFactory.getLogger(classOf[RedisSinkScala])

  lazy val alarmEwmaCacheList: ListBuffer[RedisCache[AlarmEwmaCacheData]] =
    getStringDataByPrefixKey[AlarmEwmaCacheData]("alarmEwmaCache")


  // "advancedIndicator_" + calcType
  lazy val advancedIndicatorDriftCacheList: ListBuffer[RedisCache[AdvancedIndicatorCacheData]] =
    getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_drift")

  lazy val advancedIndicatorDriftPercentCacheList: ListBuffer[RedisCache[AdvancedIndicatorCacheData]] =
    getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_driftPercent")

  lazy val advancedIndicatorAvgCacheList: ListBuffer[RedisCache[AdvancedIndicatorCacheData]] =
    getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_avg")

  lazy val advancedIndicatorMovAvgCacheList: ListBuffer[RedisCache[AdvancedIndicatorCacheData]] =
    getStringDataByPrefixKey[AdvancedIndicatorCacheData]("advancedIndicator_movAvg")


  /**
   * 获取Redis连接
   */
  def getConnection() = {
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

    //构建Redis线程池
//    jedisPool = new JedisPool(config, ProjectConfig.REDIS_HOST, ProjectConfig.REDIS_PORT)
    //从线程池中获取连接
  }

  def closeResource(jedis: Jedis): Unit = {
    if (jedis != null) {
      try {
        jedis.close()
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  /**
   * 获取 Jedis 对象
   * @return
   */
  def getJedis(): Jedis = {
    if (jedisSentinelPool == null){
      getConnection()
    }
    val jedis = jedisSentinelPool.getResource
    jedis
  }


  /**
   * 获取String类型的数据
   * @param key
   * @return
   */
  def getStringData(key:String): String= {
    var resultValue:String = null
    var jedis: Jedis = null
    try{
      jedis = getJedis()
      if(jedis.exists(key)){
        resultValue = jedis.get(key)
      }

    }catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s"get redis data Error key = ${key}")
    } finally {
      if (null != jedis)
        closeResource(jedis)
    }

    resultValue
  }


  /**
   * 获取String类型的数据
   * @param key
   * @return
   */
  def getStringDataByPrefixKey[T](prefixKey:String)(implicit m: Manifest[T]):ListBuffer[RedisCache[T]] = {

    var jedis: Jedis = null

    val result = new ListBuffer[RedisCache[T]]
    try{

      // 1- 获取Jedis
      jedis = getJedis()

      // 2- 获取 符合前缀的所有key
      logger.warn(s"getStringDataByPrefixKey prefixKey = ${prefixKey}")
      val keyList = jedis.keys(prefixKey + "*").asScala.toList

      // 3- 获取每个key的值
      keyList.map(key => {
        try{
          val value: String = jedis.get(key)
          result.append( value.fromJson[RedisCache[T]] )
        }catch {
          case e:Exception =>{
            logger.error(s"get Redis alarmEwmaCache key = ${key} error: $e")
          }
        }
      })

    }catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s"get redis data Error prefixKey = ${prefixKey}")
    } finally {
      if (null != jedis)
        closeResource(jedis)
    }

    result
  }

}
