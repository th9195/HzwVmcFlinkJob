package com.hzw.redis

import com.hzw.fdc.util.{LocalPropertiesConfig, ProjectConfig}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util

/**
 * TestRedisScala
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/4 10:35
 * @since 1.0.0
 * @update 2022/8/4 10:35
 * */
object TestRedisScala {

  var jedis :Jedis = null

  def initProjectConfig() = {
    ProjectConfig.PROPERTIES_FILE_PATH = LocalPropertiesConfig.config.getProperty("config_path")

    ProjectConfig.initConfig()
  }

  def main(args: Array[String]): Unit = {
    initProjectConfig()

    getConnection()

//    testHash()

//    testString()
    testPrefixKeys()
    closeConnect()
  }

  def closeConnect(): Unit = {
    jedis.close()
  }


  def getConnection() = {
    val config: JedisPoolConfig =  new JedisPoolConfig
    config.setMaxTotal(ProjectConfig.REDIS_MAX_TOTAL)
    config.setMaxIdle(ProjectConfig.REDIS_MAX_IDLE)
    config.setMinIdle(ProjectConfig.REDIS_MAX_IDLE)
    //构建Redis线程池
    val jedisPool = new JedisPool(config, ProjectConfig.REDIS_HOST, ProjectConfig.REDIS_PORT)
    //从线程池中获取连接
    jedis = jedisPool.getResource()
  }

  def testPrefixKeys() = {
    val keys: util.Set[String] = jedis.keys("ewma" + "*")


    val it = keys.iterator()

    while (it.hasNext){
      val key = it.next()
      println(s"key = ${key}")
    }

  }

  def testString():Unit = {
    jedis.set("ewma01","aaaa")
    jedis.set("ewma02","bbbb")
    jedis.set("ewma03","cccc")
    jedis.set("ewma04","dddd")
    jedis.set("ewma05","eeee")
  }

  def testHash(): Unit = {
    //hset/hmset/hget/hgetall/hdel/hlen/hexists
//    jedis.set("m2","value2")
    jedis.hset("m1", "name", "Toby")
    val maps: util.Map[String, String] = new util.HashMap[String, String]
    maps.put("age", "20")
    maps.put("add", "yichang")
    maps.put("sex", "male")
    jedis.hmset("m1", maps)
    System.out.println(jedis.hget("m1", "name"))
    System.out.println(jedis.hmget("m1", "name", "age"))
    System.out.println("===========================")
    val m1: util.Map[String, String] = jedis.hgetAll("m1")
    import scala.collection.JavaConversions._
    for (entry <- m1.entrySet) {
      System.out.println(entry.getKey + "\t" + entry.getValue)
    }
  }
}
