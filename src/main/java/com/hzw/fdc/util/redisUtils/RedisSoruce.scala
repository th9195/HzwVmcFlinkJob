package com.hzw.fdc.util.redisUtils

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConversions._

class RedisSoruce(key: String) extends RichSourceFunction[String] {
  val log = LoggerFactory.getLogger(classOf[RedisSoruce])
  var jedis: Jedis = null
  var isRunning = true
  val sleepTime = 5000L
  var host = "r-2zeukiiehh126kmr7i.redis.rds.aliyuncs.com"
  val pass = "aM8W$lIygzZJOEMgqj3uyEJhyVz7#w@x"

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    jedis = new Jedis(host, 6379)
    jedis.auth(pass)
    jedis.select(1)
    while (isRunning) {
      val mapDate: util.Map[String, String] = jedis.hgetAll(key)
      if (mapDate.size() > 0) {
        for ((key, value) <- mapDate) {
          ctx.collect(key + "###" + value)
        }
      } else {
        log.info("redis中数据为空")
      }
      Thread.sleep(sleepTime)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
    if (jedis != null) {
      jedis.close()
    }
  }
}
