package com.hzw.redis

import com.hzw.fdc.util.redisUtils.RedisUtil

/**
 * TestRedisUtil
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/4 17:30
 * @since 1.0.0
 * @update 2022/8/4 17:30
 * */
object TestRedisUtil {

  def main(args: Array[String]): Unit = {

    val alarmEwmaCacheList = RedisUtil.alarmEwmaCacheList
    alarmEwmaCacheList.foreach(println(_))

  }

}
