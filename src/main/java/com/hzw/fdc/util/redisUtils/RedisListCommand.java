package com.hzw.fdc.util.redisUtils;

import redis.clients.jedis.Jedis;

/**
 * override Lpush command
 */

public class RedisListCommand extends RedisCommand {

    public RedisListCommand(String key, Object value) {
        super(key, value);
    }

    public RedisListCommand(String key, Object value, int expire) {
        super(key, value, expire);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.del(getKey());
        String[] arr = getValue().toString().split(",");
        jedis.lpush(getKey(),arr);
    }
}
