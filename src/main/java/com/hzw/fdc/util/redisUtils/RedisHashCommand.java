package com.hzw.fdc.util.redisUtils;

import redis.clients.jedis.Jedis;


public class RedisHashCommand extends RedisCommand {

    public RedisHashCommand(String key, String field, Object value) {
        super(key, field, value);
    }

    public RedisHashCommand(String key, String field, Object value, int expire) {
        super(key, field, value, expire);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.hset(getKey(), getField(), getValue().toString());
    }
}
