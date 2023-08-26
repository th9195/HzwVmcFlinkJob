package com.hzw.fdc.util.redisUtils;

import redis.clients.jedis.Jedis;

public class RedisSetCommand extends RedisCommand {
    public RedisSetCommand(String key, Object value, int expire) {
        super(key, value, expire);
    }

    public RedisSetCommand(String key, Object value) {
        super(key, value);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.sadd(getKey(), getValue().toString());
    }
}
