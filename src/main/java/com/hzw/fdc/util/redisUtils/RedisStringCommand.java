package com.hzw.fdc.util.redisUtils;

import redis.clients.jedis.Jedis;

public class RedisStringCommand extends RedisCommand {
    public RedisStringCommand(String key, Object value, int expire) {
        super(key, value, expire);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.set(getKey(), getValue().toString(), "NX", "EX", getExpire());
    }
}
