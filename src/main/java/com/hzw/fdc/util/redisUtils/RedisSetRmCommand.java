package com.hzw.fdc.util.redisUtils;

import redis.clients.jedis.Jedis;

/**
 * 移除set中key 对应的某个value
 */

public class RedisSetRmCommand extends RedisCommand {

    public RedisSetRmCommand(String key, Object value) {
        super(key, value);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {

        jedis.srem(getKey(), getValue().toString());
    }
}
