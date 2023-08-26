package com.hzw.fdc.util.redisUtils;

import redis.clients.jedis.Jedis;

/**
 * 移除set中key 对应的某个value
 */

public class RedisHashRmCommand extends RedisCommand {

    public RedisHashRmCommand(String key, String filed) {
        super(key, filed);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.hdel(getKey(), getValue().toString());
    }
}
