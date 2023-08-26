package com.hzw.fdc.util.redisUtils;


import lombok.Data;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

@Data
public abstract class RedisCommand implements Serializable {
    String key;
    String field;
    Object value;
    int expire ;

    public RedisCommand(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public RedisCommand(String key, String field, Object value, int expire) {
        this.key = key;
        this.field = field;
        this.value = value;
        this.expire = expire;
    }


    public RedisCommand(String key, String field, Object value) {
        this.key = key;
        this.field = field;
        this.value = value;
    }

    public RedisCommand(String key, Object value, int expire) {
        this.key = key;
        this.value = value;
        this.expire = expire;
    }


    public void execute(Jedis jedis) {
        invokeByCommand(jedis);
        if (0 < this.expire) {
            jedis.expire(key, expire);
        }
    }

    public abstract void invokeByCommand(Jedis jedis);

}
