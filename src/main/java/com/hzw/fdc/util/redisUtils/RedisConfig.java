package com.hzw.fdc.util.redisUtils;

import lombok.Data;

import java.io.Serializable;

@Data
public class RedisConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String host = "10.1.10.101";
    private int port = 6379;
    private int database = 0;
    private String password = null;
    protected int maxTotal = 8;  //最大连接数
    protected int maxIdle = 8;   //最大空闲连接数
    protected int minIdle = 0;  //最小空闲连接数
    protected int connectionTimeout = 2000;  //超时事件

    public RedisConfig host(String host) {
        setHost(host);
        return this;
    }

    public RedisConfig port(int port) {
        setPort(port);
        return this;
    }

    public RedisConfig database(int database) {
        setDatabase(database);
        return this;
    }

    public RedisConfig password(String password) {
        setPassword(password);
        return this;
    }

    public RedisConfig maxTotal(int maxTotal) {
        setMaxTotal(maxTotal);
        return this;
    }

    public RedisConfig maxIdle(int maxIdle) {
        setMaxIdle(maxIdle);
        return this;
    }

    public RedisConfig minIdle(int minIdle) {
        setMinIdle(minIdle);
        return this;
    }

    public RedisConfig connectionTimeout(int connectionTimeout) {
        setConnectionTimeout(connectionTimeout);
        return this;
    }
}
