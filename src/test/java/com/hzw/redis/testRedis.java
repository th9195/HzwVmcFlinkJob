package com.hzw.redis;

import com.hzw.fdc.util.redisUtils.RedisConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * testRedis
 *
 * @author tobytang
 * @desc:
 * @date 2022/8/4 9:48
 * @update 2022/8/4 9:48
 * @since 1.0.0
 **/
public class testRedis {


    public static void main(String[] args) {
        RedisConfig redisConfig = new RedisConfig().database(3);



    }

    //todo:1-构建连接
    public Jedis jedis = null; //普通的jedis对象


    @After
    //释放连接
    public void closeConnect(){
        jedis.close();
    }

    @Before
    //构建jedis实例
    public void getConnection(){
        //方式一：直接实例化，指定服务端地址：机器+端口
//        jedis = new Jedis("node1",6379);
        //方式二：构建线程池
        //线程池的配置对象
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMinIdle(2);
        //构建Redis线程池
        JedisPool jedisPool = new JedisPool(config,"10.1.10.101",6379);
        //从线程池中获取连接
        jedis = jedisPool.getResource();
    }

    @Test
    public void testString(){
        //set/get/incr/exists/expire/setex/ttl
//        jedis.set("s1","hadoop");
//        System.out.println(jedis.get("s1"));
//        jedis.set("s2","10");
//        jedis.incr("s2");
//        System.out.println(jedis.get("s2"));
//        Boolean s1 = jedis.exists("s1");
//        Boolean s3 = jedis.exists("s3");
//        System.out.println(s1+"\t"+s3);
//        jedis.expire("s2",20);
//        while(true){
//            System.out.println(jedis.ttl("s2"));
//        }
        jedis.setex("s3",10,"spark");
    }


    @Test
    public void testHash(){
        //hset/hmset/hget/hgetall/hdel/hlen/hexists
        jedis.hset("m1","name","zhoujielun");
        Map<String,String> maps = new HashMap<>();
        maps.put("age","20");
        maps.put("add","sh");
        maps.put("sex","male");
        jedis.hmset("m1",maps);
        System.out.println(jedis.hget("m1","name"));
        System.out.println(jedis.hmget("m1","name","age"));
        System.out.println("===========================");
        Map<String, String> m1 = jedis.hgetAll("m1");
        for(Map.Entry<String,String> entry : m1.entrySet()){
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }
    }





}
