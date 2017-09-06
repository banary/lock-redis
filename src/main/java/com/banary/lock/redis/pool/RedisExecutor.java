package com.banary.lock.redis.pool;

import com.banary.lock.util.Integers;
import com.banary.lock.util.Strings;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.util.Pool;

import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

/**
 * redis链接池
 *
 * @author Eden
 * @date 2017/8/13 上午12:55
 */
public class RedisExecutor {

    private volatile Pool<Jedis> jedisPool;

    public RedisExecutor(Properties properties){
        //构建JedisPoolConfig
        JedisPoolConfig jedisPoolConfig = this.buildJedisPoolConfig(properties);

        String host = properties.getProperty("host", Protocol.DEFAULT_HOST);
        int port = Integers.valueOf(properties.getProperty("port"), Protocol.DEFAULT_PORT);
        int timeout = Integers.valueOf(properties.getProperty("timeout"), Protocol.DEFAULT_TIMEOUT);
        String password = properties.getProperty("password");

        this.jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout, password);
    }

    private JedisPoolConfig buildJedisPoolConfig(Properties properties){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        String testOnBorrow = properties.getProperty("testOnBorrow");
        if(Strings.isNotEmpty(testOnBorrow)){
            jedisPoolConfig.setTestOnBorrow(Boolean.valueOf(testOnBorrow));
        }
        String maxIdle = properties.getProperty("maxIdle");
        if(Strings.isNotEmpty(maxIdle)){
            jedisPoolConfig.setMaxIdle(Integer.valueOf(maxIdle));
        }
        String maxTotal = properties.getProperty("maxTotal");
        if(Strings.isNotEmpty(maxTotal)){
            jedisPoolConfig.setMaxTotal(Integer.valueOf(maxTotal));
        }
        return jedisPoolConfig;
    }

    public <R> R execute(Function<Jedis, R> mapper){
        Objects.requireNonNull(mapper);
        Jedis jedis = jedisPool.getResource();
        try{
            return mapper.apply(jedis);
        }finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }

}
