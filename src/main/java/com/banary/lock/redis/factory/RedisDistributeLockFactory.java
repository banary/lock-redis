package com.banary.lock.redis.factory;

import com.banary.lock.redis.RedisDistributeLock;
import com.banary.lock.redis.pool.RedisExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * redis分布式锁工厂
 *
 * @author Eden
 * @date 2017/8/13 上午12:47
 */
public class RedisDistributeLockFactory {

    private static Logger logger = LoggerFactory.getLogger(RedisDistributeLockFactory.class);

    private transient RedisExecutor redisExecutor;

    private transient Map<String, String> redisConfig;

    public void setRedisConfig(Map<String, String> redisConfig) {
        this.redisConfig = redisConfig;
    }

    public void init(){
        if(redisConfig==null || redisConfig.isEmpty()){
            throw new IllegalArgumentException("Redis distribute lock factory can not init, because of conifg is null!");
        }
        Properties properties = new Properties();
        redisConfig.entrySet().forEach(entry->properties.put(entry.getKey(), entry.getValue()));
        this.redisExecutor = new RedisExecutor(properties);
    }

    public RedisDistributeLock getRedisDistributeLock(String lockName){
        Objects.requireNonNull(lockName);
        return new RedisDistributeLock(lockName, redisExecutor);
    }

    public RedisDistributeLock getRedisDistributeLock(String lockName, Integer expireTime){
        Objects.requireNonNull(lockName);
        return new RedisDistributeLock(lockName, expireTime, redisExecutor);
    }

}
