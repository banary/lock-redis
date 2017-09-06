package com.banary.lock;

import com.banary.lock.api.DistributeLock;
import com.banary.lock.redis.factory.RedisDistributeLockFactory;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TestCase
 *
 * @author Eden
 * @date 2017/8/17 下午3:43
 */
public class RedisDistributeLockTest extends TestCase {

    private static final Logger logger = LoggerFactory.getLogger(RedisDistributeLockTest.class);

    public Map<String, String> buildRedisConfig(){
        Map<String, String> redisConfig = new HashMap();
        redisConfig.put("password", "redis");
        return redisConfig;
    }

    public RedisDistributeLockFactory buildRedisDistributeLockFactory(Map<String, String> redisConfig){
        //初始化redis分布式锁工厂
        RedisDistributeLockFactory factory = new RedisDistributeLockFactory();
        factory.setRedisConfig(buildRedisConfig());
        factory.init();
        return factory;
    }

    public void testSeriLock(){
        RedisDistributeLockFactory factory = buildRedisDistributeLockFactory(buildRedisConfig());
        DistributeLock lock = factory.getRedisDistributeLock("redisLockKey");
        try{
            lock.lock();
        }finally {
            if(lock!=null){
                lock.unlock();
            }
        }
    }

    public void testPallLock(){
        RedisDistributeLockFactory factory = buildRedisDistributeLockFactory(buildRedisConfig());

        List<String> lockNameList = new ArrayList<>();
        lockNameList.add("redisLockKey");
        lockNameList.add("redisLockKey");
        lockNameList.add("redisLockKey");
        lockNameList.add("redisLockKey1");
        lockNameList.add("redisLockKey2");

        for(String lockName : lockNameList){
            System.out.println(lockName);
            new Thread(()->{
                DistributeLock lock = factory.getRedisDistributeLock(lockName);
                try{
                    lock.lock();
                    try{
                        System.out.println(Thread.currentThread().getName() + System.currentTimeMillis());
                        Thread.currentThread().sleep(60*10000L);
                        System.out.println(Thread.currentThread().getName() + System.currentTimeMillis());
                    }catch (Exception e){
                        logger.error("Current thread sleep error!");
                    }
                }finally {
                    if(lock!=null){
                        lock.unlock();
                    }
                }
            }).start();
        }
        System.out.println("");
    }

}
