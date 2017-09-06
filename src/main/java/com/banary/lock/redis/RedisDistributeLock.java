package com.banary.lock.redis;

import com.banary.lock.api.AbstractDistributeLock;
import com.banary.lock.redis.pool.RedisExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Objects;
import java.util.UUID;

/**
 * redis分布式锁
 *
 * @author Eden
 * @date 2017/8/13 上午12:40
 */
public class RedisDistributeLock extends AbstractDistributeLock {

    private static Logger logger = LoggerFactory.getLogger(RedisDistributeLock.class);

    public static final String NX = "NX";

    public static final String EX = "EX";

    public static final String OK = "OK";

    //以毫秒为单位
    public static final long SLEEP_MILLS = 2000;

    //以秒为单位
    public static final int EXPIRE_SECONDS = 60*10;

    private transient RedisExecutor redisExecutor;
    private transient String uuid;
    private transient Integer expireTime;
    private transient Boolean isLock;

    public RedisDistributeLock(String lockName, RedisExecutor redisExecutor){
        this(lockName, EXPIRE_SECONDS, redisExecutor);
    }

    public RedisDistributeLock(String lockName, Integer expireTime, RedisExecutor redisExecutor){
        Objects.requireNonNull(lockName);
        Objects.requireNonNull(redisExecutor);
        this.setLockName(lockName);
        this.setExclusiveOwnerThread(Thread.currentThread());
        this.expireTime = expireTime;
        this.redisExecutor = redisExecutor;
    }

    /**
     * 阻塞式锁
     *
     * @author Eden
     * @date 2017/8/15 上午10:39
     * @param
     * @return
     */
    public void lock() {
        this.redisExecutor.execute((Jedis jedis)->{
            if(logger.isDebugEnabled()){
                logger.debug("Start to get redis block lock.");
            }
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
            while (true){
                String result = jedis.set(getLockName(), uuid, NX, EX, expireTime);
                //获得锁成功
                if(OK.equals(result)){
                    //同步JVM中的锁信息
                    this.uuid = uuid;
                    this.isLock = true;
                    if(logger.isDebugEnabled()){
                        logger.debug("Get the redis block lock success.lockName：{}, value:{}", getLockName(), uuid);
                    }
                    return true;
                }
                logger.warn("Get redis block lock failure, current thread go to sleep.");
                //休眠重试
                sleepMills(SLEEP_MILLS);
            }
        });
    }

    private void sleepMills(Long mills){
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void unlock() {
        if(Thread.currentThread() != getExclusiveOwnerThread()){
            throw new IllegalMonitorStateException();
        }
        this.redisExecutor.execute((Jedis jedis)->{
            if(logger.isDebugEnabled()){
                logger.debug("Start to unlock the redis lock.lockName:{}, value:{}", getLockName(), uuid);
            }
            if(!isLock){
                logger.warn("Current redis lock is unlock!lockName:{}, value:{}", getLockName(), uuid);
                return true;
            }
            //校验是否是自己写入redis中的锁
            String result = jedis.get(getLockName());
            if(!uuid.equals(result)){
                logger.warn("Current redis lock is unlock!lockName:{}, value:{}", getLockName(), uuid);
                return true;
            }
            //删除锁
            jedis.del(getLockName());
            this.isLock=false;
            if(logger.isDebugEnabled()){
                logger.debug("Unlock the redis lock success.lockName:{}, value:{}", getLockName(), uuid);
            }
            return true;
        });
    }

    /**
     * 非阻塞式锁实现
     *
     * @author Eden
     * @date 2017/8/15 上午10:51
     * @param
     * @return
     */
    public boolean tryLock() {
        return this.redisExecutor.execute((Jedis jedis)->{
            if(logger.isDebugEnabled()) {
                logger.debug("Start to get redis unblock lock.");
            }
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
                String result = jedis.set(getLockName(), uuid, NX, EX, expireTime);
                //获得锁成功
                if(OK.equals(result)){
                    //同步JVM中的锁信息
                    this.uuid = uuid;
                    this.isLock = true;
                    if(logger.isDebugEnabled()){
                        logger.debug("Get the redis unblock lock success.lockName：{}, value:{}", getLockName(), uuid);
                    }
                    return true;
                }
            if(logger.isDebugEnabled()) {
                logger.debug("Get redis unblock lock failure!");
            }
            return false;
        });
    }

}
