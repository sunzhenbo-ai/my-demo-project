package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 工具类
 */
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    //利用无参构造器来强制注入StringRedisTemplate对象
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 将java对象通过string类型存储至Redis，并设置过期时间
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    /**
     * 将java对象通过string类型存储至Redis，并设置逻辑过期时间
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        //设置逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 解决缓存穿透的方法代码
     * @param id
     * @return
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> function, Long time, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        //先从缓存中查取
        String json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(json)){
            //存在直接返回
            return JSONUtil.toBean(json, type);
        }
        //判断命中是否为空值
        if(json != null){
            return null;
        }
        //不存在则在数据库中查取
        R r = function.apply(id);
        //没有查到
        if(r == null){
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //查到则存入Redis
        set(key, r, time, timeUnit);
        //返回
        return r;
    }

    //创建一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 用逻辑过期时间来解决缓存击穿问题
     * @param id
     * @return
     */
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> function, Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;
        //1先从缓存中查取
        String json = stringRedisTemplate.opsForValue().get(key);
        //2判断是否存在
        if(StrUtil.isBlank(json)){
            //3不存在直接返回
            return null;
        }
        //4存在，需要将JSON反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(JSONUtil.toJsonStr(data), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5判断缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期返回店铺信息
            return r;
        }
        //5.2已过期,需要缓存重建
        //6缓存重建
        //6.1获取锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //6.2判断获取锁是否成功
        if(isLock){
            //6.3获取锁成功后再次检查redis缓存是否过期(避免前面线程已经刷新过了Redis缓存而重复操作)
            if(expireTime.isAfter(LocalDateTime.now())){
                //未过期返回店铺信息
                return r;
            }
            //6.4成功开启独立线程,实现数据重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    //查询数据库
                    R r1 = function.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r,time,timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    this.unlock(lockKey);
                }
            });
        }

        //6.5返回过期的信息
        return r;
    }

    /**
     * 获取锁的方法
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁方法
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
