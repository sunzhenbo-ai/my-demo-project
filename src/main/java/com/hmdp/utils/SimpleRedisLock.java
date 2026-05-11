package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private String name;
    private StringRedisTemplate redisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate redisTemplate) {
        this.name = name;
        this.redisTemplate = redisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true);
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setResultType(Long.class);
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String id = ID_PREFIX + Thread.currentThread().getId();
        //获取锁
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                KEY_PREFIX + name, id, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    /*
    @Override
    public void unlock() {
        //获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取琐中的标识
        String id = redisTemplate.opsForValue().get(KEY_PREFIX + name);
        //判断标识是否一致
        if (threadId.equals(id)) {
            //释放锁
            redisTemplate.delete(KEY_PREFIX + name);
        }
    }
     */

    //上述方法依旧不完善，可能会在判断后阻塞，然后锁过期导致误删，可用Lua脚本进行完善
    @Override
    public void unlock() {
        //调用Lua脚本
        redisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }
}
