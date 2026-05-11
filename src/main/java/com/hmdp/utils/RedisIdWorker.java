package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private StringRedisTemplate stringRedisTemplate;
    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //开始时间戳
    private static final long BEGIN_TIMESTAMP = 1775497325L;
    //序列号的位数
    private static final int COUNT_BITS = 31;

    public long nextId(String keyPrefix) {
        //1.生成时间戳
        LocalDateTime localDateTime = LocalDateTime.now();
        long nowSeconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSeconds - BEGIN_TIMESTAMP;
        //2.生成序列号
        //2.1获取当天日期，精确到天
        String date = localDateTime.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2自增长
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //3.接收并返回
        return timestamp << COUNT_BITS | count;
    }
}
