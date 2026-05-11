package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 配置Redisson客户端
 */
@Configuration
public class RedisConfig {
    @Bean
    public RedissonClient redissonClient(){
        //配置类
        Config config = new Config();
        //添加单点地址及密码
        config.useSingleServer()
                .setAddress("redis://192.168.88.130:6379").setPassword("Sun521123456.");
        //创建客户端
        return Redisson.create(config);
    }
}
