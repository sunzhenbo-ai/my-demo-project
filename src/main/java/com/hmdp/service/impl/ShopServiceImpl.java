package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.redisson.api.GeoUnit;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺详情数据
     */
    @Override
    public Result queryById(Long id) {
        //1缓存穿透
        //1.1手写方法
        // Shop shop = queryWithPassThrough(id);
        //1.2工具类
        //Shop shop = cacheClient.queryWithPassThrough(
        //        CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        //2用互斥锁解决缓存击穿(在缓存穿透的基础上)
        Shop shop = queryWithMutex(id);

        //3用逻辑过期时间来解决缓存击穿问题
        //3.1手写方法
        //Shop shop = queryWithLogicalExpire(id);
        //3.2工具类
        //Shop shop = cacheClient.queryWithLogicalExpire(
        //        CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }

    //创建一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 用逻辑过期时间来解决缓存击穿问题
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1先从缓存中查取
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3不存在直接返回
            return null;
        }
        //4存在，需要将JSON反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(JSONUtil.toJsonStr(data), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5判断缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期返回店铺信息
            return shop;
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
                return shop;
            }
            //6.4成功开启独立线程,实现数据重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    this.saveShop2Redis(id,1800L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    this.unlock(lockKey);
                }
            });
        }

        //6.5返回过期的信息
        return shop;
    }


    /**
     * 用互斥锁解决缓存击穿(在缓存穿透的基础上)
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1先从缓存中查取
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            //2存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //3判断命中是否为空值
        if(shopJson != null){
            return null;
        }
        //4实现缓存存重建
        //4.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否拿到互斥锁
            if(!isLock){
                //4.3失败则休眠后重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4再从缓存中查取,避免首个线程修改后释放锁了，其他线程拿到锁而重复操作
            String shopJson2 = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(shopJson2)){
                //存在直接返回
                return JSONUtil.toBean(shopJson2, Shop.class);
            }
            if(shopJson2 != null){
                return null;
            }
            //4.5获取锁成功，在则在数据库中查取
            shop = getById(id);
            //模拟重建延迟
            Thread.sleep(200);
            //5没有查到
            if(shop == null){
                //将空值写入Redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //6查到则存入Redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL + RandomUtil.randomLong(0,10), TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7释放互斥锁
            unlock(lockKey);
        }
        //8返回
        return shop;
    }

    /**
     * 解决缓存穿透的方法代码
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        //先从缓存中查取
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            //存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中是否为空值
        if(shopJson != null){
            return null;
        }
        //不存在则在数据库中查取
        Shop shop = getById(id);
        //没有查到
        if(shop == null){
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //查到则存入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),
                CACHE_SHOP_TTL + RandomUtil.randomLong(0,10), TimeUnit.MINUTES);
        //返回
        return shop;
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

    /**
     * 将热点key存入Redis,设置逻辑过期时间
     * @param id
     * @param expireSeconds
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        //模拟延迟
        Thread.sleep(200);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //导入至Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    /**
     * 更新商铺信息
     * @param shop 商铺数据
     * @return 无
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("id不能为空");
        }
        String key = CACHE_SHOP_KEY + id;
        //先更新数据库
        updateById(shop);
        //再删缓存
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1判断是否需要根据坐标查询
        if(x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        }
        //2计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3查询Redis，按照距离排序分页，结果:shopId distance
        String key = SHOP_GEO_KEY + typeId;
        //GEOSEARCH KEY BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x,y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        //4解析id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            //没有下一页了
            return Result.ok(Collections.emptyList());
        }
        //4.1截取from到end部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //4.2获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        //5根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6返回
        return Result.ok(shops);
    }
}
