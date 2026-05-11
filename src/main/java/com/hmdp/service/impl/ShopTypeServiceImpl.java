package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询商铺类型
     * @return
     */
    @Override
    public Result queryList() {
        String shopList = stringRedisTemplate.opsForValue().get(SHOP_TYPE_KEY);
        if(StrUtil.isNotBlank(shopList)) {
            List<ShopType> typeList = JSONUtil.toList(shopList,ShopType.class);
            return Result.ok(typeList);
        }
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if(typeList == null || typeList.isEmpty()) {
            return Result.fail("店铺信息错误");
        }
        stringRedisTemplate.opsForValue().set(SHOP_TYPE_KEY, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
