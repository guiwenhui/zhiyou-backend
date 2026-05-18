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

import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
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
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        List<String> shopTypes =
                stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);
        if (!shopTypes.isEmpty()) {
            List<ShopType> tep = new ArrayList<>();
            for (String types : shopTypes){
                ShopType shopType = JSONUtil.toBean(types, ShopType.class);
                tep.add(shopType);
            }
            return Result.ok(tep);
        }
        List<ShopType> tep = query().orderByAsc("sort").list();
        if (tep == null) {
            return Result.fail("店铺类型不存在");
        }
        for (ShopType shopType : tep) {
            String jsonStr = JSONUtil.toJsonStr(shopType);
            shopTypes.add(jsonStr);
        }
        stringRedisTemplate.opsForList().leftPushAll(CACHE_SHOP_TYPE_KEY, shopTypes);
        return Result.ok(tep);
    }
}