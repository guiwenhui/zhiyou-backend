package com.hmdp.config;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.UserHolder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.InMemoryChatMemory;

import jakarta.annotation.Resource;
import java.util.function.Function;

@Configuration
public class AgentToolsConfig {

    @Bean
    public ChatMemory chatMemory() {
        return new InMemoryChatMemory();
    }

    @Bean
    public ChatClient chatClient(ChatClient.Builder builder) {
        return builder.build();
    }

    @Resource
    private IShopService shopService;

    @Resource
    private IBlogService blogService;

    @Resource
    private IVoucherOrderService voucherOrderService;

    // 1. 定义入参 Record 类 (必须是确定的结构)
    public record ShopQueryRequest(Integer typeId, Double x, Double y) {}
    public record SeckillRequest(Long voucherId) {}
    public record PublishBlogRequest(String content, String images, Long shopId) {}

    // 2. 注册查询附近商户的 Tool
    @Bean
    @Description("这是一个查询本地商户的工具。当用户想寻找附近的美食、餐厅、酒店等店铺时必须调用此工具。需要传入分类的typeId(美食通常是1)以及用户的经度x和纬度y。")
    public Function<ShopQueryRequest, String> queryNearbyShops() {
        return request -> {
            // 复用黑马点评原有的 Redis Geo 查询逻辑
            Result result = shopService.queryShopByType(request.typeId(), 1, request.x(), request.y());
            if (result.getSuccess()) {
                // 将结果转为 JSON 给大模型解析
                return JSONUtil.toJsonStr(result.getData());
            }
            return "查询失败，暂无数据";
        };
    }

    // 3. 注册秒杀抢券的 Tool
    @Bean
    @Description("这是一个抢购代金券/优惠券的工具。当用户明确表示要购买、抢或者下单某张优惠券时调用。必须传入优惠券的 voucherId。")
    public Function<SeckillRequest, String> seckillVoucher() {
        return request -> {
            try {
                // 复用原有的秒杀逻辑 (包含 Lua脚本扣库存 + Redisson分布式锁)
                Result result = voucherOrderService.seckillVoucher(request.voucherId());
                return result.getSuccess() ?
                        "抢券成功！订单号为：" + result.getData() :
                        "抢券失败，原因：" + result.getErrorMsg();
            } catch (Exception e) {
                return "抢券系统繁忙，请稍后再试。";
            }
        };
    }

    @Bean
    @Description("发布探店日记工具。当用户要求写探店日记并发布时调用。你需要先构思好高质量内容并排版，然后调用此工具。")
    public Function<PublishBlogRequest, String> publishBlog() {
        return request -> {
            try {
                // 注意：由于 AI 回调 Tool 时可能丢失 ThreadLocal 上下文，
                // 如果这里 UserHolder 拿不到 ID，需要将 UserId 设计到 PublishBlogRequest 参数中让大模型回传。
                Long userId = UserHolder.getUser().getId();

                Blog blog = new Blog();
                blog.setUserId(userId);
                blog.setShopId(request.shopId());
                blog.setContent(request.content());
                blog.setImages(request.images() != null ? request.images() : "");

                blogService.saveBlog(blog);
                return "探店笔记发布成功！";
            } catch (Exception e) {
                return "发布失败：" + e.getMessage();
            }
        };
    }
}