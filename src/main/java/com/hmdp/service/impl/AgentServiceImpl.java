package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.service.IAgentService;
import com.hmdp.service.IBlogService;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_REVIEW_KEY;
import static com.hmdp.utils.SystemConstants.SYSTEM_PROMPT;

@Service
@RequiredArgsConstructor
public class AgentServiceImpl implements IAgentService {

    @Resource
    private IBlogService blogService;

    private final ChatClient chatClient;

    private final ChatMemory chatMemory;

    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public String chat(String message) {

        // 获取登录用户
        UserDTO user = UserHolder.getUser();

        if (user == null) {
            return "请先登录";
        }

        String userId = user.getId().toString();

        try {

            return chatClient.prompt()
                    .system(SYSTEM_PROMPT)
                    .user(message)

                    // 挂载聊天记忆
                    .advisors(
                            new MessageChatMemoryAdvisor(
                                    chatMemory,
                                    userId,
                                    10
                            )
                    )

                    // 注册工具
                    .functions(
                            "queryNearbyShops",
                            "seckillVoucher"
                    )

                    .call()
                    .content();

        } catch (Exception e) {

            e.printStackTrace();

            return "AI助手开小差了，请稍后再试";
        }
    }

    @Override
    public Flux<String> chatStream(String message, Long userId) {
        // 调用大模型并开启流式输出
        try {
            return chatClient.prompt()
                    .system(SYSTEM_PROMPT)
                    .user(message)
                    // 绑定记忆功能，使用userId 作为会话的唯一标识，保留最近10条会话
                    .advisors(new MessageChatMemoryAdvisor(chatMemory, userId.toString(), 10))
                    // 声明该 Agent 可以使用的方法（名称必须与 AgentToolsConfig 中的 Bean 名称一致）
                    .functions("queryNearbyShops", "seckillVoucher", "publishBlog")
                    .stream()
                    .content();
        } catch (Exception e) {
            return Flux.just("AI助手开小差了，请稍后再试");
        }
    }
        @Override
        public String getShopReviewSummary(Long shopId) {
            String cacheKey = CACHE_SHOP_REVIEW_KEY + shopId;

            // 1. 优先查缓存 (高并发下保护大模型不被频繁调用)
            String cachedSummary = stringRedisTemplate.opsForValue().get(cacheKey);
            if (StrUtil.isNotBlank(cachedSummary)) {
                return cachedSummary; // 命中缓存直接返回
            }

            // 2. 缓存未命中，查询 MySQL 最新20条针对该商户的真实探店日记 (Context)
            List<Blog> blogs = blogService.list(new LambdaQueryWrapper<Blog>()
                    .eq(Blog::getShopId, shopId)
                    .orderByDesc(Blog::getCreateTime)
                    .last("LIMIT 20"));

            if (blogs == null || blogs.isEmpty()) {
                return "数据库中暂无该商户的用户真实评价信息。";
            }

            // 提取评价内容拼装成大段文本 (过滤掉空内容)
            String contextData = blogs.stream()
                    .map(Blog::getContent)
                    .filter(StrUtil::isNotBlank)
                    .collect(Collectors.joining("\n---\n"));

            // 3. 构建 Prompt：让大模型充当舆情分析专家进行 RAG 总结
            String ragPrompt = "你是一个专业的美食与商户舆情分析专家。" +
                    "请根据以下我提供的最新【真实用户评价数据】，提取关键信息（如口味特色、排队情况、环境、性价比、服务态度等），" +
                    "并给出一份字数在200字左右的、结构化的短评报告。如果发现差评，请客观指出来。\n\n" +
                    "【真实评价数据如下】：\n" + contextData;

            try {
                // 这里开启一次全新的 ChatClient 调用，仅用于文本分析总结，不携带上下文记忆
                String summary = chatClient.prompt()
                        .user(ragPrompt)
                        .call()
                        .content();

                // 4. 将提炼后的报告写入 Redis 缓存，设置过期时间为 1 天
                if (StrUtil.isNotBlank(summary)) {
                    stringRedisTemplate.opsForValue().set(cacheKey, summary, 1, TimeUnit.DAYS);
                }

                return summary;

            } catch (Exception e) {
                e.printStackTrace();
                return "大模型评价提取失败，分析中断。";
            }
    }
}