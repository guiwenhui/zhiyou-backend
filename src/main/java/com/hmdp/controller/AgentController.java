package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.utils.UserHolder;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.InMemoryChatMemory;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/agent")
public class AgentController {

    private final ChatClient chatClient;
    private final ChatMemory chatMemory;

    // 构造器注入
    public AgentController(ChatClient.Builder chatClientBuilder) {
        this.chatMemory = new InMemoryChatMemory(); // 本地测试先用内存
        this.chatClient = chatClientBuilder.build();
    }

    @PostMapping("/chat")
    public Result chat(@RequestParam String message) {
        // 获取当前登录用户 ID (依赖原来的登录拦截器)
        String userId = UserHolder.getUser().getId().toString();

        // 设定系统 Prompt，赋予 Agent 人设
        String systemPrompt = "你是一个名为'智点'的本地生活AI助手。你的任务是帮助用户寻找好店、规划游玩路线以及抢购优惠券。" +
                "请严格基于你拥有的函数工具来回答问题，不要凭空捏造商户信息。";

        try {
            // 调用大模型
            String aiResponse = chatClient.prompt()
                    .system(systemPrompt)
                    .user(message)
                    // 1. 挂载记忆体，按用户隔离上下文 (保留最近10轮对话)
                    .advisors(new MessageChatMemoryAdvisor(chatMemory, userId, 10))
                    // 2. 注册刚刚在 AgentToolsConfig 中定义的 Bean 名称
                    .functions("queryNearbyShops", "seckillVoucher")
                    .call()
                    .content();

            return Result.ok(aiResponse);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.fail("AI助手开小差了，请稍后再试");
        }
    }
}