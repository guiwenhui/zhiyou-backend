package com.hmdp.service.impl;

import com.hmdp.dto.UserDTO;
import com.hmdp.service.IAgentService;
import com.hmdp.utils.UserHolder;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import static com.hmdp.utils.SystemConstants.SYSTEM_PROMPT;

@Service
@RequiredArgsConstructor
public class AgentServiceImpl implements IAgentService {

    private final ChatClient chatClient;

    private final ChatMemory chatMemory;

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


}