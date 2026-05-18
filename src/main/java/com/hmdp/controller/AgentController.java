package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.service.IAgentService;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.Resource;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.awt.*;

@RestController
@RequestMapping("/agent")
public class AgentController {

    @Resource
    private IAgentService agentService;

    @PostMapping("/chat")
    public Result chat(@RequestParam String message) {
        String result = agentService.chat(message);
        return Result.ok(result);
    }

    @GetMapping(value = "/chat/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> chatStream(@RequestParam String message) {
        Long userId = UserHolder.getUser().getId();
        return agentService.chatStream(message, userId);
    }
}