package com.hmdp.service;

import reactor.core.publisher.Flux;

public interface IAgentService {

    String chat(String message);


    Flux<String> chatStream(String message, Long userId);
}
