package com.learning.beginner.kafka.controller;

import com.learning.beginner.kafka.service.KafkaConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ConsumerController {

    private final KafkaConsumerService kafkaConsumerService;

    @GetMapping("/readMessages")
    public void  readFromTopics(){
        kafkaConsumerService.pollData();
    }

}
