package com.learning.beginner.kafka.controller;

import com.learning.beginner.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/sendData")
    public String sendDataToTopic(){
        kafkaProducerService.produceRecord();
        return "Data has been published!!";
    }

    @PostMapping("/sendBatches")
    public String sendBatchesToTopic(){
        return "Batch of "+kafkaProducerService.produceBatchRecord()+" records has been processed!!";
    }

    @PostMapping("/sendMessageWithKey")
    public String sendMessageWithKey(){
        return "Batch of "+kafkaProducerService.produceRecordsWithKey()+" records has been processed!!";
    }

}
