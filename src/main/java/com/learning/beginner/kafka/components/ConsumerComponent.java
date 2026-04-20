package com.learning.beginner.kafka.components;

import com.learning.beginner.kafka.config.ConsumerConfig;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.apache.catalina.core.ApplicationContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ConsumerComponent {

    private static final Logger log = LoggerFactory.getLogger(ConsumerComponent.class.getSimpleName());
    private final KafkaConsumer<String, String> kafkaConsumer;

    @PreDestroy
    public void onShutDown() {
        log.info("Running pre destroy task as shutdown signal received!!");
        kafkaConsumer.wakeup();
        log.info("Closing kafka consumer!!");
        kafkaConsumer.close();
        log.info("Consumer is now gracefully shutdown!!!");
    }
}
