package com.learning.beginner.kafka.config;


import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class ConsumerConfig {
    private final AppConfiguration appConfiguration;

    @Bean
    public KafkaConsumer<String, String> getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", appConfiguration.getBootstrapServers());
        properties.setProperty("group.id", appConfiguration.getConsumer().getGroupId());
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", appConfiguration.getConsumer().getAutoOffsetReset());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(appConfiguration.getTopic());
        return kafkaConsumer;
    }
}
