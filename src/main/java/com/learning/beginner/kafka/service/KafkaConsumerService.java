package com.learning.beginner.kafka.service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@Getter
@RequiredArgsConstructor
public class KafkaConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class.getSimpleName());

    private final KafkaConsumer<String, String> kafkaConsumer;

    public void pollData() {
        try {
            log.info("From Consumer it started Polling.......");
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {} || value: {}", record.key(), record.value());
                    log.info("Partition: {} || offset: {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown!!");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            log.info("Executing finally to close consumer");
            kafkaConsumer.close();
        }

    }
}
