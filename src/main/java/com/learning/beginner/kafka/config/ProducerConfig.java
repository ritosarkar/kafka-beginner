package com.learning.beginner.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@Configuration
@RequiredArgsConstructor
public class ProducerConfig {

    private final AppConfiguration appConfiguration;
    private final Topics topics;

    @Bean
    public KafkaProducer<String, String> getProducerProperties() {
        Properties properties = new Properties();
        //connect to localhost 172.18.0.3 or 127.0.0.1
        properties.setProperty("bootstrap.servers", appConfiguration.getBootstrapServers());
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        /*
        properties.setProperty("batch.size","400");
        properties.setProperty("partitioner.class",RoundRobinPartitioner.class.getName());
        */
        return new KafkaProducer<>(properties);
    }

    @Bean
    public ProducerRecord<String, String> getConfigProducerRecord() {
        return new ProducerRecord<>(topics.getTopic(), "with callback implementation in producer!");
    }

    @Bean(name = "ProducerRecordBatch")
    public List<ProducerRecord<String, String>> getProducerRecordBatch() {
        List<ProducerRecord<String, String>> producerRecordList = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            producerRecordList.add(new ProducerRecord<>(topics.getTopic(), "Sending record number: " + i));
        }
        return producerRecordList;
    }

    @Bean(name = "ProducerRecordsWithKey")
    public List<ProducerRecord<String, String>> getProducerRecordsWithKey() {
        List<ProducerRecord<String, String>> producerRecordList = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            var key = "id_" + i;
            var value = "hello sending record number: " + i;
            producerRecordList.add(new ProducerRecord<>(topics.getTopicOne(), key, value));
        }
        return producerRecordList;
    }
}
