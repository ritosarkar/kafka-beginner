package com.learning.beginner.kafka.service;

import com.learning.beginner.kafka.components.ProducerComponents;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Getter
public class KafkaProducerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class.getSimpleName());

    private final KafkaProducer<String, String> kafkaProducer;
    private final ProducerRecord<String, String> producerRecord;
    private final List<ProducerRecord<String, String>> producerRecordList;
    private final List<ProducerRecord<String,String>> producerRecordsWithKey;
    private final ProducerComponents producerComponents;

    @Autowired
    public KafkaProducerService(KafkaProducer<String, String> kafkaProducer,
                                ProducerRecord<String, String> producerRecord,
                                @Qualifier("ProducerRecordBatch")
                                List<ProducerRecord<String, String>> producerRecordList,
                                ProducerComponents producerComponents,
                                @Qualifier("ProducerRecordsWithKey")
                                List<ProducerRecord<String,String>> producerRecordsWithKey){

        this.kafkaProducer=kafkaProducer;
        this.producerRecord=producerRecord;
        this.producerRecordList=producerRecordList;
        this.producerComponents=producerComponents;
        this.producerRecordsWithKey=producerRecordsWithKey;
    }
    public void produceRecord() {
        producerComponents.produceRecordsList(List.of(producerRecord),kafkaProducer,1);
    }

    public int produceBatchRecord() {
        return producerComponents.produceRecordsList(producerRecordList,
                kafkaProducer,
                10);
    }

    public int produceRecordsWithKey() {
        return producerComponents.produceRecordsList(producerRecordsWithKey,
                kafkaProducer,
                2);
    }
}
