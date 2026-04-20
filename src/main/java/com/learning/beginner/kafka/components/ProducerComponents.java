package com.learning.beginner.kafka.components;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProducerComponents {

    private static final Logger log = LoggerFactory.getLogger(ProducerComponents.class.getSimpleName());

    public int produceRecordsList(List<ProducerRecord<String, String>> producerRecords,
                                  KafkaProducer<String, String> kafkaProducer,
                                  int rounds) {
        int k = 0;
        for (int j = 0; j < rounds; j++) {
            producerRecords.
                    forEach(e -> producingRecord(e, kafkaProducer));
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Error  while producing record..", e);
            }
            k++;
        }
        sendAndClose(kafkaProducer);
        return (k * producerRecords.size());
    }

    public void sendAndClose(KafkaProducer<String, String> kafkaProducer) {
        //tell the producer to send all data and block until done -- synchronous
        kafkaProducer.flush();
        //close producer
        kafkaProducer.close();
        log.info("Data has been sent successfully.");
    }


    public void producingRecord(ProducerRecord<String, String> producerRecord,
                                KafkaProducer<String, String> kafkaProducer) {
        //send data
        log.info("Sending data - {}", producerRecord.value());
        var key = producerRecord.key() != null ? producerRecord.key() : "nullKey";
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                if (key.equals("nullKey")) {
                    log.info("\nReceived new metadata-\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp());
                } else {
                    log.info("Key: {}|Partition: {}",
                            key,
                            recordMetadata.partition());
                }
            } else {
                log.error("Error while producing. Error occurred -- \n", e);
            }
        });
    }
}
