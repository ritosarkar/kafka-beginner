package com.learning.beginner.kafka.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class Topics {

    private final AppConfiguration appConfiguration;
    private final String topic ;
    private final String topicOne ;

    @Autowired
    public Topics( AppConfiguration appConfiguration){
        this.appConfiguration=appConfiguration;
        topic = appConfiguration.getTopic().getFirst();
        topicOne = appConfiguration.getTopic().get(1);
    }

}
