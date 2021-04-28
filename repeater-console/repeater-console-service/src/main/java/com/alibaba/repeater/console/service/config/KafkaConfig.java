package com.alibaba.repeater.console.service.config;

import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

/**
 * @author luguanghua
 */
@Slf4j
@Configuration
@ConditionalOnClass({KafkaConsumer.class})
@ConditionalOnProperty(name="repeat.use.kafka", havingValue = "true")
public class KafkaConfig {

    @Value("${broadcaster.record.topic}")
    private String recordTopic;

    @Value("${broadcaster.repeat.topic}")
    private String repeatTopic;

    @Bean(name = "recordConsumer")
    public KafkaConsumer<String, String> recordConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(recordTopic));

        return consumer;
    }

    @Bean(name = "repeatConsumer")
    public KafkaConsumer<String, String> repeatConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(repeatTopic));

        return consumer;
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        try (InputStream is = PropertyUtil.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            props.load(is);
        } catch (IOException e) {
            log.info(e.getMessage(), e);
        }

        return props;
    }
}
