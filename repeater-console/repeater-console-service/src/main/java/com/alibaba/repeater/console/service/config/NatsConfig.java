package com.alibaba.repeater.console.service.config;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * @author luguanghua
 */
@Slf4j
@Configuration
@ConditionalOnClass({Nats.class})
@ConditionalOnProperty(name="repeat.use.nats", havingValue = "true")
public class NatsConfig {

    @Value("${broadcaster.repeat.topic}")
    private String repeatTopic;

    @Bean(name = "recordConsumer")
    public Dispatcher initDispatcher() {
        try {
            Options o = new Options.Builder().server("nats://172.19.102.150:4222").maxReconnects(3).build();
            Connection nc = Nats.connect(o);

            return nc.createDispatcher((msg) -> {});
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }
}
