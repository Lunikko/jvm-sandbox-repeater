package com.alibaba.repeater.console.service.impl;

import com.alibaba.repeater.console.service.KafkaConsumeService;
import com.alibaba.repeater.console.service.RecordService;
import com.alibaba.repeater.console.service.ReplayService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.concurrent.*;

/**
 * @author luguanghua
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "repeat.use.kafka", havingValue = "true")
public class KafkaConsumeServiceImpl implements KafkaConsumeService {

    @Resource
    private RecordService recordService;

    @Resource
    private ReplayService replayService;

    @Resource(name = "recordConsumer")
    private KafkaConsumer<String, String> recordConsumer;

    @Resource(name = "repeatConsumer")
    private KafkaConsumer<String, String> repeatConsumer;

    private ExecutorService executorService = null;

    @PostConstruct
    public void start() {
        executorService = new ThreadPoolExecutor(2, 2,
                5L, TimeUnit.MINUTES, new LinkedBlockingDeque<>(2),
                new BasicThreadFactory.Builder().namingPattern("kafka-consumer-pool-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        consumerRecord();
        consumerRepeat();

        log.info("KafkaConsumeService started.");
    }

    @Override
    public void consumerRecord() {
        executorService.submit(() -> {
            while (true) {
                ConsumerRecords<String, String> records = recordConsumer.poll(Duration.ofMillis(25));
                if (records.isEmpty()) {
                    continue;
                }
                log.info("records size: {}", records.count());
                records.forEach(record -> {
                    log.info("save record traceId={}", record.key());
                    recordService.saveRecord(record.value());
                });

                recordConsumer.commitAsync();
            }
        });
    }

    @Override
    public void consumerRepeat() {
        executorService.submit(() -> {
            while (true) {
                ConsumerRecords<String, String> records = repeatConsumer.poll(Duration.ofMillis(25));
                if (records.isEmpty()) {
                    continue;
                }
                log.info("records size: {}", records.count());
                records.forEach(record -> {
                    log.info("save repeat traceId={}", record.key());
                    replayService.saveRepeat(record.value());
                });

                repeatConsumer.commitAsync();
            }
        });
    }
}
