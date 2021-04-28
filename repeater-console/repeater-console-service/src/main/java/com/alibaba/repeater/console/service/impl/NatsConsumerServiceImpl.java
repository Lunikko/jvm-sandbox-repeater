package com.alibaba.repeater.console.service.impl;

import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.repeater.console.service.MsgConsumeService;
import com.alibaba.repeater.console.service.RecordService;
import com.alibaba.repeater.console.service.ReplayService;
import io.nats.client.Dispatcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

/**
 * @author luguanghua
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "repeat.use.nats", havingValue = "true")
public class NatsConsumerServiceImpl implements MsgConsumeService {

    @Value("${broadcaster.record.topic}")
    private String recordTopic;

    @Value("${broadcaster.repeat.topic}")
    private String repeatTopic;

    @Resource
    private Dispatcher dispatcher;

    @Resource
    private RecordService recordService;

    @Resource
    private ReplayService replayService;

    private static final String QUEUE_NAME = "repeater-console";

    @PostConstruct
    public void start() {
        consumerRecord();
        consumerRepeat();

        log.info("NatsConsumerService started.");
    }

    @Override
    public void consumerRecord() {
        dispatcher.subscribe(recordTopic, QUEUE_NAME, msg -> {
            String record = new String(msg.getData(), StandardCharsets.UTF_8);
            if (record.contains(Constants.MSG_SEPARATOR)) {
                String[] arr = record.split(Constants.MSG_SEPARATOR, 2);
                log.info("save repeat traceId={}", arr[0]);
                recordService.saveRecord(arr[1]);
            } else {
                log.warn("Invalid record msg: {}", record);
            }
        });
    }

    @Override
    public void consumerRepeat() {
        dispatcher.subscribe(repeatTopic, QUEUE_NAME, msg -> {
            String repeat = new String(msg.getData(), StandardCharsets.UTF_8);
            if (repeat.contains(Constants.MSG_SEPARATOR)) {
                String[] arr = repeat.split(Constants.MSG_SEPARATOR, 2);
                log.info("save repeat traceId={}", arr[0]);
                replayService.saveRepeat(arr[1]);
            } else {
                log.warn("Invalid repeat msg: {}", repeat);
            }
        });
    }
}
