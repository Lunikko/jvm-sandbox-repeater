package com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api;

import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.BaseBroadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RecordModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeatModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author luguanghua
 */
public class KafkaBroadcaster extends BaseBroadcaster {

    /**
     * 录制消息投递Topic
     */
    private String broadcastRecordTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_RECORD_TOPIC, "repeater-record");

    /**
     * 回放消息投递Topic
     */
    private String broadcastRepeatTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_REPEAT_TOPIC, "repeater-repeat");

    private LazyProducer lazyProducer = null;

    public KafkaBroadcaster() {
        lazyProducer = new LazyProducer();
    }


    @Override
    public void sendRecord(RecordModel recordModel) {
        RecordWrapper wrapper = new RecordWrapper(recordModel);
        broadcast(wrapper, recordModel.getTraceId(), broadcastRecordTopic, recordModel.getTimestamp());
    }

    @Override
    public void sendRepeat(RepeatModel repeatModel) {
        broadcast(repeatModel, repeatModel.getTraceId(), broadcastRepeatTopic, System.currentTimeMillis());
    }

    private void broadcast(Object model, String traceId, String topic, Long timestamp) {
        try {
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, timestamp, traceId, SerializerWrapper.hessianSerialize(model));
            final Producer<String, String> producer = lazyProducer.get();
            producer.send(producerRecord, ((metadata, exception) -> {
                if (exception != null) {
                    log.error("broadcast failed, traceId={}, msg={}, resp={}", traceId, exception.getMessage(), metadata);
                } else {
                    log.info("broadcast success, traceId={}, resp={}", traceId, metadata);
                }
            }));
        } catch (SerializeException e) {
            log.error(e.getMessage(), e);
        }
    }

    private class LazyProducer {
        private volatile Producer<String, String> producer;

        public Producer<String, String> get() {
            Producer<String, String> result = this.producer;
            if (result == null) {
                synchronized (this) {
                    result = this.producer;
                    if (result == null) {
                        this.producer = result = this.initialize();
                    }
                }
            }

            return result;
        }

        private Producer<String, String> initialize() {
            Producer<String, String> tmp = null;
            try {
                tmp = createProducer();
            } catch (Exception e) {
                log.error("error creating producer: {}", e.getMessage(), e);
            }

            return tmp;
        }

        private Producer<String, String> createProducer() {
            return new KafkaProducer<>(PropertyUtil.getProducerProperties(), new StringSerializer(), new StringSerializer());
        }

        public boolean isInitialized() {
            return this.producer != null;
        }
    }
}
