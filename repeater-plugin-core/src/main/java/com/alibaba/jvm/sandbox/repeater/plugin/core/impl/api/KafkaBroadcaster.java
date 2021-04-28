package com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.api.Broadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.HttpUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author luguanghua
 */
public class KafkaBroadcaster implements Broadcaster {

    private static final Logger log = LoggerFactory.getLogger(KafkaBroadcaster.class);

    /**
     * 录制消息投递Topic
     */
    private String broadcastRecordTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_RECORD_TOPIC, "repeater-record");

    /**
     * 回放消息投递Topic
     */
    private String broadcastRepeatTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_REPEAT_TOPIC, "repeater-repeat");

    /**
     * 回放消息拉取URL
     */
    private String pullRecordUrl = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_REPEAT_DATASOURCE, "");

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

    @Override
    public RepeaterResult<RecordModel> pullRecord(RepeatMeta meta) {
        String url;
        if (StringUtils.isEmpty(meta.getDatasource())) {
            url = String.format(pullRecordUrl, meta.getAppName(), meta.getTraceId());
        } else {
            url = meta.getDatasource();
        }
        final HttpUtil.Resp resp = HttpUtil.doGet(url);
        if (!resp.isSuccess() || StringUtils.isEmpty(resp.getBody())) {
            log.info("get repeat data failed, datasource={}, response={}", meta.getDatasource(), resp);
            return RepeaterResult.builder().success(false).message("get repeat data failed").build();
        }
        RepeaterResult<String> pr = JSON.parseObject(resp.getBody(), new TypeReference<RepeaterResult<String>>() {
        });
        if (!pr.isSuccess()) {
            log.info("invalid repeat data found, datasource={}, response={}", meta.getDatasource(), resp);
            return RepeaterResult.builder().success(false).message("repeat data found").build();
        }
        // swap classloader cause this method will be call in target app thread
        ClassLoader swap = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(DefaultBroadcaster.class.getClassLoader());
            RecordWrapper wrapper = SerializerWrapper.hessianDeserialize(pr.getData(), RecordWrapper.class);
            SerializerWrapper.inTimeDeserialize(wrapper.getEntranceInvocation());
            if (meta.isMock() && CollectionUtils.isNotEmpty(wrapper.getSubInvocations())) {
                for (Invocation invocation : wrapper.getSubInvocations()) {
                    SerializerWrapper.inTimeDeserialize(invocation);
                }
            }
            return RepeaterResult.builder().success(true).message("operate success").data(wrapper.reTransform()).build();
        } catch (SerializeException e) {
            return RepeaterResult.builder().success(false).message(e.getMessage()).build();
        } finally {
            Thread.currentThread().setContextClassLoader(swap);
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
