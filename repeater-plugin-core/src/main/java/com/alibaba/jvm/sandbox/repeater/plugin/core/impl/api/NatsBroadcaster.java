package com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api;

import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.BaseBroadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RecordModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeatModel;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author luguanghua
 */
public class NatsBroadcaster extends BaseBroadcaster {


    /**
     * 录制消息投递Topic
     */
    private String natsUrl = PropertyUtil.getPropertyOrDefault(Constants.REPEAT_NATS_URL, "");

    /**
     * 录制消息投递Topic
     */
    private String broadcastRecordTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_RECORD_TOPIC, "repeater-record");

    /**
     * 回放消息投递Topic
     */
    private String broadcastRepeatTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_REPEAT_TOPIC, "repeater-repeat");

    private static NatsBroadcaster instance = null;

    private Connection nc = null;

    private NatsBroadcaster() {
        try {
            Options o = new Options.Builder().server(natsUrl).maxReconnects(3).build();
            nc = Nats.connect(o);
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static NatsBroadcaster getInstance() {
        synchronized (NatsBroadcaster.class) {
            if (instance == null) {
                instance = new NatsBroadcaster();
            }
        }

        return instance;
    }

    @Override
    public void sendRecord(RecordModel recordModel) {
        RecordWrapper wrapper = new RecordWrapper(recordModel);
        broadcast(wrapper, recordModel.getTraceId(), broadcastRecordTopic);
    }

    @Override
    public void sendRepeat(RepeatModel repeatModel) {
        broadcast(repeatModel, repeatModel.getTraceId(), broadcastRepeatTopic);
    }

    private void broadcast(Object model, String traceId, String topic) {
        try {
            nc.publish(topic, traceId.concat(Constants.MSG_SEPARATOR).concat(SerializerWrapper.hessianSerialize(model)).getBytes(StandardCharsets.UTF_8));
        } catch (SerializeException e) {
            log.error(e.getMessage(), e);
        }
    }
}
