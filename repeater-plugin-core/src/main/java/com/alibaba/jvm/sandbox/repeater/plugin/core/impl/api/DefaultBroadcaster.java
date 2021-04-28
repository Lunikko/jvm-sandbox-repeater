package com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api;

import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.AbstractBroadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.HttpUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.HttpUtil.Resp;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RecordModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeatModel;
import com.google.common.collect.Maps;

import java.util.HashMap;

/**
 * {@link DefaultBroadcaster} 默认的Http方式的消息投递实现
 *
 * @author zhaoyb1990
 */
public class DefaultBroadcaster extends AbstractBroadcaster {

    /**
     * 录制消息投递的URL
     */
    private String broadcastRecordUrl = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_RECORD_BROADCASTER, "");

    /**
     * 回放消息投递URL
     */
    private String broadcastRepeatUrl = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_REPEAT_BROADCASTER, "");

    public DefaultBroadcaster() {
        super();
    }

    @Override
    protected void broadcastRecord(RecordModel recordModel) {
        try {
            RecordWrapper wrapper = new RecordWrapper(recordModel);
            String body = SerializerWrapper.hessianSerialize(wrapper);
            broadcast(broadcastRecordUrl, body, recordModel.getTraceId());
        } catch (SerializeException e) {
            log.error("broadcast record failed", e);
        } catch (Throwable throwable) {
            log.error("[Error-0000]-broadcast record failed", throwable);
        }
    }

    @Override
    protected void broadcastRepeat(RepeatModel record) {
        try {
            String body = SerializerWrapper.hessianSerialize(record);
            broadcast(broadcastRepeatUrl, body, record.getTraceId());
        } catch (SerializeException e) {
            log.error("broadcast record failed", e);
        } catch (Throwable throwable) {
            log.error("[Error-0000]-broadcast record failed", throwable);
        }
    }

    /**
     * 请求发送
     * @param url 地址
     * @param body 请求内容
     * @param traceId traceId
     */
    private void broadcast(String url, String body, String traceId) {
        HashMap<String, String> headers = Maps.newHashMap();
        headers.put("content-type", "application/json");
        Resp resp = HttpUtil.invokePostBody(url, headers, body);
        if (resp.isSuccess()) {
            log.info("broadcast success, traceId={}, resp={}", traceId, resp);
        } else {
            log.info("broadcast failed, traceId={}, resp={}", traceId, resp);
        }
    }
}
