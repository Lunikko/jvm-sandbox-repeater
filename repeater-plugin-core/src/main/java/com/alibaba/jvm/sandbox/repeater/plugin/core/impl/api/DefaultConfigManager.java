package com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.api.ConfigManager;
import com.alibaba.jvm.sandbox.repeater.plugin.core.model.ApplicationModel;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.HttpUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeaterConfig;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeaterResult;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link DefaultConfigManager} http数据拉取
 * <p>
 *
 * @author zhaoyb1990
 */
@Slf4j
public class DefaultConfigManager implements ConfigManager {

    private static final String DEFAULT_CONFIG_URL = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_CONFIG_DATASOURCE, "");

    @Override
    public RepeaterResult<RepeaterConfig> pullConfig() {
        int retryTime = 100;
        HttpUtil.Resp resp = null;
        while (--retryTime > 0) {
            resp = HttpUtil.doGet(String.format(DEFAULT_CONFIG_URL, ApplicationModel.instance().getAppName(),
                    ApplicationModel.instance().getEnvironment()));
            if (resp.isSuccess()) {
                break;
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignore) {
                break;
            }
        }
        if (resp == null) {
            throw new RuntimeException("pull repeater config failed, remain retry time is " + retryTime);
        }
        try {
            String body = resp.getBody();
            log.info("pullConfig successful: {}", body);
            return JSON.parseObject(body, new TypeReference<RepeaterResult<RepeaterConfig>>() {});
        } catch (Exception e) {
            log.error("pullConfig error: {}", e.getMessage(), e);
            return RepeaterResult.builder().success(false).message(e.getMessage()).build();
        }
    }
}
