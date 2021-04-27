package com.alibaba.jvm.sandbox.repeater.module;

import com.alibaba.jvm.sandbox.api.ModuleException;
import com.alibaba.jvm.sandbox.api.resource.ConfigInfo;
import com.alibaba.jvm.sandbox.api.resource.ModuleManager;
import com.alibaba.jvm.sandbox.repeater.plugin.core.model.ApplicationModel;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.HttpUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.jvm.sandbox.repeater.plugin.Constants.REPEAT_HEARTBEAT_URL;

/**
 * {@link HeartbeatHandler}
 * <p>
 *
 * @author zhaoyb1990
 */
public class HeartbeatHandler {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatHandler.class);

    private static final long FREQUENCY = 10;

    private static final String HEARTBEAT_DOMAIN = PropertyUtil.getPropertyOrDefault(REPEAT_HEARTBEAT_URL, "");

    private static ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("heartbeat-pool-%d").daemon(true).build());

    private final ConfigInfo configInfo;
    private final ModuleManager moduleManager;
    private AtomicBoolean initialize = new AtomicBoolean(false);

    public HeartbeatHandler(ConfigInfo configInfo, ModuleManager moduleManager) {
        this.configInfo = configInfo;
        this.moduleManager = moduleManager;
    }

    public synchronized void start() {
        if (initialize.compareAndSet(false, true)) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    innerReport();
                } catch (Exception e) {
                    log.error("error occurred when report heartbeat", e);
                }
            }, 0, FREQUENCY, TimeUnit.SECONDS);
        }
    }

    public void stop() {
        if (initialize.compareAndSet(true, false)) {
            executorService.shutdown();
        }
    }

    private void innerReport() {
        Map<String, String> params = new HashMap<>(8);
        params.put("appName", ApplicationModel.instance().getAppName());
        params.put("ip", ApplicationModel.instance().getHost());
        params.put("environment", ApplicationModel.instance().getEnvironment());
        params.put("port", configInfo.getServerAddress().getPort() + "");
        params.put("version", Constants.VERSION);
        try {
            params.put("status", moduleManager.isActivated(Constants.MODULE_ID) ? "ACTIVE" : "FROZEN");
        } catch (ModuleException e) {
            // ignore
        }
        HttpUtil.doGet(HEARTBEAT_DOMAIN, params);
    }
}
