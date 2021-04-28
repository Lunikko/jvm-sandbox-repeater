package com.alibaba.jvm.sandbox.repeater.plugin.core.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * {@link PropertyUtil} 属性操作
 * <p>
 *
 * @author zhaoyb1990
 */
public class PropertyUtil {

    private static final Logger log = LoggerFactory.getLogger(PropertyUtil.class);

    private static Properties properties = new Properties();

    private PropertyUtil() {
    }

    static {
        try {
            File file = new File(PathUtils.getConfigPath() + "/repeater.properties");
            if (file.exists() && file.canRead()) {
                InputStream is = new FileInputStream(file);
                properties.load(is);
            } else {
                if (PropertyUtil.class.getClassLoader().getClass().getCanonicalName().contains("sandbox")) {
                    throw new RuntimeException("load repeater-core.properties failed");
                }
            }
        } catch (IOException e) {
            log.info(e.getMessage(), e);
        }
    }

    /**
     * 获取系统属性带默认值
     *
     * @param key          属性key
     * @param defaultValue 默认值
     * @return 属性值 or 默认值
     */
    public static String getSystemPropertyOrDefault(String key, String defaultValue) {
        String property = System.getProperty(key);
        return StringUtils.isEmpty(property) ? defaultValue : property;
    }

    /**
     * 获取repeater-core.properties的配置信息
     *
     * @param key          属性key
     * @param defaultValue 默认值
     * @return 属性值 or 默认值
     */
    public static String getPropertyOrDefault(String key, String defaultValue) {
        String property = properties.getProperty(key);
        return StringUtils.isEmpty(property) ? defaultValue : property;
    }

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        try (InputStream is = new FileInputStream(new File(PathUtils.getConfigPath() + "/producer.properties"))) {
            props.load(is);
        } catch (IOException e) {
            log.info(e.getMessage(), e);
        }

        return props;
    }
}
