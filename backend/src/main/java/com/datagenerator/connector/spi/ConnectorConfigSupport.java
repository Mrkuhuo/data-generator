package com.datagenerator.connector.spi;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ConnectorConfigSupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ConnectorConfigSupport() {
    }

    public static Map<String, Object> readConfig(ConnectorInstance connector) {
        return readConfig(connector.getConfigJson(), "连接器配置");
    }

    public static Map<String, Object> readConfig(String json, String label) {
        try {
            if (json == null || json.isBlank()) {
                return Collections.emptyMap();
            }
            return OBJECT_MAPPER.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException(label + " 不是合法的 JSON：" + exception.getMessage());
        }
    }

    public static String writeDetails(Map<String, ?> details) {
        try {
            return OBJECT_MAPPER.writeValueAsString(details);
        } catch (Exception exception) {
            return "{\"error\":\"连接器测试详情序列化失败\"}";
        }
    }

    public static String requireString(Map<String, Object> config, String... keys) {
        String value = optionalString(config, keys);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("缺少配置字段：" + String.join(" 或 ", keys));
        }
        return value;
    }

    public static String optionalString(Map<String, Object> config, String... keys) {
        Object value = firstValue(config, keys);
        if (value == null) {
            return null;
        }
        String normalized = value.toString().trim();
        return normalized.isBlank() ? null : normalized;
    }

    public static Integer optionalInteger(Map<String, Object> config, String... keys) {
        Object value = firstValue(config, keys);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString().trim());
    }

    public static Long optionalLong(Map<String, Object> config, String... keys) {
        Object value = firstValue(config, keys);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString().trim());
    }

    public static Boolean optionalBoolean(Map<String, Object> config, String... keys) {
        Object value = firstValue(config, keys);
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return Boolean.parseBoolean(value.toString().trim());
    }

    public static List<String> optionalStringList(Map<String, Object> config, String... keys) {
        Object value = firstValue(config, keys);
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> rawList) {
            List<String> values = new ArrayList<>();
            for (Object item : rawList) {
                if (item != null && !item.toString().isBlank()) {
                    values.add(item.toString().trim());
                }
            }
            return values;
        }
        if (value instanceof String stringValue) {
            List<String> values = new ArrayList<>();
            for (String item : stringValue.split(",")) {
                if (!item.isBlank()) {
                    values.add(item.trim());
                }
            }
            return values;
        }
        return List.of(value.toString());
    }

    public static Object findValue(Map<String, Object> config, String keyPath) {
        if (config == null || config.isEmpty() || keyPath == null || keyPath.isBlank()) {
            return null;
        }
        Object current = config;
        for (String segment : keyPath.split("\\.")) {
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = currentMap.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static Object firstValue(Map<String, Object> config, String... keys) {
        for (String key : keys) {
            Object value = findValue(config, key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
