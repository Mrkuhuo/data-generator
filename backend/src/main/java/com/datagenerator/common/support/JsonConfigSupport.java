package com.datagenerator.common.support;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JsonConfigSupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonConfigSupport() {
    }

    public static Map<String, Object> readConfig(String json, String label) {
        try {
            if (json == null || json.isBlank()) {
                return Collections.emptyMap();
            }
            return OBJECT_MAPPER.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException(label + " 不是合法的 JSON: " + exception.getMessage(), exception);
        }
    }

    public static String normalizeJson(String json, String label) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(readConfig(json, label));
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException(label + " 不是合法的 JSON: " + exception.getMessage(), exception);
        }
    }

    public static String writeJson(Map<String, ?> value, String fallbackError) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (Exception exception) {
            return "{\"error\":\"" + fallbackError + "\"}";
        }
    }

    public static String requireString(Map<String, Object> config, String... keys) {
        String value = optionalString(config, keys);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("缺少配置字段: " + String.join(" / ", keys));
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
            return rawList.stream()
                    .filter(item -> item != null && !item.toString().isBlank())
                    .map(item -> item.toString().trim())
                    .toList();
        }
        if (value instanceof String stringValue) {
            return List.of(stringValue.split(",")).stream()
                    .map(String::trim)
                    .filter(item -> !item.isBlank())
                    .toList();
        }
        return List.of(value.toString());
    }

    public static Object findValue(Map<String, Object> config, String keyPath) {
        if (config == null || config.isEmpty() || keyPath == null || keyPath.isBlank()) {
            return null;
        }
        if (config.containsKey(keyPath)) {
            return config.get(keyPath);
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

    public static Map<String, Object> copyMap(Map<String, Object> config) {
        return config == null ? new LinkedHashMap<>() : new LinkedHashMap<>(config);
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
