package com.datagenerator.task.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.application.KafkaConnectionSupport;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.stereotype.Component;

@Component
public class WriteTaskKafkaWriter implements WriteTaskDeliveryWriter {

    private static final Pattern INDEXED_SEGMENT = Pattern.compile("^(.*)\\[(\\d+)]$");

    private final KafkaConnectionSupport kafkaConnectionSupport;
    private final ObjectMapper objectMapper;

    public WriteTaskKafkaWriter(KafkaConnectionSupport kafkaConnectionSupport, ObjectMapper objectMapper) {
        this.kafkaConnectionSupport = kafkaConnectionSupport;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean supports(DatabaseType databaseType) {
        return databaseType == DatabaseType.KAFKA;
    }

    @Override
    public WriteTaskDeliveryResult write(
            WriteTask task,
            TargetConnection connection,
            List<Map<String, Object>> rows,
            Long executionId
    ) throws Exception {
        Map<String, Object> targetConfig = JsonConfigSupport.readConfig(task.getTargetConfigJson(), "targetConfigJson");
        String topic = task.getTableName();
        String keyMode = normalizeKeyMode(JsonConfigSupport.optionalString(targetConfig, "keyMode"));
        String keyPath = "FIELD".equals(keyMode)
                ? JsonConfigSupport.requireString(targetConfig, "keyPath", "keyField")
                : null;
        String keyField = "FIELD".equals(keyMode) ? JsonConfigSupport.optionalString(targetConfig, "keyField") : null;
        String fixedKey = "FIXED".equals(keyMode) ? JsonConfigSupport.requireString(targetConfig, "fixedKey") : null;
        Integer partition = JsonConfigSupport.optionalInteger(targetConfig, "partition");
        Map<String, Object> headers = readHeaders(targetConfig);
        List<KafkaHeaderDefinition> headerDefinitions = readHeaderDefinitions(targetConfig);

        long successCount = 0;
        List<String> errors = new ArrayList<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                kafkaConnectionSupport.buildProducerProperties(connection, executionId)
        )) {
            for (Map<String, Object> row : rows) {
                try {
                    Map<String, Object> resolvedHeaders = headerDefinitions.isEmpty()
                            ? headers
                            : resolveHeaders(headerDefinitions, row);
                    ProducerRecord<String, String> record = buildRecord(
                            topic,
                            partition,
                            resolveKey(keyMode, keyPath, fixedKey, row),
                            resolvedHeaders,
                            row
                    );
                    producer.send(record).get();
                    successCount++;
                } catch (Exception exception) {
                    if (errors.size() < 5) {
                        errors.add(exception.getMessage());
                    }
                }
            }
            producer.flush();
        } catch (Exception exception) {
            if (errors.isEmpty()) {
                errors.add(exception.getMessage());
            }
        }

        long errorCount = rows.size() - successCount;
        LinkedHashMap<String, Object> details = new LinkedHashMap<>();
        details.put("deliveryType", "KAFKA");
        details.put("topic", topic);
        details.put("bootstrapServers", kafkaConnectionSupport.bootstrapServers(connection));
        details.put("payloadFormat", "JSON");
        details.put("keyMode", keyMode);
        if (keyPath != null) {
            details.put("keyPath", keyPath);
        }
        if (keyField != null) {
            details.put("keyField", keyField);
        }
        if (fixedKey != null) {
            details.put("fixedKey", fixedKey);
        }
        if (partition != null) {
            details.put("partition", partition);
        }
        if (!headerDefinitions.isEmpty()) {
            details.put("headerDefinitions", headerDefinitions.stream().map(KafkaHeaderDefinition::toMap).toList());
            details.put("headerCount", headerDefinitions.size());
        } else if (!headers.isEmpty()) {
            details.put("headers", headers);
            details.put("headerCount", headers.size());
        } else {
            details.put("headerCount", 0);
        }
        details.put("writtenRowCount", successCount);
        details.put("errorCount", errorCount);
        if (!errors.isEmpty()) {
            details.put("errors", errors);
        }

        String summary = successCount == rows.size()
                ? "Kafka Topic 写入完成"
                : successCount > 0
                        ? "Kafka Topic 部分写入成功"
                        : "Kafka Topic 写入失败";
        return new WriteTaskDeliveryResult(successCount, errorCount, summary, details);
    }

    private ProducerRecord<String, String> buildRecord(
            String topic,
            Integer partition,
            String key,
            Map<String, Object> headers,
            Map<String, Object> row
    ) throws Exception {
        String payload = objectMapper.writeValueAsString(row);
        ProducerRecord<String, String> record = partition == null
                ? new ProducerRecord<>(topic, key, payload)
                : new ProducerRecord<>(topic, partition, key, payload);

        for (Map.Entry<String, Object> header : headers.entrySet()) {
            record.headers().add(new RecordHeader(
                    header.getKey(),
                    String.valueOf(header.getValue()).getBytes(StandardCharsets.UTF_8)
            ));
        }
        return record;
    }

    private String resolveKey(String keyMode, String keyPath, String fixedKey, Map<String, Object> row) throws Exception {
        return switch (keyMode) {
            case "NONE" -> null;
            case "FIXED" -> fixedKey;
            case "FIELD" -> stringifyKey(resolvePathValue(row, keyPath));
            default -> throw new IllegalArgumentException("不支持的 Kafka keyMode: " + keyMode);
        };
    }

    private String stringifyKey(Object keyValue) throws Exception {
        if (keyValue == null) {
            return null;
        }
        if (keyValue instanceof Map<?, ?> || keyValue instanceof List<?>) {
            return objectMapper.writeValueAsString(keyValue);
        }
        return String.valueOf(keyValue);
    }

    private Object resolvePathValue(Object current, String keyPath) {
        if (keyPath == null || keyPath.isBlank()) {
            return null;
        }
        String[] segments = keyPath.split("\\.");
        return resolvePathValue(current, segments, 0);
    }

    private Object resolvePathValue(Object current, String[] segments, int index) {
        if (current == null) {
            return null;
        }
        if (index >= segments.length) {
            return current;
        }

        String segment = segments[index];
        if (segment.endsWith("[]")) {
            Object next = resolveObjectSegment(current, segment.substring(0, segment.length() - 2));
            if (!(next instanceof List<?> listValue)) {
                return null;
            }
            ArrayList<Object> values = new ArrayList<>();
            for (Object item : listValue) {
                Object nested = resolvePathValue(item, segments, index + 1);
                if (nested instanceof List<?> nestedList) {
                    values.addAll(nestedList);
                } else if (nested != null) {
                    values.add(nested);
                }
            }
            return values;
        }

        Matcher matcher = INDEXED_SEGMENT.matcher(segment);
        if (matcher.matches()) {
            Object next = resolveObjectSegment(current, matcher.group(1));
            if (!(next instanceof List<?> listValue)) {
                return null;
            }
            int itemIndex = Integer.parseInt(matcher.group(2));
            if (itemIndex < 0 || itemIndex >= listValue.size()) {
                return null;
            }
            return resolvePathValue(listValue.get(itemIndex), segments, index + 1);
        }

        Object next = resolveObjectSegment(current, segment);
        return resolvePathValue(next, segments, index + 1);
    }

    private Object resolveObjectSegment(Object current, String segment) {
        if (segment == null || segment.isBlank()) {
            return current;
        }
        if (!(current instanceof Map<?, ?> mapValue)) {
            return null;
        }
        return mapValue.get(segment);
    }

    private Map<String, Object> readHeaders(Map<String, Object> targetConfig) {
        Object headers = JsonConfigSupport.findValue(targetConfig, "headers");
        if (!(headers instanceof Map<?, ?> rawHeaders)) {
            return Map.of();
        }
        LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawHeaders.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return normalized;
    }

    private List<KafkaHeaderDefinition> readHeaderDefinitions(Map<String, Object> targetConfig) {
        Object headerDefinitions = JsonConfigSupport.findValue(targetConfig, "headerDefinitions");
        if (!(headerDefinitions instanceof List<?> rawDefinitions)) {
            return List.of();
        }
        ArrayList<KafkaHeaderDefinition> normalized = new ArrayList<>();
        for (Object rawDefinition : rawDefinitions) {
            if (!(rawDefinition instanceof Map<?, ?> rawDefinitionMap)) {
                continue;
            }
            String name = rawDefinitionMap.get("name") == null ? null : String.valueOf(rawDefinitionMap.get("name"));
            String mode = rawDefinitionMap.get("mode") == null ? null : String.valueOf(rawDefinitionMap.get("mode"));
            String value = rawDefinitionMap.get("value") == null ? null : String.valueOf(rawDefinitionMap.get("value"));
            String path = rawDefinitionMap.get("path") == null ? null : String.valueOf(rawDefinitionMap.get("path"));
            if (name == null || name.isBlank()) {
                continue;
            }
            normalized.add(new KafkaHeaderDefinition(
                    name.trim(),
                    mode == null || mode.isBlank() ? "FIXED" : mode.trim().toUpperCase(Locale.ROOT),
                    value == null || value.isBlank() ? null : value.trim(),
                    path == null || path.isBlank() ? null : path.trim()
            ));
        }
        return normalized;
    }

    private Map<String, Object> resolveHeaders(List<KafkaHeaderDefinition> headerDefinitions, Map<String, Object> row) throws Exception {
        LinkedHashMap<String, Object> resolved = new LinkedHashMap<>();
        for (KafkaHeaderDefinition definition : headerDefinitions) {
            if ("FIELD".equals(definition.mode())) {
                Object value = resolvePathValue(row, definition.path());
                if (value != null) {
                    resolved.put(definition.name(), stringifyKey(value));
                }
                continue;
            }
            if (definition.value() != null) {
                resolved.put(definition.name(), definition.value());
            }
        }
        return resolved;
    }

    private String normalizeKeyMode(String keyMode) {
        if (keyMode == null || keyMode.isBlank()) {
            return "NONE";
        }
        return keyMode.trim().toUpperCase(Locale.ROOT);
    }

    private record KafkaHeaderDefinition(String name, String mode, String value, String path) {

        private Map<String, Object> toMap() {
            LinkedHashMap<String, Object> definition = new LinkedHashMap<>();
            definition.put("name", name);
            definition.put("mode", mode);
            if (value != null) {
                definition.put("value", value);
            }
            if (path != null) {
                definition.put("path", path);
            }
            return definition;
        }
    }
}
