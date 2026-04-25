package com.datagenerator.task.application;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.KafkaPayloadSchemaNode;
import com.datagenerator.task.domain.WriteMode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class WriteTaskDefinitionNormalizationService {

    private final KafkaPayloadSchemaService payloadSchemaService;
    private final ObjectMapper objectMapper;

    public WriteTaskDefinitionNormalizationService(
            KafkaPayloadSchemaService payloadSchemaService,
            ObjectMapper objectMapper
    ) {
        this.payloadSchemaService = payloadSchemaService;
        this.objectMapper = objectMapper;
    }

    public NormalizedWriteTaskDefinition normalize(DatabaseType databaseType, WriteTaskUpsertRequest request) {
        boolean kafkaComplexMode = isKafkaComplexMode(databaseType, request.payloadSchemaJson());
        validateTaskShape(databaseType, request, kafkaComplexMode);
        String normalizedPayloadSchemaJson = normalizePayloadSchema(databaseType, request);
        String normalizedTargetConfigJson = normalizeTargetConfig(
                databaseType,
                request,
                normalizedPayloadSchemaJson
        );
        List<WriteTaskColumnUpsertRequest> columnsToStore = kafkaComplexMode
                ? List.of()
                : normalizeColumns(databaseType, safeColumns(request));
        return new NormalizedWriteTaskDefinition(
                normalizedTargetConfigJson,
                normalizedPayloadSchemaJson,
                columnsToStore,
                kafkaComplexMode
        );
    }

    private void validateTaskShape(
            DatabaseType databaseType,
            WriteTaskUpsertRequest request,
            boolean kafkaComplexMode
    ) {
        if (databaseType != DatabaseType.KAFKA) {
            if (request.payloadSchemaJson() != null && !request.payloadSchemaJson().isBlank()) {
                throw new IllegalArgumentException("Only Kafka targets support payloadSchemaJson");
            }
            if (safeColumns(request).isEmpty()) {
                throw new IllegalArgumentException("Write task must contain at least one field");
            }
            return;
        }

        if (!kafkaComplexMode && safeColumns(request).isEmpty()) {
            throw new IllegalArgumentException("Kafka simple message mode requires at least one field");
        }
    }

    private boolean isKafkaComplexMode(DatabaseType databaseType, String payloadSchemaJson) {
        return databaseType == DatabaseType.KAFKA
                && payloadSchemaJson != null
                && !payloadSchemaJson.isBlank();
    }

    private String normalizePayloadSchema(DatabaseType databaseType, WriteTaskUpsertRequest request) {
        if (!isKafkaComplexMode(databaseType, request.payloadSchemaJson())) {
            return null;
        }
        return payloadSchemaService.normalizeJson(request.payloadSchemaJson());
    }

    private List<WriteTaskColumnUpsertRequest> normalizeColumns(
            DatabaseType databaseType,
            List<WriteTaskColumnUpsertRequest> columns
    ) {
        return columns.stream()
                .map(column -> WriteTaskColumnDefaults.normalize(databaseType, column))
                .toList();
    }

    private String normalizeTargetConfig(
            DatabaseType databaseType,
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson
    ) {
        Map<String, Object> targetConfig = JsonConfigSupport.copyMap(
                JsonConfigSupport.readConfig(request.targetConfigJson(), "targetConfigJson")
        );
        if (databaseType != DatabaseType.KAFKA) {
            return JsonConfigSupport.normalizeJson(request.targetConfigJson(), "targetConfigJson");
        }

        if (request.writeMode() != WriteMode.APPEND) {
            throw new IllegalArgumentException("Kafka target only supports APPEND write mode");
        }

        String payloadFormat = JsonConfigSupport.optionalString(targetConfig, "payloadFormat");
        if (payloadFormat != null && !"JSON".equalsIgnoreCase(payloadFormat)) {
            throw new IllegalArgumentException("Kafka target currently only supports JSON payload format");
        }
        targetConfig.put("payloadFormat", "JSON");

        String keyMode = JsonConfigSupport.optionalString(targetConfig, "keyMode");
        String normalizedKeyMode = keyMode == null || keyMode.isBlank()
                ? "NONE"
                : keyMode.trim().toUpperCase(Locale.ROOT);
        targetConfig.put("keyMode", normalizedKeyMode);
        boolean kafkaComplexMode = normalizedPayloadSchemaJson != null && !normalizedPayloadSchemaJson.isBlank();

        switch (normalizedKeyMode) {
            case "NONE" -> {
            }
            case "FIELD" -> normalizeKafkaFieldKey(targetConfig, request, normalizedPayloadSchemaJson, kafkaComplexMode);
            case "FIXED" -> JsonConfigSupport.requireString(targetConfig, "fixedKey");
            default -> throw new IllegalArgumentException("Unsupported Kafka keyMode: " + normalizedKeyMode);
        }

        Integer partition = JsonConfigSupport.optionalInteger(targetConfig, "partition");
        if (partition != null && partition < 0) {
            throw new IllegalArgumentException("Kafka partition must be greater than or equal to 0");
        }

        normalizeHeaders(targetConfig, request, normalizedPayloadSchemaJson, kafkaComplexMode);
        return writeJson(targetConfig);
    }

    private void normalizeKafkaFieldKey(
            Map<String, Object> targetConfig,
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson,
            boolean kafkaComplexMode
    ) {
        String keyPath = JsonConfigSupport.requireString(targetConfig, "keyPath", "keyField");
        List<String> availablePaths = resolveKafkaAvailablePaths(request, normalizedPayloadSchemaJson, kafkaComplexMode);
        if (!availablePaths.contains(keyPath)) {
            throw new IllegalArgumentException(
                    kafkaComplexMode
                            ? "Kafka keyPath must come from current payload schema scalar paths"
                            : "Kafka keyField must come from current field list"
            );
        }
        targetConfig.put("keyPath", keyPath);
        if (!kafkaComplexMode || (!keyPath.contains(".") && !keyPath.contains("["))) {
            targetConfig.put("keyField", keyPath);
        } else {
            targetConfig.remove("keyField");
        }
    }

    private List<String> resolveKafkaAvailablePaths(
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson,
            boolean kafkaComplexMode
    ) {
        if (!kafkaComplexMode) {
            return safeColumns(request).stream()
                    .map(WriteTaskColumnUpsertRequest::columnName)
                    .filter(columnName -> columnName != null && !columnName.isBlank())
                    .map(String::trim)
                    .toList();
        }
        KafkaPayloadSchemaNode payloadSchema = payloadSchemaService.parseAndValidate(normalizedPayloadSchemaJson);
        return payloadSchemaService.collectScalarPaths(payloadSchema);
    }

    private void normalizeHeaders(
            Map<String, Object> targetConfig,
            WriteTaskUpsertRequest request,
            String normalizedPayloadSchemaJson,
            boolean kafkaComplexMode
    ) {
        Object headerDefinitionsValue = JsonConfigSupport.findValue(targetConfig, "headerDefinitions");
        if (headerDefinitionsValue != null) {
            if (!(headerDefinitionsValue instanceof List<?> rawDefinitions)) {
                throw new IllegalArgumentException("Kafka headerDefinitions must be an array");
            }
            List<String> availablePaths = resolveKafkaAvailablePaths(request, normalizedPayloadSchemaJson, kafkaComplexMode);
            LinkedHashMap<String, Object> normalizedHeaders = new LinkedHashMap<>();
            ArrayList<Map<String, Object>> normalizedDefinitions = new ArrayList<>();
            Set<String> headerNames = new HashSet<>();
            boolean hasFieldHeader = false;

            for (Object rawDefinition : rawDefinitions) {
                if (!(rawDefinition instanceof Map<?, ?> rawDefinitionMap)) {
                    throw new IllegalArgumentException("Kafka headerDefinitions must contain object items");
                }
                Map<String, Object> definition = objectMapper.convertValue(rawDefinitionMap, new TypeReference<>() {
                });
                String name = JsonConfigSupport.requireString(definition, "name");
                String normalizedName = name.toLowerCase(Locale.ROOT);
                if (!headerNames.add(normalizedName)) {
                    throw new IllegalArgumentException("Kafka header name must be unique: " + name);
                }

                String mode = JsonConfigSupport.optionalString(definition, "mode");
                String normalizedMode = mode == null || mode.isBlank()
                        ? "FIXED"
                        : mode.trim().toUpperCase(Locale.ROOT);
                LinkedHashMap<String, Object> normalizedDefinition = new LinkedHashMap<>();
                normalizedDefinition.put("name", name);

                switch (normalizedMode) {
                    case "FIXED" -> {
                        String value = JsonConfigSupport.requireString(definition, "value");
                        normalizedDefinition.put("mode", "FIXED");
                        normalizedDefinition.put("value", value);
                        normalizedHeaders.put(name, value);
                    }
                    case "FIELD" -> {
                        String path = JsonConfigSupport.requireString(definition, "path");
                        if (!availablePaths.contains(path)) {
                            throw new IllegalArgumentException(
                                    kafkaComplexMode
                                            ? "Kafka header path must come from current payload schema"
                                            : "Kafka header path must come from current field list"
                            );
                        }
                        normalizedDefinition.put("mode", "FIELD");
                        normalizedDefinition.put("path", path);
                        hasFieldHeader = true;
                    }
                    default -> throw new IllegalArgumentException("Unsupported Kafka header mode: " + normalizedMode);
                }
                normalizedDefinitions.add(normalizedDefinition);
            }

            if (normalizedHeaders.isEmpty()) {
                targetConfig.remove("headers");
            } else {
                targetConfig.put("headers", normalizedHeaders);
            }
            if (hasFieldHeader) {
                targetConfig.put("headerDefinitions", normalizedDefinitions);
            } else {
                targetConfig.remove("headerDefinitions");
            }
            return;
        }

        Object headers = JsonConfigSupport.findValue(targetConfig, "headers");
        if (headers == null) {
            targetConfig.remove("headerDefinitions");
            return;
        }
        if (!(headers instanceof Map<?, ?> rawHeaders)) {
            throw new IllegalArgumentException("Kafka headers must be an object");
        }
        LinkedHashMap<String, Object> normalizedHeaders = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawHeaders.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                normalizedHeaders.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        targetConfig.put("headers", normalizedHeaders);
        targetConfig.remove("headerDefinitions");
    }

    private List<WriteTaskColumnUpsertRequest> safeColumns(WriteTaskUpsertRequest request) {
        return request.columns() == null ? List.of() : request.columns();
    }

    private String writeJson(Map<String, ?> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("JSON serialization failed: " + exception.getMessage(), exception);
        }
    }

    public record NormalizedWriteTaskDefinition(
            String targetConfigJson,
            String payloadSchemaJson,
            List<WriteTaskColumnUpsertRequest> columns,
            boolean kafkaComplexMode
    ) {
    }
}
