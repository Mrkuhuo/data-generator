package com.datagenerator.task.application;

import com.datagenerator.task.api.KafkaSchemaImportResponse;
import com.datagenerator.task.api.KafkaSchemaImportWarning;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.KafkaPayloadNodeType;
import com.datagenerator.task.domain.KafkaPayloadSchemaNode;
import com.datagenerator.task.domain.KafkaPayloadValueType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class KafkaSchemaImportService {

    private static final String DEFAULT_CHARSET = "abcdefghijklmnopqrstuvwxyz0123456789";

    private final ObjectMapper objectMapper;
    private final KafkaPayloadSchemaService payloadSchemaService;

    public KafkaSchemaImportService(ObjectMapper objectMapper, KafkaPayloadSchemaService payloadSchemaService) {
        this.objectMapper = objectMapper;
        this.payloadSchemaService = payloadSchemaService;
    }

    public KafkaSchemaImportResponse importExampleJson(String content) {
        JsonNode root = parseJsonContent(content, "示例 JSON");
        ArrayList<KafkaSchemaImportWarning> warnings = new ArrayList<>();
        KafkaPayloadSchemaNode schema = inferFromExample(root, null, "$", true, warnings);
        return toResponse("EXAMPLE_JSON", schema, warnings);
    }

    public KafkaSchemaImportResponse importJsonSchema(String content) {
        JsonNode root = parseJsonContent(content, "JSON Schema");
        ArrayList<KafkaSchemaImportWarning> warnings = new ArrayList<>();
        KafkaPayloadSchemaNode schema = inferFromJsonSchema(root, null, "$", true, warnings);
        return toResponse("JSON_SCHEMA", schema, warnings);
    }

    private KafkaSchemaImportResponse toResponse(
            String schemaSource,
            KafkaPayloadSchemaNode schema,
            List<KafkaSchemaImportWarning> warnings
    ) {
        try {
            String normalizedPayloadSchemaJson = payloadSchemaService.normalizeJson(objectMapper.writeValueAsString(schema));
            KafkaPayloadSchemaNode normalizedSchema = payloadSchemaService.parseAndValidate(normalizedPayloadSchemaJson);
            return new KafkaSchemaImportResponse(
                    schemaSource,
                    normalizedPayloadSchemaJson,
                    payloadSchemaService.collectScalarPaths(normalizedSchema),
                    List.copyOf(warnings)
            );
        } catch (Exception exception) {
            throw new IllegalArgumentException("Kafka 消息结构生成失败: " + exception.getMessage(), exception);
        }
    }

    private JsonNode parseJsonContent(String content, String label) {
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException(label + " 不能为空");
        }
        try {
            return objectMapper.readTree(content);
        } catch (Exception exception) {
            throw new IllegalArgumentException(label + " 不是合法的 JSON: " + exception.getMessage(), exception);
        }
    }

    private KafkaPayloadSchemaNode inferFromExample(
            JsonNode node,
            String name,
            String path,
            boolean root,
            List<KafkaSchemaImportWarning> warnings
    ) {
        if (node == null || node.isNull()) {
            warnings.add(warning(path, "NULL_VALUE", "当前值为 null，已按可空文本字段处理"));
            return scalarNode(name, true, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(12, null));
        }

        if (node.isObject()) {
            ArrayList<KafkaPayloadSchemaNode> children = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                children.add(inferFromExample(field.getValue(), field.getKey(), appendPath(path, field.getKey()), false, warnings));
            }
            if (children.isEmpty()) {
                warnings.add(warning(path, "EMPTY_OBJECT", "对象为空，已补充一个占位字段，保存前请调整"));
                children.add(scalarNode("field_1", true, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(12, null)));
            }
            return new KafkaPayloadSchemaNode(
                    KafkaPayloadNodeType.OBJECT,
                    root ? null : name,
                    false,
                    null,
                    null,
                    null,
                    children,
                    null,
                    null,
                    null
            );
        }

        if (node.isArray()) {
            return inferArrayFromExample(node, name, path, root, warnings);
        }

        return inferScalarFromExample(node, name, warnings);
    }

    private KafkaPayloadSchemaNode inferArrayFromExample(
            JsonNode node,
            String name,
            String path,
            boolean root,
            List<KafkaSchemaImportWarning> warnings
    ) {
        JsonNode sample = null;
        boolean nullableItems = false;
        Set<String> sampleKinds = new LinkedHashSet<>();

        for (JsonNode item : node) {
            if (item == null || item.isNull()) {
                nullableItems = true;
                continue;
            }
            sampleKinds.add(exampleNodeKind(item));
            if (sample == null) {
                sample = item;
            }
        }

        if (sample == null) {
            warnings.add(warning(path, "EMPTY_ARRAY", "数组为空，已按文本元素数组处理"));
            return new KafkaPayloadSchemaNode(
                    KafkaPayloadNodeType.ARRAY,
                    root ? null : name,
                    false,
                    null,
                    null,
                    null,
                    null,
                    scalarNode(null, true, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(12, null)),
                    0,
                    3
            );
        }

        if (sampleKinds.size() > 1) {
            warnings.add(warning(path, "MIXED_ARRAY", "数组元素类型不一致，已按首个非空元素推断结构"));
        }

        KafkaPayloadSchemaNode itemSchema = inferFromExample(sample, null, path + "[]", false, warnings);
        if (nullableItems) {
            itemSchema = copyNullable(itemSchema, true);
        }
        return new KafkaPayloadSchemaNode(
                KafkaPayloadNodeType.ARRAY,
                root ? null : name,
                false,
                null,
                null,
                null,
                null,
                itemSchema,
                node.isEmpty() ? 0 : 1,
                Math.max(node.size(), 3)
        );
    }

    private KafkaPayloadSchemaNode inferScalarFromExample(JsonNode node, String name, List<KafkaSchemaImportWarning> warnings) {
        if (node.isBoolean()) {
            return scalarNode(name, false, KafkaPayloadValueType.BOOLEAN, ColumnGeneratorType.BOOLEAN, Map.of("trueRate", node.booleanValue() ? 0.75 : 0.25));
        }

        if (node.isIntegralNumber()) {
            long value = node.longValue();
            KafkaPayloadValueType valueType = value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE
                    ? KafkaPayloadValueType.INT
                    : KafkaPayloadValueType.LONG;
            boolean sequence = looksLikeIdentifier(name);
            return scalarNode(
                    name,
                    false,
                    valueType,
                    sequence ? ColumnGeneratorType.SEQUENCE : ColumnGeneratorType.RANDOM_INT,
                    sequence
                            ? Map.of("start", value == 0 ? 1L : value, "step", 1L)
                            : integerGeneratorConfig(value)
            );
        }

        if (node.isNumber()) {
            BigDecimal value = node.decimalValue();
            return scalarNode(
                    name,
                    false,
                    KafkaPayloadValueType.DECIMAL,
                    ColumnGeneratorType.RANDOM_DECIMAL,
                    decimalGeneratorConfig(value)
            );
        }

        String text = node.asText("");
        if (looksLikeUuid(text)) {
            return scalarNode(name, false, KafkaPayloadValueType.UUID, ColumnGeneratorType.UUID, Map.of());
        }
        Instant instant = parseInstant(text);
        if (instant != null) {
            return scalarNode(name, false, KafkaPayloadValueType.DATETIME, ColumnGeneratorType.DATETIME, datetimeGeneratorConfig(instant));
        }
        if (looksLikeEmail(text)) {
            return scalarNode(name, false, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(Math.max(text.length(), 12), domainFromEmail(text)));
        }
        if (text.isBlank()) {
            warnings.add(warning(name == null || name.isBlank() ? "$" : name, "BLANK_STRING", "字符串为空，已按随机文本字段处理"));
        }
        return scalarNode(name, false, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(Math.max(text.length(), 12), null));
    }

    private KafkaPayloadSchemaNode inferFromJsonSchema(
            JsonNode schemaNode,
            String name,
            String path,
            boolean root,
            List<KafkaSchemaImportWarning> warnings
    ) {
        SchemaDescriptor descriptor = describeJsonSchema(schemaNode, path, warnings);
        return switch (descriptor.type()) {
            case "object" -> inferObjectFromJsonSchema(schemaNode, name, path, root, descriptor.nullable(), warnings);
            case "array" -> inferArrayFromJsonSchema(schemaNode, name, path, root, descriptor.nullable(), warnings);
            case "integer" -> scalarNode(
                    root ? null : name,
                    descriptor.nullable(),
                    inferIntegerValueType(schemaNode),
                    looksLikeIdentifier(name) ? ColumnGeneratorType.SEQUENCE : ColumnGeneratorType.RANDOM_INT,
                    looksLikeIdentifier(name) ? Map.of("start", 1, "step", 1) : Map.of("min", 0, "max", 1000)
            );
            case "number" -> scalarNode(
                    root ? null : name,
                    descriptor.nullable(),
                    KafkaPayloadValueType.DECIMAL,
                    ColumnGeneratorType.RANDOM_DECIMAL,
                    Map.of("min", 0, "max", 1000, "scale", 2)
            );
            case "boolean" -> scalarNode(
                    root ? null : name,
                    descriptor.nullable(),
                    KafkaPayloadValueType.BOOLEAN,
                    ColumnGeneratorType.BOOLEAN,
                    Map.of("trueRate", 0.5)
            );
            default -> inferStringFromJsonSchema(schemaNode, name, descriptor.nullable(), path, warnings);
        };
    }

    private KafkaPayloadSchemaNode inferObjectFromJsonSchema(
            JsonNode schemaNode,
            String name,
            String path,
            boolean root,
            boolean nullable,
            List<KafkaSchemaImportWarning> warnings
    ) {
        JsonNode propertiesNode = schemaNode.path("properties");
        JsonNode requiredNode = schemaNode.path("required");
        Set<String> requiredFields = new LinkedHashSet<>();
        if (requiredNode.isArray()) {
            requiredNode.forEach(item -> requiredFields.add(item.asText("")));
        }

        ArrayList<KafkaPayloadSchemaNode> children = new ArrayList<>();
        if (propertiesNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                KafkaPayloadSchemaNode child = inferFromJsonSchema(
                        field.getValue(),
                        field.getKey(),
                        appendPath(path, field.getKey()),
                        false,
                        warnings
                );
                if (!requiredFields.contains(field.getKey())) {
                    child = copyNullable(child, true);
                }
                children.add(child);
            }
        }

        if (children.isEmpty()) {
            warnings.add(warning(path, "EMPTY_SCHEMA_OBJECT", "对象 Schema 未定义 properties，已补充一个占位字段"));
            children.add(scalarNode("field_1", true, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(12, null)));
        }

        if (schemaNode.has("additionalProperties") && schemaNode.get("additionalProperties").isObject()) {
            warnings.add(warning(path, "UNSUPPORTED_ADDITIONAL_PROPERTIES", "暂未自动展开 additionalProperties，请按需手工补充"));
        }

        return new KafkaPayloadSchemaNode(
                KafkaPayloadNodeType.OBJECT,
                root ? null : name,
                nullable,
                null,
                null,
                null,
                children,
                null,
                null,
                null
        );
    }

    private KafkaPayloadSchemaNode inferArrayFromJsonSchema(
            JsonNode schemaNode,
            String name,
            String path,
            boolean root,
            boolean nullable,
            List<KafkaSchemaImportWarning> warnings
    ) {
        JsonNode itemsNode = schemaNode.get("items");
        KafkaPayloadSchemaNode itemSchema;
        if (itemsNode == null || itemsNode.isNull()) {
            warnings.add(warning(path, "MISSING_ITEMS", "数组 Schema 缺少 items，已按文本元素数组处理"));
            itemSchema = scalarNode(null, true, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(12, null));
        } else {
            itemSchema = inferFromJsonSchema(itemsNode, null, path + "[]", false, warnings);
        }

        Integer minItems = schemaNode.has("minItems") && schemaNode.get("minItems").canConvertToInt()
                ? schemaNode.get("minItems").intValue()
                : 1;
        Integer maxItems = schemaNode.has("maxItems") && schemaNode.get("maxItems").canConvertToInt()
                ? schemaNode.get("maxItems").intValue()
                : Math.max(minItems, 3);

        return new KafkaPayloadSchemaNode(
                KafkaPayloadNodeType.ARRAY,
                root ? null : name,
                nullable,
                null,
                null,
                null,
                null,
                itemSchema,
                Math.max(minItems, 0),
                Math.max(maxItems, Math.max(minItems, 0))
        );
    }

    private KafkaPayloadSchemaNode inferStringFromJsonSchema(
            JsonNode schemaNode,
            String name,
            boolean nullable,
            String path,
            List<KafkaSchemaImportWarning> warnings
    ) {
        JsonNode enumNode = schemaNode.get("enum");
        if (enumNode != null && enumNode.isArray() && !enumNode.isEmpty()) {
            ArrayList<String> values = new ArrayList<>();
            enumNode.forEach(item -> values.add(item.isTextual() ? item.textValue() : item.toString()));
            return scalarNode(
                    name,
                    nullable,
                    KafkaPayloadValueType.STRING,
                    ColumnGeneratorType.ENUM,
                    Map.of("values", values)
            );
        }

        String format = schemaNode.path("format").asText("");
        return switch (format.toLowerCase(Locale.ROOT)) {
            case "uuid" -> scalarNode(name, nullable, KafkaPayloadValueType.UUID, ColumnGeneratorType.UUID, Map.of());
            case "date", "date-time" -> scalarNode(
                    name,
                    nullable,
                    KafkaPayloadValueType.DATETIME,
                    ColumnGeneratorType.DATETIME,
                    datetimeGeneratorConfig(Instant.now())
            );
            case "email" -> scalarNode(name, nullable, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(16, "demo.local"));
            default -> {
                if (schemaNode.has("oneOf") || schemaNode.has("anyOf") || schemaNode.has("allOf")) {
                    warnings.add(warning(path, "UNSUPPORTED_COMPOSITION", "暂未自动解析 oneOf/anyOf/allOf，已按文本字段处理"));
                }
                yield scalarNode(name, nullable, KafkaPayloadValueType.STRING, ColumnGeneratorType.STRING, stringGeneratorConfig(12, null));
            }
        };
    }

    private SchemaDescriptor describeJsonSchema(JsonNode schemaNode, String path, List<KafkaSchemaImportWarning> warnings) {
        LinkedHashSet<String> types = new LinkedHashSet<>();
        JsonNode typeNode = schemaNode.get("type");
        if (typeNode != null) {
            if (typeNode.isTextual()) {
                types.add(typeNode.asText("").toLowerCase(Locale.ROOT));
            } else if (typeNode.isArray()) {
                typeNode.forEach(item -> {
                    if (item.isTextual()) {
                        types.add(item.asText("").toLowerCase(Locale.ROOT));
                    }
                });
            }
        }

        boolean nullable = types.remove("null");
        if (types.isEmpty()) {
            if (schemaNode.has("properties")) {
                types.add("object");
            } else if (schemaNode.has("items")) {
                types.add("array");
            } else if (schemaNode.has("enum")) {
                types.add("string");
            } else {
                warnings.add(warning(path, "MISSING_TYPE", "Schema 未声明 type，已按 string 处理"));
                types.add("string");
            }
        }

        String resolvedType = types.iterator().next();
        if (types.size() > 1) {
            warnings.add(warning(path, "MULTIPLE_TYPES", "Schema 存在多个 type，已按第一个可识别类型处理"));
        }
        return new SchemaDescriptor(resolvedType, nullable);
    }

    private KafkaPayloadValueType inferIntegerValueType(JsonNode schemaNode) {
        String format = schemaNode.path("format").asText("");
        if ("int64".equalsIgnoreCase(format)) {
            return KafkaPayloadValueType.LONG;
        }
        return KafkaPayloadValueType.INT;
    }

    private KafkaPayloadSchemaNode scalarNode(
            String name,
            boolean nullable,
            KafkaPayloadValueType valueType,
            ColumnGeneratorType generatorType,
            Map<String, Object> generatorConfig
    ) {
        return new KafkaPayloadSchemaNode(
                KafkaPayloadNodeType.SCALAR,
                name,
                nullable,
                valueType,
                generatorType,
                generatorConfig,
                null,
                null,
                null,
                null
        );
    }

    private KafkaPayloadSchemaNode copyNullable(KafkaPayloadSchemaNode node, boolean nullable) {
        return new KafkaPayloadSchemaNode(
                node.type(),
                node.name(),
                nullable,
                node.valueType(),
                node.generatorType(),
                node.generatorConfig(),
                node.children(),
                node.itemSchema(),
                node.minItems(),
                node.maxItems()
        );
    }

    private String appendPath(String path, String segment) {
        if (path == null || path.isBlank() || "$".equals(path)) {
            return segment;
        }
        return path + "." + segment;
    }

    private String exampleNodeKind(JsonNode node) {
        if (node == null || node.isNull()) {
            return "null";
        }
        if (node.isObject()) {
            return "object";
        }
        if (node.isArray()) {
            return "array";
        }
        if (node.isIntegralNumber()) {
            return "integer";
        }
        if (node.isNumber()) {
            return "number";
        }
        if (node.isBoolean()) {
            return "boolean";
        }
        return "string";
    }

    private boolean looksLikeIdentifier(String name) {
        if (name == null) {
            return false;
        }
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        return "id".equals(normalized)
                || normalized.endsWith("id")
                || normalized.endsWith("_id")
                || normalized.endsWith("Id");
    }

    private boolean looksLikeUuid(String value) {
        try {
            UUID.fromString(value);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    private boolean looksLikeEmail(String value) {
        return value != null && value.contains("@") && value.indexOf('@') > 0 && value.indexOf('@') < value.length() - 1;
    }

    private String domainFromEmail(String value) {
        if (!looksLikeEmail(value)) {
            return "demo.local";
        }
        return value.substring(value.indexOf('@') + 1).trim();
    }

    private Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (Exception ignored) {
        }
        try {
            return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC);
        } catch (Exception ignored) {
        }
        try {
            return LocalDate.parse(value).atStartOfDay().toInstant(ZoneOffset.UTC);
        } catch (Exception ignored) {
        }
        return null;
    }

    private Map<String, Object> stringGeneratorConfig(int length, String domain) {
        LinkedHashMap<String, Object> config = new LinkedHashMap<>();
        if (domain != null && !domain.isBlank()) {
            config.put("mode", "email");
            config.put("domain", domain);
            config.put("prefix", "");
            config.put("suffix", "");
            return config;
        }
        config.put("mode", "random");
        config.put("length", Math.max(length, 1));
        config.put("charset", DEFAULT_CHARSET);
        config.put("prefix", "");
        config.put("suffix", "");
        return config;
    }

    private Map<String, Object> integerGeneratorConfig(long sampleValue) {
        long spread = Math.max(Math.abs(sampleValue), 100L);
        return Map.of(
                "min", sampleValue - spread,
                "max", sampleValue + spread
        );
    }

    private Map<String, Object> decimalGeneratorConfig(BigDecimal sampleValue) {
        double value = sampleValue.doubleValue();
        double spread = Math.max(Math.abs(value), 100D);
        return Map.of(
                "min", value - spread,
                "max", value + spread,
                "scale", Math.max(sampleValue.scale(), 2)
        );
    }

    private Map<String, Object> datetimeGeneratorConfig(Instant sampleValue) {
        return Map.of(
                "from", sampleValue.minusSeconds(30L * 24 * 3600).toString(),
                "to", sampleValue.plusSeconds(30L * 24 * 3600).toString()
        );
    }

    private KafkaSchemaImportWarning warning(String path, String code, String message) {
        return new KafkaSchemaImportWarning(path, code, message);
    }

    private record SchemaDescriptor(String type, boolean nullable) {
    }
}
