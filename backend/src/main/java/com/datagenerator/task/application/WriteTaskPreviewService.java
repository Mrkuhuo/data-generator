package com.datagenerator.task.application;

import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskPreviewResponse;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.KafkaPayloadSchemaNode;
import com.datagenerator.task.domain.KafkaPayloadValueType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.springframework.stereotype.Service;

@Service
public class WriteTaskPreviewService {

    private final KafkaPayloadSchemaService payloadSchemaService;
    private final WriteTaskValueGenerator valueGenerator;

    public WriteTaskPreviewService(
            KafkaPayloadSchemaService payloadSchemaService,
            WriteTaskValueGenerator valueGenerator
    ) {
        this.payloadSchemaService = payloadSchemaService;
        this.valueGenerator = valueGenerator;
    }

    public WriteTaskPreviewResponse preview(WriteTaskUpsertRequest request, Integer requestedCount, Long requestedSeed) {
        int count = sanitizeCount(requestedCount != null ? requestedCount : request.rowCount());
        long seed = requestedSeed != null ? requestedSeed : (request.seed() != null ? request.seed() : System.currentTimeMillis());
        Random random = new Random(seed);
        Map<String, Long> sequenceState = new LinkedHashMap<>();
        List<Map<String, Object>> rows = new ArrayList<>();

        KafkaPayloadSchemaNode payloadSchema = hasPayloadSchema(request)
                ? payloadSchemaService.parseAndValidate(request.payloadSchemaJson())
                : null;
        List<WriteTaskColumnUpsertRequest> columns = request.columns() == null ? List.of() : request.columns();
        if (payloadSchema == null && columns.isEmpty()) {
            throw new IllegalArgumentException("预览任务至少需要一个字段或 payloadSchemaJson");
        }

        for (int rowIndex = 0; rowIndex < count; rowIndex++) {
            if (payloadSchema != null) {
                Object row = generatePayloadNode(payloadSchema, "", rowIndex, random, sequenceState);
                if (!(row instanceof Map<?, ?> rawRow)) {
                    throw new IllegalArgumentException("Kafka 消息 Schema 根节点必须生成对象");
                }
                rows.add(castRow(rawRow));
                continue;
            }

            Map<String, Object> row = new LinkedHashMap<>();
            for (WriteTaskColumnUpsertRequest column : columns) {
                row.put(
                        column.columnName(),
                        valueGenerator.generateValue(
                                column.columnName(),
                                column.generatorType(),
                                column.generatorConfig(),
                                rowIndex,
                                random,
                                sequenceState
                        )
                );
            }
            rows.add(row);
        }

        return new WriteTaskPreviewResponse(count, seed, rows);
    }

    private boolean hasPayloadSchema(WriteTaskUpsertRequest request) {
        return request.payloadSchemaJson() != null && !request.payloadSchemaJson().isBlank();
    }

    private Object generatePayloadNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        return switch (node.type()) {
            case OBJECT -> generateObjectNode(node, path, rowIndex, random, sequenceState);
            case ARRAY -> generateArrayNode(node, path, rowIndex, random, sequenceState);
            case SCALAR -> generateScalarNode(node, path, rowIndex, random, sequenceState);
        };
    }

    private Map<String, Object> generateObjectNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        LinkedHashMap<String, Object> value = new LinkedHashMap<>();
        for (KafkaPayloadSchemaNode child : node.childrenOrEmpty()) {
            value.put(
                    child.name(),
                    generatePayloadNode(child, appendPath(path, child.name()), rowIndex, random, sequenceState)
            );
        }
        return value;
    }

    private List<Object> generateArrayNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        int size = resolveArraySize(node, random);
        ArrayList<Object> items = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            items.add(generatePayloadNode(node.itemSchema(), path + "[]", rowIndex, random, sequenceState));
        }
        return items;
    }

    private Object generateScalarNode(
            KafkaPayloadSchemaNode node,
            String path,
            int rowIndex,
            Random random,
            Map<String, Long> sequenceState
    ) {
        Object rawValue = valueGenerator.generateValue(
                path,
                node.generatorType(),
                node.generatorConfigOrEmpty(),
                rowIndex,
                random,
                sequenceState
        );
        return castScalarValue(node.valueType(), rawValue);
    }

    private Object castScalarValue(KafkaPayloadValueType valueType, Object value) {
        if (value == null || valueType == null) {
            return value;
        }
        return switch (valueType) {
            case STRING, DATETIME, UUID -> String.valueOf(value);
            case INT -> value instanceof Number number ? number.intValue() : Integer.parseInt(String.valueOf(value));
            case LONG -> value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
            case DECIMAL -> value instanceof BigDecimal decimal ? decimal : new BigDecimal(String.valueOf(value));
            case BOOLEAN -> value instanceof Boolean booleanValue
                    ? booleanValue
                    : Boolean.parseBoolean(String.valueOf(value));
        };
    }

    private int resolveArraySize(KafkaPayloadSchemaNode node, Random random) {
        int minItems = node.minItems() == null ? 1 : Math.max(0, node.minItems());
        int maxItems = node.maxItems() == null ? minItems : node.maxItems();
        if (maxItems <= minItems) {
            return minItems;
        }
        return minItems + random.nextInt(maxItems - minItems + 1);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castRow(Map<?, ?> rawRow) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        rawRow.forEach((key, value) -> row.put(String.valueOf(key), value));
        return row;
    }

    private int sanitizeCount(int count) {
        if (count < 1) {
            return 1;
        }
        return Math.min(count, 100);
    }

    private String appendPath(String path, String segment) {
        if (path == null || path.isBlank()) {
            return segment;
        }
        if (path.endsWith("[]")) {
            return path + "." + segment;
        }
        return path + "." + segment;
    }
}
