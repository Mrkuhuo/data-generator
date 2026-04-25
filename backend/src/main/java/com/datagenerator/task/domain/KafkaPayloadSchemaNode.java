package com.datagenerator.task.domain;

import java.util.List;
import java.util.Map;

public record KafkaPayloadSchemaNode(
        KafkaPayloadNodeType type,
        String name,
        Boolean nullable,
        KafkaPayloadValueType valueType,
        ColumnGeneratorType generatorType,
        Map<String, Object> generatorConfig,
        List<KafkaPayloadSchemaNode> children,
        KafkaPayloadSchemaNode itemSchema,
        Integer minItems,
        Integer maxItems
) {

    public boolean nullableOrDefault() {
        return Boolean.TRUE.equals(nullable);
    }

    public Map<String, Object> generatorConfigOrEmpty() {
        return generatorConfig == null ? Map.of() : generatorConfig;
    }

    public List<KafkaPayloadSchemaNode> childrenOrEmpty() {
        return children == null ? List.of() : children;
    }
}
