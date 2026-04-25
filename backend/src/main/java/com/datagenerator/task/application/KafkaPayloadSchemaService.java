package com.datagenerator.task.application;

import com.datagenerator.task.domain.KafkaPayloadNodeType;
import com.datagenerator.task.domain.KafkaPayloadSchemaNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class KafkaPayloadSchemaService {

    private final ObjectMapper objectMapper;

    public KafkaPayloadSchemaService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public KafkaPayloadSchemaNode parseAndValidate(String payloadSchemaJson) {
        if (payloadSchemaJson == null || payloadSchemaJson.isBlank()) {
            throw new IllegalArgumentException("Kafka 复杂消息模式必须提供 payloadSchemaJson");
        }
        try {
            KafkaPayloadSchemaNode schema = objectMapper.readValue(payloadSchemaJson, KafkaPayloadSchemaNode.class);
            validateNode(schema, "$", true, false);
            return schema;
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("payloadSchemaJson 不是合法的 Kafka 消息 Schema: " + exception.getMessage(), exception);
        }
    }

    public String normalizeJson(String payloadSchemaJson) {
        try {
            return objectMapper.writeValueAsString(parseAndValidate(payloadSchemaJson));
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("payloadSchemaJson 序列化失败: " + exception.getMessage(), exception);
        }
    }

    public List<String> collectScalarPaths(KafkaPayloadSchemaNode root) {
        List<String> paths = new ArrayList<>();
        collectScalarPaths(root, "", paths);
        return List.copyOf(paths);
    }

    private void collectScalarPaths(KafkaPayloadSchemaNode node, String path, List<String> paths) {
        if (node == null || node.type() == null) {
            return;
        }
        switch (node.type()) {
            case OBJECT -> node.childrenOrEmpty()
                    .forEach(child -> collectScalarPaths(child, appendPath(path, child.name()), paths));
            case ARRAY -> collectScalarPaths(node.itemSchema(), path + "[]", paths);
            case SCALAR -> paths.add(path);
        }
    }

    private void validateNode(
            KafkaPayloadSchemaNode node,
            String path,
            boolean root,
            boolean itemSchema
    ) {
        if (node == null) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 缺少节点定义");
        }
        if (node.type() == null) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 缺少 type");
        }
        if (!root && !itemSchema && (node.name() == null || node.name().isBlank())) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 缺少字段名");
        }

        switch (node.type()) {
            case OBJECT -> validateObjectNode(node, path);
            case ARRAY -> validateArrayNode(node, path);
            case SCALAR -> validateScalarNode(node, path);
            default -> throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 存在不支持的节点类型");
        }
    }

    private void validateObjectNode(KafkaPayloadSchemaNode node, String path) {
        List<KafkaPayloadSchemaNode> children = node.childrenOrEmpty();
        if (children.isEmpty()) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 OBJECT 节点至少需要一个子字段");
        }

        Set<String> names = new LinkedHashSet<>();
        for (KafkaPayloadSchemaNode child : children) {
            if (child.name() == null || child.name().isBlank()) {
                throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 OBJECT 子字段缺少 name");
            }
            if (!names.add(child.name())) {
                throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 存在重复字段名: " + child.name());
            }
            validateNode(child, appendPath(pathForChildren(path), child.name()), false, false);
        }
    }

    private void validateArrayNode(KafkaPayloadSchemaNode node, String path) {
        if (node.itemSchema() == null) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 ARRAY 节点必须定义 itemSchema");
        }
        int minItems = node.minItems() == null ? 1 : node.minItems();
        int maxItems = node.maxItems() == null ? minItems : node.maxItems();
        if (minItems < 0) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 minItems 不能小于 0");
        }
        if (maxItems < minItems) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 maxItems 不能小于 minItems");
        }
        validateNode(node.itemSchema(), path + "[]", false, true);
    }

    private void validateScalarNode(KafkaPayloadSchemaNode node, String path) {
        if (node.valueType() == null) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 SCALAR 节点必须定义 valueType");
        }
        if (node.generatorType() == null) {
            throw new IllegalArgumentException("Kafka 消息 Schema 在 " + path + " 的 SCALAR 节点必须定义 generatorType");
        }
    }

    private String appendPath(String path, String segment) {
        if (path == null || path.isBlank()) {
            return segment == null ? "" : segment;
        }
        if (segment == null || segment.isBlank()) {
            return path;
        }
        return path + "." + segment;
    }

    private String pathForChildren(String path) {
        return "$".equals(path) ? "" : path;
    }
}
