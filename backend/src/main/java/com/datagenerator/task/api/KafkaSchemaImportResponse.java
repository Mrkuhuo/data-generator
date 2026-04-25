package com.datagenerator.task.api;

import java.util.List;

public record KafkaSchemaImportResponse(
        String schemaSource,
        String payloadSchemaJson,
        List<String> scalarPaths,
        List<KafkaSchemaImportWarning> warnings
) {
}
