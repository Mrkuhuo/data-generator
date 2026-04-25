package com.datagenerator.task.api;

public record KafkaSchemaImportWarning(
        String path,
        String code,
        String message
) {
}
