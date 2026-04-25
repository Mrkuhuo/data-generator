package com.datagenerator.task.api;

import jakarta.validation.constraints.NotBlank;

public record KafkaSchemaImportRequest(
        @NotBlank String content
) {
}
