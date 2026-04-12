package com.datagenerator.dataset.api;

import com.datagenerator.dataset.domain.DatasetStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record DatasetUpsertRequest(
        @NotBlank String name,
        String category,
        @NotBlank String version,
        @NotNull DatasetStatus status,
        String description,
        @NotBlank String schemaJson,
        @NotBlank String sampleConfigJson
) {
}

