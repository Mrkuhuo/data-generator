package com.datagenerator.task.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record WriteTaskPreviewRequest(
        @Valid WriteTaskUpsertRequest task,
        @Min(1) @Max(100) Integer count,
        Long seed
) {
}
