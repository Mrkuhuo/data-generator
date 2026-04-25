package com.datagenerator.task.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record WriteTaskGroupPreviewRequest(
        @NotNull @Valid WriteTaskGroupUpsertRequest group,
        Integer previewCount,
        Long seed
) {
}
