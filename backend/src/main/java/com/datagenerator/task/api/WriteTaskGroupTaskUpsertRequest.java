package com.datagenerator.task.api;

import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTaskStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record WriteTaskGroupTaskUpsertRequest(
        Long id,
        @NotBlank String taskKey,
        @NotBlank String name,
        @NotBlank String tableName,
        @NotNull TableMode tableMode,
        @NotNull WriteMode writeMode,
        @NotNull @Min(1) Integer batchSize,
        Long seed,
        String description,
        @NotNull WriteTaskStatus status,
        @NotNull @Valid WriteTaskGroupRowPlanRequest rowPlan,
        String targetConfigJson,
        String payloadSchemaJson,
        List<@Valid WriteTaskGroupTaskColumnUpsertRequest> columns
) {
}
