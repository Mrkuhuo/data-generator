package com.datagenerator.task.api;

import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;

public record WriteTaskUpsertRequest(
        @NotBlank String name,
        @NotNull Long connectionId,
        @NotBlank String tableName,
        @NotNull TableMode tableMode,
        @NotNull WriteMode writeMode,
        @NotNull @Min(1) @Max(100000) Integer rowCount,
        @NotNull @Min(1) @Max(5000) Integer batchSize,
        Long seed,
        @NotNull WriteTaskStatus status,
        @NotNull WriteTaskScheduleType scheduleType,
        String cronExpression,
        Instant triggerAt,
        @Min(1) Integer intervalSeconds,
        @Min(1) Integer maxRuns,
        @Min(1) Long maxRowsTotal,
        String description,
        String targetConfigJson,
        String payloadSchemaJson,
        List<@Valid WriteTaskColumnUpsertRequest> columns
) {
}
