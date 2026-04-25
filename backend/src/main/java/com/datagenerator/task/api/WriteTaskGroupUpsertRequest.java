package com.datagenerator.task.api;

import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Min;
import java.time.Instant;
import java.util.List;

public record WriteTaskGroupUpsertRequest(
        @NotBlank String name,
        @NotNull Long connectionId,
        String description,
        Long seed,
        @NotNull WriteTaskStatus status,
        @NotNull WriteTaskScheduleType scheduleType,
        String cronExpression,
        Instant triggerAt,
        @Min(1) Integer intervalSeconds,
        @Min(1) Integer maxRuns,
        @Min(1) Long maxRowsTotal,
        List<@Valid WriteTaskGroupTaskUpsertRequest> tasks,
        List<@Valid WriteTaskGroupRelationUpsertRequest> relations
) {
}
