package com.datagenerator.job.api;

import com.datagenerator.job.domain.JobScheduleType;
import com.datagenerator.job.domain.JobStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record JobUpsertRequest(
        @NotBlank String name,
        @NotNull Long datasetDefinitionId,
        @NotNull Long targetConnectorId,
        @NotNull JobWriteStrategy writeStrategy,
        @NotNull JobScheduleType scheduleType,
        String cronExpression,
        @NotNull JobStatus status,
        @NotBlank String runtimeConfigJson
) {
}

