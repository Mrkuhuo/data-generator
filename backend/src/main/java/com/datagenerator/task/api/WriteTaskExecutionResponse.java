package com.datagenerator.task.api;

import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteTaskExecution;
import java.time.Instant;

public record WriteTaskExecutionResponse(
        Long id,
        Long writeTaskId,
        WriteExecutionTriggerType triggerType,
        WriteExecutionStatus status,
        Instant startedAt,
        Instant finishedAt,
        Long generatedCount,
        Long successCount,
        Long errorCount,
        String errorSummary,
        String deliveryDetailsJson
) {

    public static WriteTaskExecutionResponse from(WriteTaskExecution execution) {
        return new WriteTaskExecutionResponse(
                execution.getId(),
                execution.getWriteTaskId(),
                execution.getTriggerType(),
                execution.getStatus(),
                execution.getStartedAt(),
                execution.getFinishedAt(),
                execution.getGeneratedCount(),
                execution.getSuccessCount(),
                execution.getErrorCount(),
                execution.getErrorSummary(),
                execution.getDeliveryDetailsJson()
        );
    }
}
