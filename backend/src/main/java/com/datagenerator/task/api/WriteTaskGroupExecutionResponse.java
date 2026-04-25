package com.datagenerator.task.api;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.task.domain.WriteTaskGroupExecution;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record WriteTaskGroupExecutionResponse(
        Long id,
        Long writeTaskGroupId,
        String triggerType,
        String status,
        Instant startedAt,
        Instant finishedAt,
        Integer plannedTableCount,
        Integer completedTableCount,
        Integer successTableCount,
        Integer failureTableCount,
        Long insertedRowCount,
        String errorSummary,
        Map<String, Object> summary,
        List<WriteTaskGroupTableExecutionResponse> tables
) {

    public static WriteTaskGroupExecutionResponse from(
            WriteTaskGroupExecution execution,
            List<WriteTaskGroupTableExecutionResponse> tables
    ) {
        return new WriteTaskGroupExecutionResponse(
                execution.getId(),
                execution.getWriteTaskGroupId(),
                execution.getTriggerType() == null ? "MANUAL" : execution.getTriggerType().name(),
                execution.getStatus().name(),
                execution.getStartedAt(),
                execution.getFinishedAt(),
                execution.getPlannedTableCount(),
                execution.getCompletedTableCount(),
                execution.getSuccessTableCount(),
                execution.getFailureTableCount(),
                execution.getInsertedRowCount(),
                execution.getErrorSummary(),
                JsonConfigSupport.readConfig(execution.getSummaryJson(), "summaryJson"),
                tables
        );
    }
}
