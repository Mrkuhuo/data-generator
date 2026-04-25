package com.datagenerator.task.api;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.task.domain.WriteTaskGroupTableExecution;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public record WriteTaskGroupTableExecutionResponse(
        Long id,
        Long writeTaskId,
        String tableName,
        String status,
        Long beforeWriteRowCount,
        Long afterWriteRowCount,
        Long insertedCount,
        Long nullViolationCount,
        Long blankStringCount,
        Long fkMissCount,
        Long pkDuplicateCount,
        String errorSummary,
        Map<String, Object> summary
) {

    public static WriteTaskGroupTableExecutionResponse from(
            WriteTaskGroupTableExecution execution,
            ObjectMapper objectMapper
    ) {
        return new WriteTaskGroupTableExecutionResponse(
                execution.getId(),
                execution.getWriteTaskId(),
                execution.getTableName(),
                execution.getStatus().name(),
                execution.getBeforeWriteRowCount(),
                execution.getAfterWriteRowCount(),
                execution.getInsertedCount(),
                execution.getNullViolationCount(),
                execution.getBlankStringCount(),
                execution.getFkMissCount(),
                execution.getPkDuplicateCount(),
                execution.getErrorSummary(),
                JsonConfigSupport.readConfig(execution.getSummaryJson(), "summaryJson")
        );
    }
}
