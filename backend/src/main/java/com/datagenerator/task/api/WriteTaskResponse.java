package com.datagenerator.task.api;

import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;

public record WriteTaskResponse(
        Long id,
        Instant createdAt,
        Instant updatedAt,
        String name,
        Long connectionId,
        String tableName,
        TableMode tableMode,
        WriteMode writeMode,
        Integer rowCount,
        Integer batchSize,
        Long seed,
        WriteTaskStatus status,
        WriteTaskScheduleType scheduleType,
        String cronExpression,
        Instant triggerAt,
        Integer intervalSeconds,
        Integer maxRuns,
        Long maxRowsTotal,
        String description,
        String targetConfigJson,
        String payloadSchemaJson,
        Instant lastTriggeredAt,
        String schedulerState,
        Instant nextFireAt,
        Instant previousFireAt,
        List<WriteTaskColumnResponse> columns
) {

    public static WriteTaskResponse from(WriteTask task, ObjectMapper objectMapper) {
        return new WriteTaskResponse(
                task.getId(),
                task.getCreatedAt(),
                task.getUpdatedAt(),
                task.getName(),
                task.getConnectionId(),
                task.getTableName(),
                task.getTableMode(),
                task.getWriteMode(),
                task.getRowCount(),
                task.getBatchSize(),
                task.getSeed(),
                task.getStatus(),
                task.getScheduleType(),
                task.getCronExpression(),
                task.getTriggerAt(),
                task.getIntervalSeconds(),
                task.getMaxRuns(),
                task.getMaxRowsTotal(),
                task.getDescription(),
                task.getTargetConfigJson(),
                task.getPayloadSchemaJson(),
                task.getLastTriggeredAt(),
                task.getSchedulerState(),
                task.getNextFireAt(),
                task.getPreviousFireAt(),
                task.getColumns().stream()
                        .map(column -> WriteTaskColumnResponse.from(column, objectMapper))
                        .toList()
        );
    }
}
