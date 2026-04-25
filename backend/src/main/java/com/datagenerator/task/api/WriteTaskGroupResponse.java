package com.datagenerator.task.api;

import com.datagenerator.task.domain.WriteTaskGroup;
import java.time.Instant;
import java.util.List;

public record WriteTaskGroupResponse(
        Long id,
        Instant createdAt,
        Instant updatedAt,
        String name,
        Long connectionId,
        String description,
        Long seed,
        String status,
        String scheduleType,
        String cronExpression,
        Instant triggerAt,
        Integer intervalSeconds,
        Integer maxRuns,
        Long maxRowsTotal,
        Instant lastTriggeredAt,
        String schedulerState,
        Instant nextFireAt,
        Instant previousFireAt,
        List<WriteTaskGroupTaskResponse> tasks,
        List<WriteTaskGroupRelationResponse> relations
) {

    public static WriteTaskGroupResponse from(
            WriteTaskGroup group,
            List<WriteTaskGroupTaskResponse> tasks,
            List<WriteTaskGroupRelationResponse> relations
    ) {
        return new WriteTaskGroupResponse(
                group.getId(),
                group.getCreatedAt(),
                group.getUpdatedAt(),
                group.getName(),
                group.getConnectionId(),
                group.getDescription(),
                group.getSeed(),
                group.getStatus().name(),
                group.getScheduleType().name(),
                group.getCronExpression(),
                group.getTriggerAt(),
                group.getIntervalSeconds(),
                group.getMaxRuns(),
                group.getMaxRowsTotal(),
                group.getLastTriggeredAt(),
                group.getSchedulerState(),
                group.getNextFireAt(),
                group.getPreviousFireAt(),
                tasks,
                relations
        );
    }
}
