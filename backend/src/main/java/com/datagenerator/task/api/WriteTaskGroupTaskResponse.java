package com.datagenerator.task.api;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.task.domain.WriteTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;

public record WriteTaskGroupTaskResponse(
        Long id,
        String taskKey,
        String name,
        String tableName,
        String tableMode,
        String writeMode,
        Integer batchSize,
        Long seed,
        String description,
        String status,
        WriteTaskGroupRowPlanResponse rowPlan,
        String targetConfigJson,
        String payloadSchemaJson,
        List<WriteTaskGroupTaskColumnResponse> columns
) {

    public static WriteTaskGroupTaskResponse from(WriteTask task, ObjectMapper objectMapper) {
        Map<String, Object> rowPlan = JsonConfigSupport.readConfig(task.getRowPlanJson(), "rowPlanJson");
        return new WriteTaskGroupTaskResponse(
                task.getId(),
                task.getTaskKey(),
                task.getName(),
                task.getTableName(),
                task.getTableMode().name(),
                task.getWriteMode().name(),
                task.getBatchSize(),
                task.getSeed(),
                task.getDescription(),
                task.getStatus().name(),
                new WriteTaskGroupRowPlanResponse(
                        rowPlan.containsKey("mode") ? com.datagenerator.task.domain.WriteTaskGroupRowPlanMode.valueOf(String.valueOf(rowPlan.get("mode"))) : null,
                        JsonConfigSupport.optionalInteger(rowPlan, "rowCount"),
                        JsonConfigSupport.optionalString(rowPlan, "driverTaskKey"),
                        JsonConfigSupport.optionalInteger(rowPlan, "minChildrenPerParent"),
                        JsonConfigSupport.optionalInteger(rowPlan, "maxChildrenPerParent")
                ),
                task.getTargetConfigJson(),
                task.getPayloadSchemaJson(),
                task.getColumns().stream()
                        .map(column -> WriteTaskGroupTaskColumnResponse.from(column, objectMapper))
                        .toList()
        );
    }
}
