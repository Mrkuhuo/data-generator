package com.datagenerator.task.api;

import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public record WriteTaskGroupTaskColumnResponse(
        Long id,
        String columnName,
        String dbType,
        Integer lengthValue,
        Integer precisionValue,
        Integer scaleValue,
        boolean nullableFlag,
        boolean primaryKeyFlag,
        boolean foreignKeyFlag,
        ColumnGeneratorType generatorType,
        Map<String, Object> generatorConfig,
        Integer sortOrder
) {

    public static WriteTaskGroupTaskColumnResponse from(WriteTaskColumn column, ObjectMapper objectMapper) {
        return new WriteTaskGroupTaskColumnResponse(
                column.getId(),
                column.getColumnName(),
                column.getDbType(),
                column.getLengthValue(),
                column.getPrecisionValue(),
                column.getScaleValue(),
                column.isNullableFlag(),
                column.isPrimaryKeyFlag(),
                column.isForeignKeyFlag(),
                column.getGeneratorType(),
                readConfig(column.getGeneratorConfigJson(), objectMapper),
                column.getSortOrder()
        );
    }

    private static Map<String, Object> readConfig(String json, ObjectMapper objectMapper) {
        try {
            if (json == null || json.isBlank()) {
                return Map.of();
            }
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid column generator config: " + exception.getMessage(), exception);
        }
    }
}
