package com.datagenerator.task.api;

import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public record WriteTaskColumnResponse(
        Long id,
        String columnName,
        String dbType,
        Integer lengthValue,
        Integer precisionValue,
        Integer scaleValue,
        boolean nullableFlag,
        boolean primaryKeyFlag,
        ColumnGeneratorType generatorType,
        Map<String, Object> generatorConfig,
        Integer sortOrder
) {

    public static WriteTaskColumnResponse from(WriteTaskColumn column, ObjectMapper objectMapper) {
        return new WriteTaskColumnResponse(
                column.getId(),
                column.getColumnName(),
                column.getDbType(),
                column.getLengthValue(),
                column.getPrecisionValue(),
                column.getScaleValue(),
                column.isNullableFlag(),
                column.isPrimaryKeyFlag(),
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
            throw new IllegalArgumentException("列生成配置格式非法: " + exception.getMessage(), exception);
        }
    }
}
