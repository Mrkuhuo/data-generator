package com.datagenerator.task.api;

import com.datagenerator.task.domain.ColumnGeneratorType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

public record WriteTaskGroupTaskColumnUpsertRequest(
        @NotBlank String columnName,
        String dbType,
        Integer lengthValue,
        Integer precisionValue,
        Integer scaleValue,
        boolean nullableFlag,
        boolean primaryKeyFlag,
        boolean foreignKeyFlag,
        @NotNull ColumnGeneratorType generatorType,
        Map<String, Object> generatorConfig,
        @NotNull Integer sortOrder
) {
}
