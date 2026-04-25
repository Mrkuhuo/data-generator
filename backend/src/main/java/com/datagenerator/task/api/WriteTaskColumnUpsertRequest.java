package com.datagenerator.task.api;

import com.datagenerator.task.domain.ColumnGeneratorType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

public record WriteTaskColumnUpsertRequest(
        @NotBlank String columnName,
        @NotBlank String dbType,
        Integer lengthValue,
        Integer precisionValue,
        Integer scaleValue,
        boolean nullableFlag,
        boolean primaryKeyFlag,
        @NotNull ColumnGeneratorType generatorType,
        Map<String, Object> generatorConfig,
        @NotNull Integer sortOrder
) {
}
