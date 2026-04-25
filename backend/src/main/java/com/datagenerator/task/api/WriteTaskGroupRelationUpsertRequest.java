package com.datagenerator.task.api;

import com.datagenerator.task.domain.ReferenceSourceMode;
import com.datagenerator.task.domain.RelationReusePolicy;
import com.datagenerator.task.domain.RelationSelectionStrategy;
import com.datagenerator.task.domain.WriteTaskRelationMode;
import com.datagenerator.task.domain.WriteTaskRelationType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record WriteTaskGroupRelationUpsertRequest(
        Long id,
        @NotBlank String relationName,
        @NotBlank String parentTaskKey,
        @NotBlank String childTaskKey,
        @NotNull WriteTaskRelationMode relationMode,
        @NotNull WriteTaskRelationType relationType,
        @NotNull ReferenceSourceMode sourceMode,
        @NotNull RelationSelectionStrategy selectionStrategy,
        @NotNull RelationReusePolicy reusePolicy,
        List<String> parentColumns,
        List<String> childColumns,
        Double nullRate,
        Double mixedExistingRatio,
        Integer minChildrenPerParent,
        Integer maxChildrenPerParent,
        String mappingConfigJson,
        @NotNull Integer sortOrder
) {
}
