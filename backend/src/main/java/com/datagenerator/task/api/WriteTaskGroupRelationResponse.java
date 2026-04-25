package com.datagenerator.task.api;

import com.datagenerator.common.support.JsonConfigSupport;
import com.datagenerator.task.domain.WriteTaskRelation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public record WriteTaskGroupRelationResponse(
        Long id,
        String relationName,
        Long parentTaskId,
        Long childTaskId,
        String parentTaskKey,
        String childTaskKey,
        String relationMode,
        String relationType,
        String sourceMode,
        String selectionStrategy,
        String reusePolicy,
        List<String> parentColumns,
        List<String> childColumns,
        double nullRate,
        Double mixedExistingRatio,
        Integer minChildrenPerParent,
        Integer maxChildrenPerParent,
        String mappingConfigJson,
        Integer sortOrder
) {

    public static WriteTaskGroupRelationResponse from(
            WriteTaskRelation relation,
            Map<Long, String> taskKeysById,
            ObjectMapper objectMapper
    ) {
        return new WriteTaskGroupRelationResponse(
                relation.getId(),
                relation.getRelationName(),
                relation.getParentTaskId(),
                relation.getChildTaskId(),
                taskKeysById.get(relation.getParentTaskId()),
                taskKeysById.get(relation.getChildTaskId()),
                relation.getRelationMode().name(),
                relation.getRelationType().name(),
                relation.getSourceMode().name(),
                relation.getSelectionStrategy().name(),
                relation.getReusePolicy().name(),
                readStringList(relation.getParentColumnsJson(), objectMapper),
                readStringList(relation.getChildColumnsJson(), objectMapper),
                toPrimitiveDouble(relation.getNullRate()),
                toDouble(relation.getMixedExistingRatio()),
                relation.getMinChildrenPerParent(),
                relation.getMaxChildrenPerParent(),
                relation.getMappingConfigJson(),
                relation.getSortOrder()
        );
    }

    private static List<String> readStringList(String json, ObjectMapper objectMapper) {
        try {
            if (json == null || json.isBlank()) {
                return List.of();
            }
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid relation config: " + exception.getMessage(), exception);
        }
    }

    private static Double toDouble(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.doubleValue();
    }

    private static double toPrimitiveDouble(BigDecimal value) {
        if (value == null) {
            return 0D;
        }
        return value.doubleValue();
    }
}
