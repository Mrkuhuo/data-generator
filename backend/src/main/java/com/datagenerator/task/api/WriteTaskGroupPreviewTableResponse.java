package com.datagenerator.task.api;

import java.util.List;
import java.util.Map;

public record WriteTaskGroupPreviewTableResponse(
        Long taskId,
        String taskKey,
        String taskName,
        String tableName,
        Integer generatedRowCount,
        Integer previewRowCount,
        Integer foreignKeyMissCount,
        Integer nullViolationCount,
        List<Map<String, Object>> rows
) {
}
