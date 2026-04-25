package com.datagenerator.task.application;

import java.util.LinkedHashMap;
import java.util.Map;

public record WriteTaskDeliveryResult(
        long successCount,
        long errorCount,
        String summary,
        Map<String, Object> details
) {

    public WriteTaskDeliveryResult {
        details = details == null ? Map.of() : new LinkedHashMap<>(details);
    }
}
