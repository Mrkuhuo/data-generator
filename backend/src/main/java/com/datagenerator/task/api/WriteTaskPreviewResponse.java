package com.datagenerator.task.api;

import java.util.List;
import java.util.Map;

public record WriteTaskPreviewResponse(
        int count,
        long seed,
        List<Map<String, Object>> rows
) {
}
