package com.datagenerator.dataset.api;

import java.util.List;
import java.util.Map;

public record DatasetPreviewResponse(
        int count,
        long seed,
        List<Map<String, Object>> rows
) {
}

