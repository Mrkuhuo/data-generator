package com.datagenerator.dataset.preview;

import com.datagenerator.dataset.domain.DatasetDefinition;
import java.util.List;
import java.util.Map;

public record GeneratedDatasetBatch(
        DatasetDefinition dataset,
        int count,
        long seed,
        List<Map<String, Object>> rows
) {
}

