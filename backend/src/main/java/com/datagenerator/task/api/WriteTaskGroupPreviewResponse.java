package com.datagenerator.task.api;

import java.util.List;

public record WriteTaskGroupPreviewResponse(
        long seed,
        List<WriteTaskGroupPreviewTableResponse> tables
) {
}
