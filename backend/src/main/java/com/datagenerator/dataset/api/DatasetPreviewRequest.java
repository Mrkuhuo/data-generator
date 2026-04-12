package com.datagenerator.dataset.api;

public record DatasetPreviewRequest(
        Integer count,
        Long seed
) {
}

