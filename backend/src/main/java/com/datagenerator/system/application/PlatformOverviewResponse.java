package com.datagenerator.system.application;

public record PlatformOverviewResponse(
        long connectorCount,
        long datasetCount,
        long jobCount,
        long executionCount,
        long connectionCount,
        long writeTaskCount,
        long writeExecutionCount
) {
}
