package com.datagenerator.connector.spi;

public record ConnectorTestResult(
        boolean success,
        String status,
        String message,
        String detailsJson
) {
}

