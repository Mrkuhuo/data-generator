package com.datagenerator.connector.api;

public record ConnectorTestResponse(
        Long connectorId,
        boolean success,
        String status,
        String message,
        String detailsJson
) {
}

