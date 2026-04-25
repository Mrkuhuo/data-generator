package com.datagenerator.connection.api;

public record ConnectionTestResponse(
        Long connectionId,
        boolean success,
        String status,
        String message,
        String detailsJson
) {
}
