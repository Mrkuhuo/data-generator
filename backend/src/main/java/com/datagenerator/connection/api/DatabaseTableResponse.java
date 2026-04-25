package com.datagenerator.connection.api;

public record DatabaseTableResponse(
        String schemaName,
        String tableName
) {
}
