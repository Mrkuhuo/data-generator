package com.datagenerator.connection.application.dialect;

public record TableReference(
        String catalogName,
        String schemaName,
        String tableName
) {
}
