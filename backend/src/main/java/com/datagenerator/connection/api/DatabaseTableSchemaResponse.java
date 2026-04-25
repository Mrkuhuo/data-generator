package com.datagenerator.connection.api;

import java.util.List;

public record DatabaseTableSchemaResponse(
        String tableName,
        List<DatabaseColumnResponse> columns,
        List<DatabaseForeignKeyResponse> foreignKeys
) {
}
