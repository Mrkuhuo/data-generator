package com.datagenerator.connection.api;

import java.util.List;

public record DatabaseModelResponse(
        List<DatabaseTableSchemaResponse> tables,
        List<DatabaseForeignKeyResponse> relations
) {
}
