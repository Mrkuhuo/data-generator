package com.datagenerator.connection.api;

import java.util.List;

public record DatabaseForeignKeyResponse(
        String constraintName,
        String parentTable,
        List<String> parentColumns,
        String childTable,
        List<String> childColumns
) {
}
