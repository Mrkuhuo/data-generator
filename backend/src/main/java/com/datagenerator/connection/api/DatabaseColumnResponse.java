package com.datagenerator.connection.api;

public record DatabaseColumnResponse(
        String columnName,
        String dbType,
        Integer length,
        Integer precision,
        Integer scale,
        boolean nullable,
        boolean primaryKey,
        boolean autoIncrement,
        java.util.List<String> enumValues
) {
}
