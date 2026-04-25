package com.datagenerator.connection.api;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnectionStatus;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record TargetConnectionUpsertRequest(
        String name,
        @NotNull DatabaseType dbType,
        String host,
        @NotNull @Min(1) @Max(65535) Integer port,
        String databaseName,
        String schemaName,
        String username,
        String password,
        String jdbcParams,
        String configJson,
        @NotNull TargetConnectionStatus status,
        String description
) {
}
