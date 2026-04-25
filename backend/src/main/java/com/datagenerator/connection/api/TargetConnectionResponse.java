package com.datagenerator.connection.api;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.connection.domain.TargetConnectionStatus;
import java.time.Instant;

public record TargetConnectionResponse(
        Long id,
        Instant createdAt,
        Instant updatedAt,
        String name,
        DatabaseType dbType,
        String host,
        Integer port,
        String databaseName,
        String schemaName,
        String username,
        String jdbcParams,
        String configJson,
        TargetConnectionStatus status,
        String description,
        boolean hasPassword,
        String lastTestStatus,
        String lastTestMessage,
        String lastTestDetailsJson,
        Instant lastTestedAt
) {

    public static TargetConnectionResponse from(TargetConnection connection) {
        return new TargetConnectionResponse(
                connection.getId(),
                connection.getCreatedAt(),
                connection.getUpdatedAt(),
                connection.getName(),
                connection.getDbType(),
                connection.getHost(),
                connection.getPort(),
                connection.getDatabaseName(),
                connection.getSchemaName(),
                connection.getUsername(),
                connection.getJdbcParams(),
                connection.getConfigJson(),
                connection.getStatus(),
                connection.getDescription(),
                connection.getPasswordValue() != null && !connection.getPasswordValue().isBlank(),
                connection.getLastTestStatus(),
                connection.getLastTestMessage(),
                connection.getLastTestDetailsJson(),
                connection.getLastTestedAt()
        );
    }
}
