package com.datagenerator.system.application;

import com.datagenerator.connection.domain.DatabaseType;

public record DemoDataRebuildResponse(
        Long replacedConnectionId,
        Long connectionId,
        String connectionName,
        DatabaseType dbType,
        String host,
        Integer port,
        String databaseName,
        String jdbcParams,
        Long taskId,
        String taskName,
        String tableName
) {
}
