package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class PostgresqlDatabaseDialect extends AbstractJdbcDatabaseDialect {

    @Override
    public DatabaseType type() {
        return DatabaseType.POSTGRESQL;
    }

    @Override
    public String buildJdbcUrl(TargetConnection connection) {
        return "jdbc:postgresql://%s:%d/%s".formatted(
                connection.getHost(),
                connection.getPort(),
                connection.getDatabaseName()
        );
    }

    @Override
    public String normalizeParamsForStorage(String jdbcParams) {
        Map<String, String> params = new LinkedHashMap<>(parseParams(jdbcParams));
        return params.isEmpty() ? null : toParamString(params);
    }

    @Override
    public String defaultSchema(TargetConnection connection) {
        if (connection.getSchemaName() != null && !connection.getSchemaName().isBlank()) {
            return connection.getSchemaName().trim();
        }
        return "public";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + validateIdentifier(identifier) + "\"";
    }

    @Override
    protected String listTablesCatalog(Connection connection, TargetConnection targetConnection) {
        return targetConnection.getDatabaseName();
    }

    @Override
    protected String listTablesSchemaPattern(Connection connection, TargetConnection targetConnection) {
        return defaultSchema(targetConnection);
    }

    @Override
    protected TableReference resolveTableReference(TargetConnection targetConnection, String tableName) {
        List<String> segments = splitTableName(tableName);
        return switch (segments.size()) {
            case 1 -> new TableReference(targetConnection.getDatabaseName(), defaultSchema(targetConnection), validateIdentifier(segments.get(0)));
            case 2 -> new TableReference(targetConnection.getDatabaseName(), validateIdentifier(segments.get(0)), validateIdentifier(segments.get(1)));
            default -> throw new IllegalArgumentException("PostgreSQL 表名格式不正确: " + tableName);
        };
    }

    @Override
    protected String resolveTableCatalog(Connection connection, TargetConnection targetConnection, TableReference tableReference) {
        return tableReference.catalogName();
    }

    @Override
    protected String resolveTableSchema(Connection connection, TargetConnection targetConnection, TableReference tableReference) {
        return tableReference.schemaName();
    }

    @Override
    public String buildInsertSql(TargetConnection targetConnection, String tableName, List<WriteTaskColumn> columns) {
        String quotedColumns = columns.stream()
                .map(WriteTaskColumn::getColumnName)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = columns.stream()
                .map(this::renderInsertPlaceholder)
                .collect(Collectors.joining(", "));
        return "INSERT INTO " + quoteQualifiedIdentifier(targetConnection, tableName)
                + " (" + quotedColumns + ") VALUES (" + placeholders + ")";
    }

    @Override
    protected String renderColumnType(WriteTaskColumn column) {
        String typeName = normalizeDbType(column.getDbType());
        return switch (typeName) {
            case "BOOLEAN", "BOOL", "BIT" -> "BOOLEAN";
            case "UUID" -> "UUID";
            case "TEXT", "CLOB" -> "TEXT";
            case "DATETIME" -> "TIMESTAMP";
            default -> super.renderColumnType(column);
        };
    }

    private String renderInsertPlaceholder(WriteTaskColumn column) {
        String typeName = normalizeDbType(column.getDbType());
        return switch (typeName) {
            case "JSON" -> "CAST(? AS JSON)";
            case "JSONB" -> "CAST(? AS JSONB)";
            default -> "?";
        };
    }
}
