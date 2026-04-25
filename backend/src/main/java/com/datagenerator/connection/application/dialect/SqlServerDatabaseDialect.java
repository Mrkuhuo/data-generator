package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class SqlServerDatabaseDialect extends AbstractJdbcDatabaseDialect {

    @Override
    public DatabaseType type() {
        return DatabaseType.SQLSERVER;
    }

    @Override
    public String buildJdbcUrl(TargetConnection connection) {
        return "jdbc:sqlserver://%s:%d;databaseName=%s".formatted(
                connection.getHost(),
                connection.getPort(),
                connection.getDatabaseName()
        );
    }

    @Override
    public String normalizeParamsForStorage(String jdbcParams) {
        Map<String, String> params = new LinkedHashMap<>(parseParams(jdbcParams));
        putIfMissingIgnoreCase(params, "encrypt", "true");
        putIfMissingIgnoreCase(params, "trustServerCertificate", "true");
        return toParamString(params);
    }

    @Override
    public String defaultSchema(TargetConnection connection) {
        if (connection.getSchemaName() != null && !connection.getSchemaName().isBlank()) {
            return connection.getSchemaName().trim();
        }
        return "dbo";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "[" + validateIdentifier(identifier) + "]";
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
            case 3 -> new TableReference(validateIdentifier(segments.get(0)), validateIdentifier(segments.get(1)), validateIdentifier(segments.get(2)));
            default -> throw new IllegalArgumentException("SQL Server 表名格式不正确: " + tableName);
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
    protected String paramJoiner() {
        return ";";
    }

    @Override
    protected String renderColumnType(WriteTaskColumn column) {
        String typeName = normalizeDbType(column.getDbType());
        return switch (typeName) {
            case "BOOLEAN", "BOOL", "BIT" -> "BIT";
            case "UUID", "UNIQUEIDENTIFIER" -> "UNIQUEIDENTIFIER";
            case "TEXT", "CLOB" -> "NVARCHAR(MAX)";
            case "DATETIME", "TIMESTAMP" -> "DATETIME2";
            case "VARCHAR2", "NVARCHAR2", "NVARCHAR" -> typeWithLength("NVARCHAR", column, 255);
            default -> super.renderColumnType(column);
        };
    }

    private String typeWithLength(String typeName, WriteTaskColumn column, int fallbackLength) {
        int length = column.getLengthValue() == null ? fallbackLength : column.getLengthValue();
        return typeName + "(" + length + ")";
    }
}
