package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class OracleDatabaseDialect extends AbstractJdbcDatabaseDialect {

    @Override
    public DatabaseType type() {
        return DatabaseType.ORACLE;
    }

    @Override
    public String buildJdbcUrl(TargetConnection connection) {
        return "jdbc:oracle:thin:@//%s:%d/%s".formatted(
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
            return connection.getSchemaName().trim().toUpperCase(Locale.ROOT);
        }
        return connection.getUsername().trim().toUpperCase(Locale.ROOT);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + validateIdentifier(identifier) + "\"";
    }

    @Override
    protected String listTablesCatalog(Connection connection, TargetConnection targetConnection) {
        return null;
    }

    @Override
    protected String listTablesSchemaPattern(Connection connection, TargetConnection targetConnection) {
        return defaultSchema(targetConnection);
    }

    @Override
    protected TableReference resolveTableReference(TargetConnection targetConnection, String tableName) {
        List<String> segments = splitTableName(tableName);
        return switch (segments.size()) {
            case 1 -> new TableReference(null, defaultSchema(targetConnection), validateIdentifier(segments.get(0)));
            case 2 -> new TableReference(null, validateIdentifier(segments.get(0)).toUpperCase(Locale.ROOT), validateIdentifier(segments.get(1)));
            default -> throw new IllegalArgumentException("Oracle 表名格式不正确: " + tableName);
        };
    }

    @Override
    protected String resolveTableCatalog(Connection connection, TargetConnection targetConnection, TableReference tableReference) {
        return null;
    }

    @Override
    protected String resolveTableSchema(Connection connection, TargetConnection targetConnection, TableReference tableReference) {
        return tableReference.schemaName();
    }

    @Override
    protected String normalizeDbType(String typeName) {
        if (typeName == null) {
            return "VARCHAR2";
        }
        String normalized = typeName.toUpperCase(Locale.ROOT);
        if ("VARCHAR".equals(normalized)) {
            return "VARCHAR2";
        }
        return normalized;
    }

    @Override
    protected String renderColumnType(WriteTaskColumn column) {
        String typeName = normalizeDbType(column.getDbType());
        return switch (typeName) {
            case "BOOLEAN", "BOOL", "BIT" -> "NUMBER(1)";
            case "UUID" -> "VARCHAR2(36)";
            case "TEXT", "CLOB" -> "CLOB";
            case "DATETIME", "TIMESTAMP" -> "TIMESTAMP";
            case "DATE" -> "DATE";
            case "VARCHAR", "VARCHAR2" -> typeWithLength("VARCHAR2", column, 255);
            case "NVARCHAR", "NVARCHAR2" -> typeWithLength("NVARCHAR2", column, 255);
            case "CHAR", "NCHAR" -> typeWithLength(typeName, column, 1);
            case "BIGINT" -> "NUMBER(19)";
            case "INT", "INTEGER" -> "NUMBER(10)";
            case "SMALLINT" -> "NUMBER(5)";
            case "TINYINT" -> "NUMBER(3)";
            case "DECIMAL", "NUMERIC", "NUMBER" -> numberType(column);
            default -> super.renderColumnType(column);
        };
    }

    private String typeWithLength(String typeName, WriteTaskColumn column, int fallbackLength) {
        int length = column.getLengthValue() == null ? fallbackLength : column.getLengthValue();
        return typeName + "(" + length + ")";
    }

    private String numberType(WriteTaskColumn column) {
        if (column.getPrecisionValue() != null && column.getScaleValue() != null) {
            return "NUMBER(" + column.getPrecisionValue() + "," + column.getScaleValue() + ")";
        }
        if (column.getPrecisionValue() != null) {
            return "NUMBER(" + column.getPrecisionValue() + ")";
        }
        return "NUMBER";
    }
}
