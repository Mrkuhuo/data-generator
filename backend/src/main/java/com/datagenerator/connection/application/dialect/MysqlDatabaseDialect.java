package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class MysqlDatabaseDialect extends AbstractJdbcDatabaseDialect {

    private static final String DEFAULT_MYSQL_CHARACTER_ENCODING = "UTF-8";
    private static final String DEFAULT_MYSQL_SERVER_TIMEZONE = "Asia/Shanghai";

    @Override
    public DatabaseType type() {
        return DatabaseType.MYSQL;
    }

    @Override
    public String buildJdbcUrl(TargetConnection connection) {
        return "jdbc:mysql://%s:%d/%s".formatted(
                connection.getHost(),
                connection.getPort(),
                connection.getDatabaseName()
        );
    }

    @Override
    public String normalizeParamsForStorage(String jdbcParams) {
        Map<String, String> params = parseParams(jdbcParams);
        putIfMissingIgnoreCase(params, "useUnicode", "true");

        String characterEncoding = getIgnoreCase(params, "characterEncoding");
        if (characterEncoding == null || characterEncoding.isBlank() || isLegacyUtf8(characterEncoding)) {
            putIgnoreCase(params, "characterEncoding", DEFAULT_MYSQL_CHARACTER_ENCODING);
        }

        putIfMissingIgnoreCase(params, "serverTimezone", DEFAULT_MYSQL_SERVER_TIMEZONE);
        return toParamString(params);
    }

    @Override
    public String defaultSchema(TargetConnection connection) {
        return connection.getDatabaseName();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + validateIdentifier(identifier) + "`";
    }

    @Override
    protected String listTablesCatalog(Connection connection, TargetConnection targetConnection) {
        return targetConnection.getDatabaseName();
    }

    @Override
    protected String listTablesSchemaPattern(Connection connection, TargetConnection targetConnection) {
        return null;
    }

    @Override
    protected TableReference resolveTableReference(TargetConnection targetConnection, String tableName) {
        List<String> segments = splitTableName(tableName);
        return switch (segments.size()) {
            case 1 -> new TableReference(targetConnection.getDatabaseName(), null, validateIdentifier(segments.get(0)));
            case 2 -> new TableReference(validateIdentifier(segments.get(0)), null, validateIdentifier(segments.get(1)));
            default -> throw new IllegalArgumentException("MySQL 表名格式不正确: " + tableName);
        };
    }

    @Override
    protected String resolveTableCatalog(Connection connection, TargetConnection targetConnection, TableReference tableReference) {
        return tableReference.catalogName();
    }

    @Override
    protected String resolveTableSchema(Connection connection, TargetConnection targetConnection, TableReference tableReference) {
        return null;
    }

    @Override
    public List<DatabaseColumnResponse> listColumns(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException {
        List<DatabaseColumnResponse> columns = super.listColumns(connection, targetConnection, tableName);
        if (columns.stream().noneMatch(column -> "ENUM".equalsIgnoreCase(column.dbType()))) {
            return columns;
        }

        TableReference tableReference = resolveTableReference(targetConnection, tableName);
        Map<String, List<String>> enumValuesByColumn = loadEnumValues(connection, tableReference);
        if (enumValuesByColumn.isEmpty()) {
            return columns;
        }

        return columns.stream()
                .map(column -> new DatabaseColumnResponse(
                        column.columnName(),
                        column.dbType(),
                        column.length(),
                        column.precision(),
                        column.scale(),
                        column.nullable(),
                        column.primaryKey(),
                        column.autoIncrement(),
                        enumValuesByColumn.getOrDefault(column.columnName(), column.enumValues())
                ))
                .toList();
    }

    @Override
    protected String renderColumnType(WriteTaskColumn column) {
        String typeName = normalizeDbType(column.getDbType());
        return switch (typeName) {
            case "BOOLEAN", "BOOL" -> "BIT";
            case "UUID" -> "VARCHAR(36)";
            case "TEXT", "CLOB" -> "TEXT";
            case "DATETIME", "TIMESTAMP" -> typeName;
            case "VARCHAR", "NVARCHAR" -> typeWithLength("VARCHAR", column, 255);
            case "CHAR", "NCHAR" -> typeWithLength("CHAR", column, 1);
            case "VARBINARY" -> typeWithLength("VARBINARY", column, 255);
            case "BINARY" -> typeWithLength("BINARY", column, 16);
            default -> super.renderColumnType(column);
        };
    }

    private String typeWithLength(String typeName, WriteTaskColumn column, int fallbackLength) {
        int length = column.getLengthValue() == null ? fallbackLength : column.getLengthValue();
        return typeName + "(" + length + ")";
    }

    private Map<String, List<String>> loadEnumValues(Connection connection, TableReference tableReference) throws SQLException {
        String sql = """
                SELECT COLUMN_NAME, COLUMN_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME = ?
                  AND DATA_TYPE = 'enum'
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableReference.catalogName());
            statement.setString(2, tableReference.tableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                LinkedHashMap<String, List<String>> valuesByColumn = new LinkedHashMap<>();
                while (resultSet.next()) {
                    valuesByColumn.put(
                            resultSet.getString("COLUMN_NAME"),
                            parseEnumValues(resultSet.getString("COLUMN_TYPE"))
                    );
                }
                return valuesByColumn;
            }
        }
    }

    private List<String> parseEnumValues(String columnType) {
        if (columnType == null || columnType.isBlank()) {
            return List.of();
        }
        String normalized = columnType.trim();
        if (!normalized.regionMatches(true, 0, "enum(", 0, 5) || !normalized.endsWith(")")) {
            return List.of();
        }

        ArrayList<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int index = 5; index < normalized.length() - 1; index++) {
            char currentChar = normalized.charAt(index);
            if (!inQuote) {
                if (currentChar == '\'') {
                    inQuote = true;
                    current.setLength(0);
                }
                continue;
            }

            if (currentChar == '\\' && index + 1 < normalized.length() - 1) {
                current.append(normalized.charAt(index + 1));
                index++;
                continue;
            }
            if (currentChar == '\'' && index + 1 < normalized.length() - 1 && normalized.charAt(index + 1) == '\'') {
                current.append('\'');
                index++;
                continue;
            }
            if (currentChar == '\'') {
                values.add(current.toString());
                inQuote = false;
                continue;
            }
            current.append(currentChar);
        }
        return values;
    }

    private boolean isLegacyUtf8(String value) {
        return "utf8".equalsIgnoreCase(value)
                || "utf-8".equalsIgnoreCase(value)
                || "utf8mb4".equalsIgnoreCase(value);
    }
}
