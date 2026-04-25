package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.api.DatabaseForeignKeyResponse;
import com.datagenerator.connection.api.DatabaseModelResponse;
import com.datagenerator.connection.api.DatabaseTableSchemaResponse;
import com.datagenerator.connection.api.DatabaseTableResponse;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractJdbcDatabaseDialect implements DatabaseDialect {

    @Override
    public Properties buildConnectionProperties(TargetConnection connection) {
        Properties properties = new Properties();
        properties.setProperty("user", connection.getUsername());
        properties.setProperty("password", connection.getPasswordValue());
        parseParams(connection.getJdbcParams()).forEach(properties::setProperty);
        return properties;
    }

    @Override
    public String normalizeParamsForStorage(String jdbcParams) {
        String normalized = normalizeParams(jdbcParams);
        return normalized.isBlank() ? null : normalized;
    }

    @Override
    public List<DatabaseTableResponse> listTables(Connection connection, TargetConnection targetConnection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        List<DatabaseTableResponse> tables = new ArrayList<>();
        try (ResultSet resultSet = metadata.getTables(
                listTablesCatalog(connection, targetConnection),
                listTablesSchemaPattern(connection, targetConnection),
                "%",
                supportedTableTypes()
        )) {
            while (resultSet.next()) {
                tables.add(new DatabaseTableResponse(
                        extractTableSchema(resultSet),
                        resultSet.getString("TABLE_NAME")
                ));
            }
        }
        return tables;
    }

    @Override
    public List<DatabaseColumnResponse> listColumns(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName 不能为空");
        }

        DatabaseMetaData metadata = connection.getMetaData();
        TableReference tableReference = resolveTableReference(targetConnection, tableName);
        Set<String> primaryKeys = new HashSet<>();

        try (ResultSet primaryKeySet = metadata.getPrimaryKeys(
                resolveTableCatalog(connection, targetConnection, tableReference),
                resolveTableSchema(connection, targetConnection, tableReference),
                tableReference.tableName()
        )) {
            while (primaryKeySet.next()) {
                primaryKeys.add(primaryKeySet.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
            }
        }

        List<DatabaseColumnResponse> columns = new ArrayList<>();
        try (ResultSet resultSet = metadata.getColumns(
                resolveTableCatalog(connection, targetConnection, tableReference),
                resolveTableSchema(connection, targetConnection, tableReference),
                tableReference.tableName(),
                "%"
        )) {
            while (resultSet.next()) {
                String normalizedType = normalizeDbType(resultSet.getString("TYPE_NAME"));
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                boolean columnSizeMissing = resultSet.wasNull();
                int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
                boolean decimalDigitsMissing = resultSet.wasNull();
                String columnName = resultSet.getString("COLUMN_NAME");

                columns.add(new DatabaseColumnResponse(
                        columnName,
                        normalizedType,
                        isLengthType(normalizedType) && !columnSizeMissing ? columnSize : null,
                        isNumericType(normalizedType) && !columnSizeMissing ? columnSize : null,
                        isNumericType(normalizedType) && !decimalDigitsMissing ? decimalDigits : null,
                        resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable,
                        primaryKeys.contains(columnName.toLowerCase(Locale.ROOT)),
                        "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT")),
                        null
                ));
            }
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("未找到数据表: " + tableName);
        }
        return columns;
    }

    @Override
    public List<DatabaseForeignKeyResponse> listForeignKeys(
            Connection connection,
            TargetConnection targetConnection,
            String tableName
    ) throws SQLException {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName 涓嶈兘涓虹┖");
        }

        DatabaseMetaData metadata = connection.getMetaData();
        TableReference tableReference = resolveTableReference(targetConnection, tableName);
        LinkedHashMap<String, ForeignKeyBuilder> builders = new LinkedHashMap<>();

        try (ResultSet resultSet = metadata.getImportedKeys(
                resolveTableCatalog(connection, targetConnection, tableReference),
                resolveTableSchema(connection, targetConnection, tableReference),
                tableReference.tableName()
        )) {
            while (resultSet.next()) {
                String constraintName = normalizeConstraintName(resultSet.getString("FK_NAME"), tableReference.tableName(), builders.size() + 1);
                String parentTable = qualifyTableName(
                        resultSet.getString("PKTABLE_SCHEM"),
                        resultSet.getString("PKTABLE_CAT"),
                        resultSet.getString("PKTABLE_NAME")
                );
                String childTable = qualifyTableName(
                        resultSet.getString("FKTABLE_SCHEM"),
                        resultSet.getString("FKTABLE_CAT"),
                        resultSet.getString("FKTABLE_NAME")
                );
                ForeignKeyBuilder builder = builders.computeIfAbsent(
                        constraintName,
                        ignored -> new ForeignKeyBuilder(constraintName, parentTable, childTable)
                );
                builder.parentColumns().add(resultSet.getString("PKCOLUMN_NAME"));
                builder.childColumns().add(resultSet.getString("FKCOLUMN_NAME"));
            }
        }

        return builders.values().stream()
                .map(ForeignKeyBuilder::build)
                .toList();
    }

    @Override
    public DatabaseTableSchemaResponse describeTable(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException {
        return new DatabaseTableSchemaResponse(
                tableName,
                listColumns(connection, targetConnection, tableName),
                listForeignKeys(connection, targetConnection, tableName)
        );
    }

    @Override
    public DatabaseModelResponse describeModel(Connection connection, TargetConnection targetConnection, List<String> tableNames) throws SQLException {
        List<String> normalizedTables = tableNames == null ? List.of() : tableNames.stream()
                .filter(tableName -> tableName != null && !tableName.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
        List<DatabaseTableSchemaResponse> tables = new ArrayList<>();
        List<DatabaseForeignKeyResponse> relations = new ArrayList<>();
        for (String tableName : normalizedTables) {
            DatabaseTableSchemaResponse schema = describeTable(connection, targetConnection, tableName);
            tables.add(schema);
            relations.addAll(schema.foreignKeys());
        }
        return new DatabaseModelResponse(tables, relations);
    }

    @Override
    public boolean tableExists(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException {
        TableReference tableReference = resolveTableReference(targetConnection, tableName);
        DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getTables(
                resolveTableCatalog(connection, targetConnection, tableReference),
                resolveTableSchema(connection, targetConnection, tableReference),
                tableReference.tableName(),
                supportedTableTypes()
        )) {
            return resultSet.next();
        }
    }

    @Override
    public Long queryMaxValue(Connection connection, TargetConnection targetConnection, String tableName, String columnName) throws SQLException {
        String sql = "SELECT MAX(" + quoteIdentifier(columnName) + ") FROM "
                + quoteQualifiedIdentifier(targetConnection, tableName);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            Object value = resultSet.getObject(1);
            if (value == null) {
                return null;
            }
            if (value instanceof Number number) {
                return number.longValue();
            }
            return Long.parseLong(value.toString());
        }
    }

    @Override
    public void createTableIfMissing(Connection connection, TargetConnection targetConnection, WriteTask task) throws SQLException {
        if (tableExists(connection, targetConnection, task.getTableName())) {
            return;
        }

        String columnDefinitions = task.getColumns().stream()
                .map(this::buildColumnDefinition)
                .collect(Collectors.joining(", "));
        String primaryKeyDefinition = buildPrimaryKeyDefinition(task.getColumns());
        String sql = "CREATE TABLE " + quoteQualifiedIdentifier(targetConnection, task.getTableName()) + " ("
                + columnDefinitions
                + (primaryKeyDefinition == null ? "" : ", " + primaryKeyDefinition)
                + ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public long countRows(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quoteQualifiedIdentifier(targetConnection, tableName);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Override
    public void clearTargetTable(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM " + quoteQualifiedIdentifier(targetConnection, tableName));
        }
    }

    @Override
    public String buildInsertSql(TargetConnection targetConnection, String tableName, List<WriteTaskColumn> columns) {
        String quotedColumns = columns.stream()
                .map(WriteTaskColumn::getColumnName)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = columns.stream().map(column -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + quoteQualifiedIdentifier(targetConnection, tableName)
                + " (" + quotedColumns + ") VALUES (" + placeholders + ")";
    }

    protected String buildColumnDefinition(WriteTaskColumn column) {
        StringBuilder definition = new StringBuilder();
        definition.append(quoteIdentifier(column.getColumnName()))
                .append(" ")
                .append(renderColumnType(column));
        if (!column.isNullableFlag()) {
            definition.append(" NOT NULL");
        }
        return definition.toString();
    }

    protected String buildPrimaryKeyDefinition(List<WriteTaskColumn> columns) {
        List<WriteTaskColumn> primaryKeys = columns.stream()
                .filter(WriteTaskColumn::isPrimaryKeyFlag)
                .toList();
        if (primaryKeys.isEmpty()) {
            return null;
        }
        return "PRIMARY KEY (" + primaryKeys.stream()
                .map(WriteTaskColumn::getColumnName)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String quoteQualifiedIdentifier(TargetConnection targetConnection, String identifier) {
        TableReference tableReference = resolveTableReference(targetConnection, identifier);
        List<String> segments = new ArrayList<>();
        if (tableReference.catalogName() != null && !tableReference.catalogName().isBlank()) {
            segments.add(quoteIdentifier(tableReference.catalogName()));
        }
        if (tableReference.schemaName() != null && !tableReference.schemaName().isBlank()) {
            segments.add(quoteIdentifier(tableReference.schemaName()));
        }
        segments.add(quoteIdentifier(tableReference.tableName()));
        return String.join(".", segments);
    }

    protected String normalizeParams(String jdbcParams) {
        if (jdbcParams == null || jdbcParams.isBlank()) {
            return "";
        }
        String normalized = jdbcParams.trim();
        while (normalized.startsWith("?") || normalized.startsWith(";") || normalized.startsWith("&")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }

    protected LinkedHashMap<String, String> parseParams(String jdbcParams) {
        LinkedHashMap<String, String> params = new LinkedHashMap<>();
        String normalized = normalizeParams(jdbcParams);
        if (normalized.isBlank()) {
            return params;
        }

        for (String pair : normalized.split(paramSplitRegex())) {
            if (pair == null || pair.isBlank()) {
                continue;
            }
            int separatorIndex = pair.indexOf('=');
            if (separatorIndex < 0) {
                params.put(pair.trim(), "");
                continue;
            }
            String key = pair.substring(0, separatorIndex).trim();
            String value = pair.substring(separatorIndex + 1).trim();
            if (!key.isBlank()) {
                params.put(key, value);
            }
        }
        return params;
    }

    protected String toParamString(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(paramJoiner()));
    }

    protected void putIfMissingIgnoreCase(Map<String, String> params, String key, String value) {
        if (getIgnoreCase(params, key) == null) {
            params.put(key, value);
        }
    }

    protected void putIgnoreCase(Map<String, String> params, String key, String value) {
        String existingKey = findKeyIgnoreCase(params, key);
        if (existingKey != null) {
            params.put(existingKey, value);
            return;
        }
        params.put(key, value);
    }

    protected String getIgnoreCase(Map<String, String> params, String key) {
        String existingKey = findKeyIgnoreCase(params, key);
        return existingKey == null ? null : params.get(existingKey);
    }

    protected String findKeyIgnoreCase(Map<String, String> params, String key) {
        return params.keySet().stream()
                .filter(existingKey -> existingKey.equalsIgnoreCase(key))
                .findFirst()
                .orElse(null);
    }

    protected String normalizeDbType(String typeName) {
        return typeName == null ? "VARCHAR" : typeName.toUpperCase(Locale.ROOT);
    }

    protected String renderColumnType(WriteTaskColumn column) {
        String typeName = normalizeDbType(column.getDbType());
        if (column.getLengthValue() != null && isLengthType(typeName)) {
            return typeName + "(" + column.getLengthValue() + ")";
        }
        if (column.getPrecisionValue() != null) {
            if (column.getScaleValue() != null) {
                return typeName + "(" + column.getPrecisionValue() + "," + column.getScaleValue() + ")";
            }
            return typeName + "(" + column.getPrecisionValue() + ")";
        }
        return typeName;
    }

    protected boolean isLengthType(String typeName) {
        return typeName.contains("CHAR") || typeName.contains("BINARY");
    }

    protected boolean isNumericType(String typeName) {
        return typeName.contains("DECIMAL")
                || typeName.contains("NUMERIC")
                || typeName.contains("NUMBER")
                || typeName.contains("INT")
                || typeName.contains("FLOAT")
                || typeName.contains("DOUBLE")
                || typeName.contains("REAL");
    }

    protected String[] supportedTableTypes() {
        return new String[]{"TABLE"};
    }

    protected String paramJoiner() {
        return "&";
    }

    protected String paramSplitRegex() {
        return "[&;]";
    }

    protected String extractTableSchema(ResultSet resultSet) throws SQLException {
        String schemaName = resultSet.getString("TABLE_SCHEM");
        if (schemaName != null && !schemaName.isBlank()) {
            return schemaName;
        }
        String catalogName = resultSet.getString("TABLE_CAT");
        return catalogName == null || catalogName.isBlank() ? null : catalogName;
    }

    protected abstract String listTablesCatalog(Connection connection, TargetConnection targetConnection) throws SQLException;

    protected abstract String listTablesSchemaPattern(Connection connection, TargetConnection targetConnection) throws SQLException;

    protected abstract TableReference resolveTableReference(TargetConnection targetConnection, String tableName);

    protected abstract String resolveTableCatalog(Connection connection, TargetConnection targetConnection, TableReference tableReference) throws SQLException;

    protected abstract String resolveTableSchema(Connection connection, TargetConnection targetConnection, TableReference tableReference) throws SQLException;

    protected List<String> splitTableName(String tableName) {
        return List.of(tableName.trim().split("\\."));
    }

    protected String validateIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException("标识符不能为空");
        }
        String trimmed = identifier.trim();
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_$#]*")) {
            throw new IllegalArgumentException("不支持的标识符: " + identifier);
        }
        return trimmed;
    }

    private String normalizeConstraintName(String constraintName, String tableName, int ordinal) {
        if (constraintName != null && !constraintName.isBlank()) {
            return constraintName;
        }
        return tableName + "_fk_" + ordinal;
    }

    private String qualifyTableName(String schemaName, String catalogName, String tableName) {
        if (schemaName == null || schemaName.isBlank()) {
            if (catalogName == null || catalogName.isBlank()) {
                return tableName;
            }
            return catalogName + "." + tableName;
        }
        return schemaName + "." + tableName;
    }

    private record ForeignKeyBuilder(
            String constraintName,
            String parentTable,
            String childTable,
            List<String> parentColumns,
            List<String> childColumns
    ) {
        private ForeignKeyBuilder(String constraintName, String parentTable, String childTable) {
            this(constraintName, parentTable, childTable, new ArrayList<>(), new ArrayList<>());
        }

        private DatabaseForeignKeyResponse build() {
            return new DatabaseForeignKeyResponse(
                    constraintName,
                    parentTable,
                    List.copyOf(parentColumns),
                    childTable,
                    List.copyOf(childColumns)
            );
        }
    }
}
