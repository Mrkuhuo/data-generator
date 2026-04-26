package com.datagenerator.task.application;

import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class WriteTaskJdbcWriter implements WriteTaskDeliveryWriter {

    private final ConnectionJdbcSupport jdbcSupport;
    private final ObjectMapper objectMapper;

    public WriteTaskJdbcWriter(ConnectionJdbcSupport jdbcSupport, ObjectMapper objectMapper) {
        this.jdbcSupport = jdbcSupport;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean supports(DatabaseType databaseType) {
        return databaseType != DatabaseType.KAFKA;
    }

    @Override
    public WriteTaskDeliveryResult write(
            WriteTask task,
            TargetConnection connection,
            List<Map<String, Object>> rows,
            Long executionId
    ) throws Exception {
        try (Connection jdbcConnection = openTransactionalConnection(connection)) {
            try {
                WriteTaskDeliveryResult result = writeWithinTransaction(task, connection, jdbcConnection, rows, executionId);
                jdbcConnection.commit();
                return result;
            } catch (Exception exception) {
                jdbcConnection.rollback();
                throw exception;
            }
        }
    }

    public WriteTaskDeliveryResult write(WriteTask task, TargetConnection connection, List<Map<String, Object>> rows) throws Exception {
        return write(task, connection, rows, null);
    }

    public Connection openTransactionalConnection(TargetConnection connection) throws Exception {
        Connection jdbcConnection = jdbcSupport.open(connection);
        jdbcConnection.setAutoCommit(false);
        return jdbcConnection;
    }

    public WriteTaskDeliveryResult writeWithinTransaction(
            WriteTask task,
            TargetConnection connection,
            Connection jdbcConnection,
            List<Map<String, Object>> rows,
            Long executionId
    ) throws Exception {
        DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
        if (task.getTableMode() == com.datagenerator.task.domain.TableMode.CREATE_IF_MISSING) {
            dialect.createTableIfMissing(jdbcConnection, connection, task);
        }
        Long beforeRowCount = tryCountRows(jdbcConnection, connection, task.getTableName());

        if (task.getWriteMode() == com.datagenerator.task.domain.WriteMode.OVERWRITE) {
            dialect.clearTargetTable(jdbcConnection, connection, task.getTableName());
        }

        if (!rows.isEmpty()) {
            insertRows(jdbcConnection, task, connection, rows);
        }
        long afterRowCount = dialect.countRows(jdbcConnection, connection, task.getTableName());
        long safeBeforeRowCount = beforeRowCount == null ? 0 : beforeRowCount;

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("deliveryType", "JDBC");
        details.put("beforeWriteRowCount", safeBeforeRowCount);
        details.put("afterWriteRowCount", afterRowCount);
        details.put("rowDelta", afterRowCount - safeBeforeRowCount);
        details.put("writtenRowCount", rows.size());

        return new WriteTaskDeliveryResult(rows.size(), 0, "目标表写入完成", details);
    }

    private Long tryCountRows(Connection connection, TargetConnection targetConnection, String tableName) {
        try {
            return jdbcSupport.dialect(targetConnection.getDbType()).countRows(connection, targetConnection, tableName);
        } catch (SQLException exception) {
            return null;
        }
    }

    private void insertRows(Connection connection, WriteTask task, TargetConnection targetConnection, List<Map<String, Object>> rows) throws Exception {
        List<WriteTaskColumn> columns = resolveEffectiveColumns(connection, task, targetConnection);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("写入任务缺少字段定义");
        }

        String sql = jdbcSupport.dialect(targetConnection.getDbType()).buildInsertSql(targetConnection, task.getTableName(), columns);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int currentBatchSize = 0;
            for (Map<String, Object> row : rows) {
                for (int index = 0; index < columns.size(); index++) {
                    WriteTaskColumn column = columns.get(index);
                    bindValue(statement, index + 1, targetConnection.getDbType(), column, row.get(column.getColumnName()));
                }
                statement.addBatch();
                currentBatchSize++;
                if (currentBatchSize >= task.getBatchSize()) {
                    statement.executeBatch();
                    currentBatchSize = 0;
                }
            }
            if (currentBatchSize > 0) {
                statement.executeBatch();
            }
        }
    }

    private List<WriteTaskColumn> resolveEffectiveColumns(
            Connection connection,
            WriteTask task,
            TargetConnection targetConnection
    ) throws SQLException {
        List<WriteTaskColumn> columns = task.getColumns();
        if (task.getTableMode() != TableMode.USE_EXISTING || columns.isEmpty()) {
            return columns;
        }

        DatabaseDialect dialect = jdbcSupport.dialect(targetConnection.getDbType());
        List<DatabaseColumnResponse> actualColumns = dialect.listColumns(connection, targetConnection, task.getTableName());
        if (actualColumns.isEmpty()) {
            return columns;
        }

        Map<String, DatabaseColumnResponse> actualByName = new LinkedHashMap<>();
        for (DatabaseColumnResponse actualColumn : actualColumns) {
            actualByName.put(actualColumn.columnName().toLowerCase(Locale.ROOT), actualColumn);
        }

        List<WriteTaskColumn> effectiveColumns = new ArrayList<>();
        boolean changed = false;
        for (WriteTaskColumn column : columns) {
            DatabaseColumnResponse actualColumn = actualByName.get(column.getColumnName().toLowerCase(Locale.ROOT));
            if (actualColumn == null) {
                effectiveColumns.add(column);
                continue;
            }

            if (matchesActualColumn(column, actualColumn)) {
                effectiveColumns.add(column);
                continue;
            }

            effectiveColumns.add(copyWithActualType(column, actualColumn));
            changed = true;
        }

        return changed ? effectiveColumns : columns;
    }

    private boolean matchesActualColumn(WriteTaskColumn column, DatabaseColumnResponse actualColumn) {
        return equalsIgnoreCase(column.getDbType(), actualColumn.dbType())
                && equalsNullable(column.getLengthValue(), actualColumn.length())
                && equalsNullable(column.getPrecisionValue(), actualColumn.precision())
                && equalsNullable(column.getScaleValue(), actualColumn.scale());
    }

    private boolean equalsIgnoreCase(String left, String right) {
        if (left == null || right == null) {
            return left == right;
        }
        return left.equalsIgnoreCase(right);
    }

    private boolean equalsNullable(Integer left, Integer right) {
        return left == null ? right == null : left.equals(right);
    }

    private WriteTaskColumn copyWithActualType(WriteTaskColumn column, DatabaseColumnResponse actualColumn) {
        WriteTaskColumn copy = new WriteTaskColumn();
        copy.setColumnName(column.getColumnName());
        copy.setDbType(actualColumn.dbType());
        copy.setLengthValue(actualColumn.length());
        copy.setPrecisionValue(actualColumn.precision());
        copy.setScaleValue(actualColumn.scale());
        copy.setNullableFlag(column.isNullableFlag());
        copy.setPrimaryKeyFlag(column.isPrimaryKeyFlag());
        copy.setGeneratorType(column.getGeneratorType());
        copy.setGeneratorConfigJson(column.getGeneratorConfigJson());
        copy.setSortOrder(column.getSortOrder());
        return copy;
    }

    private void bindValue(
            PreparedStatement statement,
            int index,
            DatabaseType databaseType,
            WriteTaskColumn column,
            Object value
    ) throws Exception {
        String dbType = column.getDbType() == null ? "" : column.getDbType().toUpperCase(Locale.ROOT);
        if (isPostgresqlJsonType(databaseType, dbType)) {
            bindPostgresqlJsonValue(statement, index, value);
            return;
        }
        if (value == null) {
            statement.setObject(index, null);
            return;
        }
        if (value instanceof String stringValue) {
            if (dbType.contains("TIMESTAMP") || dbType.contains("DATETIME")) {
                statement.setTimestamp(index, parseTimestamp(stringValue));
                return;
            }
            if ("DATE".equals(dbType)) {
                statement.setObject(index, parseDate(stringValue));
                return;
            }
            statement.setString(index, stringValue);
            return;
        }
        if (value instanceof Integer integerValue) {
            statement.setInt(index, integerValue);
            return;
        }
        if (value instanceof Long longValue) {
            statement.setLong(index, longValue);
            return;
        }
        if (value instanceof Boolean booleanValue) {
            statement.setBoolean(index, booleanValue);
            return;
        }
        if (value instanceof Double doubleValue) {
            statement.setDouble(index, doubleValue);
            return;
        }
        if (value instanceof Float floatValue) {
            statement.setFloat(index, floatValue);
            return;
        }
        if (value instanceof java.math.BigDecimal bigDecimalValue) {
            statement.setBigDecimal(index, bigDecimalValue);
            return;
        }
        if (value instanceof Instant instantValue) {
            statement.setTimestamp(index, java.sql.Timestamp.from(instantValue));
            return;
        }
        if (value instanceof LocalDateTime localDateTimeValue) {
            statement.setObject(index, localDateTimeValue);
            return;
        }
        if (value instanceof LocalDate localDateValue) {
            statement.setObject(index, localDateValue);
            return;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            statement.setString(index, objectMapper.writeValueAsString(value));
            return;
        }
        statement.setObject(index, value);
    }

    private boolean isPostgresqlJsonType(DatabaseType databaseType, String dbType) {
        return databaseType == DatabaseType.POSTGRESQL && ("JSON".equals(dbType) || "JSONB".equals(dbType));
    }

    private void bindPostgresqlJsonValue(PreparedStatement statement, int index, Object value) throws Exception {
        if (value == null) {
            statement.setNull(index, Types.OTHER);
            return;
        }
        statement.setObject(index, normalizeJsonValue(value), Types.OTHER);
    }

    private String normalizeJsonValue(Object value) throws Exception {
        if (value instanceof String stringValue) {
            String trimmed = stringValue.trim();
            if (!trimmed.isEmpty()) {
                try {
                    objectMapper.readTree(trimmed);
                    return trimmed;
                } catch (Exception ignored) {
                }
            }
            return objectMapper.writeValueAsString(stringValue);
        }
        return objectMapper.writeValueAsString(value);
    }

    private java.sql.Timestamp parseTimestamp(String value) {
        try {
            return java.sql.Timestamp.from(Instant.parse(value));
        } catch (Exception ignored) {
        }

        try {
            return java.sql.Timestamp.from(OffsetDateTime.parse(value).toInstant());
        } catch (Exception ignored) {
        }

        try {
            return java.sql.Timestamp.valueOf(LocalDateTime.parse(value.replace(" ", "T")));
        } catch (Exception exception) {
            throw new IllegalArgumentException("无法解析时间值: " + value, exception);
        }
    }

    private LocalDate parseDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (Exception ignored) {
        }

        try {
            return Instant.parse(value).atZone(java.time.ZoneOffset.UTC).toLocalDate();
        } catch (Exception ignored) {
        }

        try {
            return OffsetDateTime.parse(value).toLocalDate();
        } catch (Exception ignored) {
        }

        try {
            return LocalDateTime.parse(value.replace(" ", "T")).toLocalDate();
        } catch (Exception exception) {
            throw new IllegalArgumentException("无法解析日期值: " + value, exception);
        }
    }
}
