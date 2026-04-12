package com.datagenerator.connector.spi.impl;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.spi.ConnectorAdapter;
import com.datagenerator.connector.spi.ConnectorConfigSupport;
import com.datagenerator.connector.spi.ConnectorDeliveryRequest;
import com.datagenerator.connector.spi.ConnectorDeliveryResult;
import com.datagenerator.connector.spi.ConnectorTestResult;
import com.datagenerator.connector.spi.DeliveryStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract class AbstractJdbcConnectorAdapter implements ConnectorAdapter {

    private final ObjectMapper objectMapper;

    protected AbstractJdbcConnectorAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    protected abstract String databaseLabel();

    protected abstract String identifierQuote();

    @Override
    public ConnectorTestResult test(ConnectorInstance connector) {
        Map<String, Object> config = ConnectorConfigSupport.readConfig(connector);
        String jdbcUrl = ConnectorConfigSupport.requireString(config, "jdbcUrl");
        String username = ConnectorConfigSupport.requireString(config, "username");
        String password = ConnectorConfigSupport.optionalString(config, "password");

        Map<String, Object> details = baseConnectionDetails(jdbcUrl, username);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password == null ? "" : password)) {
            enrichConnectionDetails(connection, details);
            return new ConnectorTestResult(
                    true,
                    "READY",
                    databaseLabel() + " 连接测试成功",
                    ConnectorConfigSupport.writeDetails(details)
            );
        } catch (Exception exception) {
            details.put("error", exception.getMessage());
            return new ConnectorTestResult(
                    false,
                    "UNREACHABLE",
                    databaseLabel() + " 连接测试失败",
                    ConnectorConfigSupport.writeDetails(details)
            );
        }
    }

    @Override
    public ConnectorDeliveryResult deliver(ConnectorDeliveryRequest request) {
        if (request.job().getWriteStrategy() != JobWriteStrategy.APPEND
                && request.job().getWriteStrategy() != JobWriteStrategy.OVERWRITE) {
            return new ConnectorDeliveryResult(
                    DeliveryStatus.FAILED,
                    0,
                    request.rows().size(),
                    databaseLabel() + " 投递仅支持 APPEND 和 OVERWRITE",
                    ConnectorConfigSupport.writeDetails(Map.of("writeStrategy", request.job().getWriteStrategy()))
            );
        }

        Map<String, Object> connectorConfig = ConnectorConfigSupport.readConfig(request.connector());
        Map<String, Object> runtimeConfig = ConnectorConfigSupport.readConfig(
                request.job().getRuntimeConfigJson(),
                "Job runtime config"
        );

        String jdbcUrl = ConnectorConfigSupport.requireString(connectorConfig, "jdbcUrl");
        String username = ConnectorConfigSupport.requireString(connectorConfig, "username");
        String password = ConnectorConfigSupport.optionalString(connectorConfig, "password");
        String tableName = ConnectorConfigSupport.requireString(runtimeConfig, "target.table", "table", "targetTable");
        int batchSize = normalizeBatchSize(ConnectorConfigSupport.optionalInteger(runtimeConfig, "target.batchSize", "batchSize"));
        List<String> columns = resolveColumns(runtimeConfig, request.rows());

        Map<String, Object> details = baseConnectionDetails(jdbcUrl, username);
        details.put("table", tableName);
        details.put("writeStrategy", request.job().getWriteStrategy());
        details.put("batchSize", batchSize);
        details.put("requestedRows", request.rows().size());
        if (!columns.isEmpty()) {
            details.put("columns", columns);
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password == null ? "" : password);
            connection.setAutoCommit(false);

            if (request.job().getWriteStrategy() == JobWriteStrategy.OVERWRITE) {
                clearTargetTable(connection, tableName);
                details.put("targetCleared", true);
            }

            if (!request.rows().isEmpty()) {
                insertRows(connection, tableName, columns, request.rows(), batchSize);
            }

            connection.commit();
            details.put("deliveredRows", request.rows().size());

            return new ConnectorDeliveryResult(
                    DeliveryStatus.SUCCESS,
                    request.rows().size(),
                    0,
                    "已向 " + databaseLabel() + " 投递 " + request.rows().size() + " 条数据",
                    ConnectorConfigSupport.writeDetails(details)
            );
        } catch (Exception exception) {
            rollbackQuietly(connection);
            details.put("error", exception.getMessage());
            return new ConnectorDeliveryResult(
                    DeliveryStatus.FAILED,
                    0,
                    request.rows().size(),
                    databaseLabel() + " 投递失败",
                    ConnectorConfigSupport.writeDetails(details)
            );
        } finally {
            closeQuietly(connection);
        }
    }

    protected void enrichConnectionDetails(Connection connection, Map<String, Object> details) throws SQLException {
        details.put("catalog", connection.getCatalog());
        if (connection.getSchema() != null) {
            details.put("schema", connection.getSchema());
        }
    }

    private Map<String, Object> baseConnectionDetails(String jdbcUrl, String username) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("jdbcUrl", jdbcUrl);
        details.put("username", username);
        return details;
    }

    private int normalizeBatchSize(Integer batchSize) {
        if (batchSize == null || batchSize < 1) {
            return 500;
        }
        return batchSize;
    }

    private List<String> resolveColumns(Map<String, Object> runtimeConfig, List<Map<String, Object>> rows) {
        List<String> configuredColumns = ConnectorConfigSupport.optionalStringList(runtimeConfig, "target.columns", "columns");
        if (!configuredColumns.isEmpty()) {
            return configuredColumns;
        }

        LinkedHashSet<String> discoveredColumns = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            discoveredColumns.addAll(row.keySet());
        }
        return new ArrayList<>(discoveredColumns);
    }

    private void clearTargetTable(Connection connection, String tableName) throws SQLException {
        String sql = "DELETE FROM " + quoteQualifiedIdentifier(tableName);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private void insertRows(
            Connection connection,
            String tableName,
            List<String> columns,
            List<Map<String, Object>> rows,
            int batchSize
    ) throws Exception {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("JDBC 投递无法推导出有效字段列");
        }

        String sql = buildInsertSql(tableName, columns);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int currentBatchSize = 0;
            for (Map<String, Object> row : rows) {
                bindRow(preparedStatement, columns, row);
                preparedStatement.addBatch();
                currentBatchSize++;

                if (currentBatchSize >= batchSize) {
                    preparedStatement.executeBatch();
                    currentBatchSize = 0;
                }
            }

            if (currentBatchSize > 0) {
                preparedStatement.executeBatch();
            }
        }
    }

    private String buildInsertSql(String tableName, List<String> columns) {
        String quotedTable = quoteQualifiedIdentifier(tableName);
        String quotedColumns = columns.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = columns.stream()
                .map(column -> "?")
                .collect(Collectors.joining(", "));
        return "INSERT INTO " + quotedTable + " (" + quotedColumns + ") VALUES (" + placeholders + ")";
    }

    private String quoteQualifiedIdentifier(String identifier) {
        String[] segments = identifier.trim().split("\\.");
        return java.util.Arrays.stream(segments)
                .map(String::trim)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining("."));
    }

    private String quoteIdentifier(String identifier) {
        if (!identifier.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("不支持的标识符：" + identifier);
        }
        return identifierQuote() + identifier + identifierQuote();
    }

    private void bindRow(PreparedStatement preparedStatement, List<String> columns, Map<String, Object> row) throws Exception {
        for (int index = 0; index < columns.size(); index++) {
            Object value = row.get(columns.get(index));
            bindValue(preparedStatement, index + 1, value);
        }
    }

    private void bindValue(PreparedStatement preparedStatement, int parameterIndex, Object value) throws Exception {
        if (value == null) {
            preparedStatement.setObject(parameterIndex, null);
            return;
        }
        if (value instanceof String stringValue) {
            preparedStatement.setString(parameterIndex, stringValue);
            return;
        }
        if (value instanceof Integer integerValue) {
            preparedStatement.setInt(parameterIndex, integerValue);
            return;
        }
        if (value instanceof Long longValue) {
            preparedStatement.setLong(parameterIndex, longValue);
            return;
        }
        if (value instanceof Short shortValue) {
            preparedStatement.setShort(parameterIndex, shortValue);
            return;
        }
        if (value instanceof Byte byteValue) {
            preparedStatement.setByte(parameterIndex, byteValue);
            return;
        }
        if (value instanceof Float floatValue) {
            preparedStatement.setFloat(parameterIndex, floatValue);
            return;
        }
        if (value instanceof Double doubleValue) {
            preparedStatement.setDouble(parameterIndex, doubleValue);
            return;
        }
        if (value instanceof Boolean booleanValue) {
            preparedStatement.setBoolean(parameterIndex, booleanValue);
            return;
        }
        if (value instanceof java.math.BigDecimal bigDecimalValue) {
            preparedStatement.setBigDecimal(parameterIndex, bigDecimalValue);
            return;
        }
        if (value instanceof Instant instantValue) {
            preparedStatement.setTimestamp(parameterIndex, java.sql.Timestamp.from(instantValue));
            return;
        }
        if (value instanceof LocalDateTime localDateTimeValue) {
            preparedStatement.setObject(parameterIndex, localDateTimeValue);
            return;
        }
        if (value instanceof LocalDate localDateValue) {
            preparedStatement.setObject(parameterIndex, localDateValue);
            return;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            preparedStatement.setString(parameterIndex, objectMapper.writeValueAsString(value));
            return;
        }
        preparedStatement.setObject(parameterIndex, value);
    }

    private void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    private void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException ignored) {
        }
    }
}
