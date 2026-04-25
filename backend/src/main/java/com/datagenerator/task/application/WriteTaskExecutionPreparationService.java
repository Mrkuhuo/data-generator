package com.datagenerator.task.application;

import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class WriteTaskExecutionPreparationService {

    private final ConnectionJdbcSupport jdbcSupport;

    public WriteTaskExecutionPreparationService(ConnectionJdbcSupport jdbcSupport) {
        this.jdbcSupport = jdbcSupport;
    }

    public WriteTaskUpsertRequest prepareForExecution(
            WriteTask task,
            WriteTaskUpsertRequest baseRequest,
            TargetConnection connection
    ) {
        if (connection.getDbType() == DatabaseType.KAFKA) {
            return baseRequest;
        }

        if (task.getWriteMode() != WriteMode.APPEND) {
            return baseRequest;
        }

        List<WriteTaskColumnUpsertRequest> sequenceColumns = baseRequest.columns().stream()
                .filter(column -> column.generatorType() == ColumnGeneratorType.SEQUENCE)
                .toList();
        if (sequenceColumns.isEmpty()) {
            return baseRequest;
        }

        Map<String, Long> adjustedStarts = resolveAdjustedSequenceStarts(task, sequenceColumns, connection);
        if (adjustedStarts.isEmpty()) {
            return baseRequest;
        }

        List<WriteTaskColumnUpsertRequest> adjustedColumns = baseRequest.columns().stream()
                .map(column -> adjustColumn(column, adjustedStarts))
                .toList();

        return new WriteTaskUpsertRequest(
                baseRequest.name(),
                baseRequest.connectionId(),
                baseRequest.tableName(),
                baseRequest.tableMode(),
                baseRequest.writeMode(),
                baseRequest.rowCount(),
                baseRequest.batchSize(),
                baseRequest.seed(),
                baseRequest.status(),
                baseRequest.scheduleType(),
                baseRequest.cronExpression(),
                baseRequest.triggerAt(),
                baseRequest.intervalSeconds(),
                baseRequest.maxRuns(),
                baseRequest.maxRowsTotal(),
                baseRequest.description(),
                baseRequest.targetConfigJson(),
                baseRequest.payloadSchemaJson(),
                adjustedColumns
        );
    }

    private WriteTaskColumnUpsertRequest adjustColumn(
            WriteTaskColumnUpsertRequest column,
            Map<String, Long> adjustedStarts
    ) {
        Long nextStart = adjustedStarts.get(column.columnName());
        if (nextStart == null) {
            return column;
        }

        Map<String, Object> generatorConfig = new LinkedHashMap<>(column.generatorConfig() == null ? Map.of() : column.generatorConfig());
        generatorConfig.put("start", nextStart);

        return new WriteTaskColumnUpsertRequest(
                column.columnName(),
                column.dbType(),
                column.lengthValue(),
                column.precisionValue(),
                column.scaleValue(),
                column.nullableFlag(),
                column.primaryKeyFlag(),
                column.generatorType(),
                generatorConfig,
                column.sortOrder()
        );
    }

    private Map<String, Long> resolveAdjustedSequenceStarts(
            WriteTask task,
            List<WriteTaskColumnUpsertRequest> sequenceColumns,
            TargetConnection connection
    ) {
        try (Connection jdbcConnection = jdbcSupport.open(connection)) {
            DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
            if (!dialect.tableExists(jdbcConnection, connection, task.getTableName())) {
                return Map.of();
            }

            LinkedHashMap<String, Long> adjustedStarts = new LinkedHashMap<>();
            for (WriteTaskColumnUpsertRequest column : sequenceColumns) {
                Long currentMax = dialect.queryMaxValue(jdbcConnection, connection, task.getTableName(), column.columnName());
                if (currentMax == null) {
                    continue;
                }

                long step = asLong(column.generatorConfig() == null ? null : column.generatorConfig().get("step"), 1L);
                long configuredStart = asLong(column.generatorConfig() == null ? null : column.generatorConfig().get("start"), 1L);
                long nextStart = Math.max(configuredStart, currentMax + step);
                if (nextStart != configuredStart) {
                    adjustedStarts.put(column.columnName(), nextStart);
                }
            }
            return adjustedStarts;
        } catch (Exception exception) {
            throw new IllegalArgumentException("读取目标表现有序列值失败: " + exception.getMessage(), exception);
        }
    }

    private long asLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = value.toString().trim();
        if (text.isEmpty()) {
            return fallback;
        }
        return Long.parseLong(text);
    }
}
