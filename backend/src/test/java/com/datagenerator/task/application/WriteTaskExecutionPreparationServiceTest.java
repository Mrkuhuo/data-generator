package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.api.WriteTaskUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WriteTaskExecutionPreparationServiceTest {

    @Test
    void prepareForExecution_shouldAdvanceSequenceStartWhenAppendingIntoExistingTable() throws Exception {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        Connection connection = mock(Connection.class);

        when(jdbcSupport.open(any())).thenReturn(connection);
        when(jdbcSupport.dialect(DatabaseType.MYSQL)).thenReturn(dialect);
        when(dialect.tableExists(eq(connection), any(TargetConnection.class), eq("qa_orders"))).thenReturn(true);
        when(dialect.queryMaxValue(eq(connection), any(TargetConnection.class), eq("qa_orders"), eq("order_id"))).thenReturn(15L);

        WriteTaskExecutionPreparationService service = new WriteTaskExecutionPreparationService(jdbcSupport);

        WriteTask task = new WriteTask();
        task.setTableName("qa_orders");
        task.setWriteMode(WriteMode.APPEND);

        WriteTaskUpsertRequest prepared = service.prepareForExecution(task, requestWithSequenceStart(1), targetConnection());

        assertThat(prepared.columns().get(0).generatorConfig()).containsEntry("start", 16L);
    }

    @Test
    void prepareForExecution_shouldKeepSequenceStartWhenTableDoesNotExist() throws Exception {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        Connection connection = mock(Connection.class);

        when(jdbcSupport.open(any())).thenReturn(connection);
        when(jdbcSupport.dialect(DatabaseType.MYSQL)).thenReturn(dialect);
        when(dialect.tableExists(eq(connection), any(TargetConnection.class), eq("qa_orders"))).thenReturn(false);

        WriteTaskExecutionPreparationService service = new WriteTaskExecutionPreparationService(jdbcSupport);

        WriteTask task = new WriteTask();
        task.setTableName("qa_orders");
        task.setWriteMode(WriteMode.APPEND);

        WriteTaskUpsertRequest prepared = service.prepareForExecution(task, requestWithSequenceStart(1), targetConnection());

        assertThat(prepared.columns().get(0).generatorConfig()).containsEntry("start", 1L);
    }

    @Test
    void prepareForExecution_shouldKeepGreaterConfiguredStartWhenDatabaseMaxIsLower() throws Exception {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        Connection connection = mock(Connection.class);

        when(jdbcSupport.open(any())).thenReturn(connection);
        when(jdbcSupport.dialect(DatabaseType.MYSQL)).thenReturn(dialect);
        when(dialect.tableExists(eq(connection), any(TargetConnection.class), eq("qa_orders"))).thenReturn(true);
        when(dialect.queryMaxValue(eq(connection), any(TargetConnection.class), eq("qa_orders"), eq("order_id"))).thenReturn(4L);

        WriteTaskExecutionPreparationService service = new WriteTaskExecutionPreparationService(jdbcSupport);

        WriteTask task = new WriteTask();
        task.setTableName("qa_orders");
        task.setWriteMode(WriteMode.APPEND);

        WriteTaskUpsertRequest prepared = service.prepareForExecution(task, requestWithSequenceStart(10), targetConnection());

        assertThat(prepared.columns().get(0).generatorConfig()).containsEntry("start", 10L);
    }

    @Test
    void prepareForExecution_shouldUseDialectForQualifiedSqlServerTableName() throws Exception {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        Connection connection = mock(Connection.class);

        TargetConnection connectionDefinition = new TargetConnection();
        connectionDefinition.setDbType(DatabaseType.SQLSERVER);
        connectionDefinition.setDatabaseName("sales");
        connectionDefinition.setSchemaName("dbo");

        when(jdbcSupport.open(any())).thenReturn(connection);
        when(jdbcSupport.dialect(DatabaseType.SQLSERVER)).thenReturn(dialect);
        when(dialect.tableExists(eq(connection), eq(connectionDefinition), eq("sales.customers"))).thenReturn(true);
        when(dialect.queryMaxValue(eq(connection), eq(connectionDefinition), eq("sales.customers"), eq("customer_id"))).thenReturn(99L);

        WriteTaskExecutionPreparationService service = new WriteTaskExecutionPreparationService(jdbcSupport);

        WriteTask task = new WriteTask();
        task.setTableName("sales.customers");
        task.setWriteMode(WriteMode.APPEND);

        WriteTaskUpsertRequest prepared = service.prepareForExecution(
                task,
                requestWithCustomColumn("sales.customers", "customer_id", 1),
                connectionDefinition
        );

        assertThat(prepared.columns().get(0).generatorConfig()).containsEntry("start", 100L);
    }

    @Test
    void prepareForExecution_shouldBypassJdbcPreparationForKafka() {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        WriteTaskExecutionPreparationService service = new WriteTaskExecutionPreparationService(jdbcSupport);

        WriteTask task = new WriteTask();
        task.setTableName("demo.topic");
        task.setWriteMode(WriteMode.APPEND);

        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.KAFKA);

        WriteTaskUpsertRequest request = requestWithCustomColumn("demo.topic", "order_id", 1);
        WriteTaskUpsertRequest prepared = service.prepareForExecution(task, request, connection);

        assertThat(prepared).isEqualTo(request);
        verifyNoInteractions(jdbcSupport);
    }

    private TargetConnection targetConnection() {
        TargetConnection targetConnection = new TargetConnection();
        targetConnection.setDbType(DatabaseType.MYSQL);
        return targetConnection;
    }

    private WriteTaskUpsertRequest requestWithSequenceStart(long start) {
        return requestWithCustomColumn("qa_orders", "order_id", start);
    }

    private WriteTaskUpsertRequest requestWithCustomColumn(String tableName, String columnName, long start) {
        return new WriteTaskUpsertRequest(
                "qa",
                1L,
                tableName,
                TableMode.USE_EXISTING,
                WriteMode.APPEND,
                5,
                100,
                1L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                List.of(
                        new WriteTaskColumnUpsertRequest(
                                columnName,
                                "BIGINT",
                                null,
                                null,
                                null,
                                false,
                                true,
                                ColumnGeneratorType.SEQUENCE,
                                Map.of("start", start, "step", 1),
                                0
                        )
                )
        );
    }
}
