package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.application.ConnectionJdbcSupport;
import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class WriteTaskJdbcWriterTest {

    @Test
    void write_shouldCreateTableBeforeCountingWhenCreateIfMissing() throws Exception {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        TargetConnection targetConnection = new TargetConnection();
        targetConnection.setDbType(DatabaseType.POSTGRESQL);
        targetConnection.setDatabaseName("postgres");
        targetConnection.setSchemaName("public");

        WriteTaskColumn column = new WriteTaskColumn();
        column.setColumnName("id");
        column.setDbType("BIGINT");
        column.setNullableFlag(false);
        column.setPrimaryKeyFlag(true);
        column.setGeneratorType(ColumnGeneratorType.SEQUENCE);
        column.setGeneratorConfigJson("{\"start\":1,\"step\":1}");
        column.setSortOrder(0);

        WriteTask task = new WriteTask();
        task.setTableName("public.smoke_orders");
        task.setTableMode(TableMode.CREATE_IF_MISSING);
        task.setWriteMode(WriteMode.APPEND);
        task.setBatchSize(50);
        task.setColumns(List.of(column));

        when(jdbcSupport.open(any())).thenReturn(connection);
        when(jdbcSupport.dialect(DatabaseType.POSTGRESQL)).thenReturn(dialect);
        when(dialect.countRows(connection, targetConnection, "public.smoke_orders")).thenReturn(0L, 1L);
        when(dialect.buildInsertSql(targetConnection, "public.smoke_orders", task.getColumns()))
                .thenReturn("INSERT INTO smoke_orders (id) VALUES (?)");
        when(connection.prepareStatement("INSERT INTO smoke_orders (id) VALUES (?)")).thenReturn(statement);

        WriteTaskJdbcWriter writer = new WriteTaskJdbcWriter(jdbcSupport, new ObjectMapper());
        WriteTaskDeliveryResult result = writer.write(
                task,
                targetConnection,
                List.of(Map.of("id", 1L))
        );

        InOrder inOrder = inOrder(dialect);
        inOrder.verify(dialect).createTableIfMissing(connection, targetConnection, task);
        inOrder.verify(dialect).countRows(connection, targetConnection, "public.smoke_orders");

        verify(statement).setLong(1, 1L);
        verify(connection).commit();
        assertThat(result.successCount()).isEqualTo(1L);
        assertThat(result.details()).containsEntry("beforeWriteRowCount", 0L);
        assertThat(result.details()).containsEntry("afterWriteRowCount", 1L);
        assertThat(result.details()).containsEntry("rowDelta", 1L);
    }

    @Test
    void write_shouldUseActualPostgresqlJsonbTypeFromExistingTableWhenStoredTaskTypeIsOutdated() throws Exception {
        ConnectionJdbcSupport jdbcSupport = mock(ConnectionJdbcSupport.class);
        DatabaseDialect dialect = mock(DatabaseDialect.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        TargetConnection targetConnection = new TargetConnection();
        targetConnection.setDbType(DatabaseType.POSTGRESQL);
        targetConnection.setDatabaseName("demo_sink");
        targetConnection.setSchemaName("public");

        WriteTaskColumn jsonColumn = new WriteTaskColumn();
        jsonColumn.setColumnName("profile");
        jsonColumn.setDbType("VARCHAR");
        jsonColumn.setNullableFlag(true);
        jsonColumn.setPrimaryKeyFlag(false);
        jsonColumn.setGeneratorType(ColumnGeneratorType.STRING);
        jsonColumn.setGeneratorConfigJson("{\"mode\":\"random\"}");
        jsonColumn.setSortOrder(0);

        WriteTask task = new WriteTask();
        task.setTableName("public.synthetic_user_activity");
        task.setTableMode(TableMode.USE_EXISTING);
        task.setWriteMode(WriteMode.APPEND);
        task.setBatchSize(50);
        task.setColumns(List.of(jsonColumn));

        when(jdbcSupport.open(any())).thenReturn(connection);
        when(jdbcSupport.dialect(DatabaseType.POSTGRESQL)).thenReturn(dialect);
        when(dialect.countRows(connection, targetConnection, "public.synthetic_user_activity")).thenReturn(0L, 1L);
        when(dialect.listColumns(connection, targetConnection, "public.synthetic_user_activity")).thenReturn(List.of(
                new DatabaseColumnResponse("profile", "JSONB", null, null, null, true, false, false, null)
        ));
        when(dialect.buildInsertSql(eq(targetConnection), eq("public.synthetic_user_activity"), any()))
                .thenAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    List<WriteTaskColumn> columns = invocation.getArgument(2);
                    assertThat(columns).singleElement().extracting(WriteTaskColumn::getDbType).isEqualTo("JSONB");
                    return "INSERT INTO synthetic_user_activity (profile) VALUES (CAST(? AS JSONB))";
                });
        when(connection.prepareStatement("INSERT INTO synthetic_user_activity (profile) VALUES (CAST(? AS JSONB))"))
                .thenReturn(statement);

        WriteTaskJdbcWriter writer = new WriteTaskJdbcWriter(jdbcSupport, new ObjectMapper());
        writer.write(task, targetConnection, List.of(Map.of("profile", "plain-text")));

        verify(statement).setObject(1, "\"plain-text\"", Types.OTHER);
        verify(connection).commit();
    }
}
