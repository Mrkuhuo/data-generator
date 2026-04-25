package com.datagenerator.connection.application.dialect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datagenerator.connection.domain.TargetConnection;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.util.List;
import org.junit.jupiter.api.Test;

class MysqlDatabaseDialectTest {

    private final MysqlDatabaseDialect dialect = new MysqlDatabaseDialect();

    @Test
    void renderColumnType_shouldApplyDefaultLengthForVarchar() {
        WriteTaskColumn column = new WriteTaskColumn();
        column.setColumnName("name");
        column.setDbType("VARCHAR");

        assertThat(dialect.renderColumnType(column)).isEqualTo("VARCHAR(255)");
    }

    @Test
    void renderColumnType_shouldApplyDefaultLengthForCharAndBinaryTypes() {
        WriteTaskColumn charColumn = new WriteTaskColumn();
        charColumn.setColumnName("flag");
        charColumn.setDbType("CHAR");

        WriteTaskColumn binaryColumn = new WriteTaskColumn();
        binaryColumn.setColumnName("payload");
        binaryColumn.setDbType("VARBINARY");

        assertThat(dialect.renderColumnType(charColumn)).isEqualTo("CHAR(1)");
        assertThat(dialect.renderColumnType(binaryColumn)).isEqualTo("VARBINARY(255)");
    }

    @Test
    void listForeignKeys_shouldPreferCatalogWhenSchemaIsMissing() throws Exception {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet resultSet = mock(ResultSet.class);
        TargetConnection targetConnection = new TargetConnection();
        targetConnection.setDatabaseName("mysql_test");

        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getImportedKeys(any(), any(), anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("FK_NAME")).thenReturn("fk_orders_customer");
        when(resultSet.getString("PKTABLE_SCHEM")).thenReturn(null);
        when(resultSet.getString("PKTABLE_CAT")).thenReturn("mysql_test");
        when(resultSet.getString("PKTABLE_NAME")).thenReturn("orders");
        when(resultSet.getString("FKTABLE_SCHEM")).thenReturn(null);
        when(resultSet.getString("FKTABLE_CAT")).thenReturn("mysql_test");
        when(resultSet.getString("FKTABLE_NAME")).thenReturn("order_items");
        when(resultSet.getString("PKCOLUMN_NAME")).thenReturn("id");
        when(resultSet.getString("FKCOLUMN_NAME")).thenReturn("order_id");

        var foreignKeys = dialect.listForeignKeys(connection, targetConnection, "mysql_test.order_items");

        assertThat(foreignKeys).hasSize(1);
        assertThat(foreignKeys.get(0).parentTable()).isEqualTo("mysql_test.orders");
        assertThat(foreignKeys.get(0).childTable()).isEqualTo("mysql_test.order_items");
        assertThat(foreignKeys.get(0).parentColumns()).isEqualTo(List.of("id"));
        assertThat(foreignKeys.get(0).childColumns()).isEqualTo(List.of("order_id"));
    }

    @Test
    void listColumns_shouldIncludeEnumValuesFromInformationSchema() throws Exception {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet primaryKeys = mock(ResultSet.class);
        ResultSet columns = mock(ResultSet.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet enumColumns = mock(ResultSet.class);
        TargetConnection targetConnection = new TargetConnection();
        targetConnection.setDatabaseName("mysql_test");

        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getPrimaryKeys(any(), any(), anyString())).thenReturn(primaryKeys);
        when(primaryKeys.next()).thenReturn(false);
        when(metaData.getColumns(any(), any(), anyString(), anyString())).thenReturn(columns);
        when(columns.next()).thenReturn(true, false);
        when(columns.getString("TYPE_NAME")).thenReturn("ENUM");
        when(columns.getInt("COLUMN_SIZE")).thenReturn(10);
        when(columns.getInt("DECIMAL_DIGITS")).thenReturn(0);
        when(columns.wasNull()).thenReturn(false, true);
        when(columns.getString("COLUMN_NAME")).thenReturn("status");
        when(columns.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNullable);
        when(columns.getString("IS_AUTOINCREMENT")).thenReturn("NO");

        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(enumColumns);
        when(enumColumns.next()).thenReturn(true, false);
        when(enumColumns.getString("COLUMN_NAME")).thenReturn("status");
        when(enumColumns.getString("COLUMN_TYPE")).thenReturn("enum('pending','processing','completed','cancelled')");

        var columnsResult = dialect.listColumns(connection, targetConnection, "mysql_test.orders");

        assertThat(columnsResult).hasSize(1);
        assertThat(columnsResult.get(0).dbType()).isEqualTo("ENUM");
        assertThat(columnsResult.get(0).enumValues()).containsExactly("pending", "processing", "completed", "cancelled");
    }
}
