package com.datagenerator.connection.application.dialect;

import static org.assertj.core.api.Assertions.assertThat;

import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.util.List;
import org.junit.jupiter.api.Test;

class PostgresqlDatabaseDialectTest {

    private final PostgresqlDatabaseDialect dialect = new PostgresqlDatabaseDialect();

    @Test
    void buildInsertSql_shouldCastJsonAndJsonbColumns() {
        TargetConnection connection = new TargetConnection();
        connection.setDatabaseName("demo_sink");
        connection.setSchemaName("public");

        WriteTaskColumn jsonColumn = new WriteTaskColumn();
        jsonColumn.setColumnName("profile");
        jsonColumn.setDbType("JSONB");

        WriteTaskColumn textColumn = new WriteTaskColumn();
        textColumn.setColumnName("email");
        textColumn.setDbType("VARCHAR");

        String sql = dialect.buildInsertSql(connection, "public.synthetic_user_activity", List.of(jsonColumn, textColumn));

        assertThat(sql).isEqualTo(
                "INSERT INTO \"demo_sink\".\"public\".\"synthetic_user_activity\" (\"profile\", \"email\") "
                        + "VALUES (CAST(? AS JSONB), ?)"
        );
    }
}
