package com.datagenerator.connection.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.datagenerator.connection.application.dialect.DatabaseDialectRegistry;
import com.datagenerator.connection.application.dialect.MysqlDatabaseDialect;
import com.datagenerator.connection.application.dialect.OracleDatabaseDialect;
import com.datagenerator.connection.application.dialect.PostgresqlDatabaseDialect;
import com.datagenerator.connection.application.dialect.SqlServerDatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ConnectionJdbcSupportTest {

    private final TargetConnectionSecretCodec secretCodec = new TargetConnectionSecretCodec("test-secret-key");
    private final ConnectionJdbcSupport support = new ConnectionJdbcSupport(new DatabaseDialectRegistry(List.of(
            new MysqlDatabaseDialect(),
            new PostgresqlDatabaseDialect(),
            new SqlServerDatabaseDialect(),
            new OracleDatabaseDialect()
    )), secretCodec);

    @Test
    void normalizeParamsForStorage_shouldProvideUtf8DefaultsForMysql() {
        assertThat(support.normalizeParamsForStorage(DatabaseType.MYSQL, null))
                .isEqualTo("useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai");
    }

    @Test
    void normalizeParamsForStorage_shouldUpgradeLegacyUtf8Encoding() {
        String params = support.normalizeParamsForStorage(
                DatabaseType.MYSQL,
                "connectTimeout=3000&characterEncoding=utf8"
        );

        assertThat(params).contains("connectTimeout=3000");
        assertThat(params).contains("characterEncoding=UTF-8");
        assertThat(params).contains("useUnicode=true");
        assertThat(params).contains("serverTimezone=Asia/Shanghai");
    }

    @Test
    void normalizeParamsForStorage_shouldProvideSqlServerDefaults() {
        assertThat(support.normalizeParamsForStorage(DatabaseType.SQLSERVER, null))
                .isEqualTo("encrypt=true;trustServerCertificate=true");
    }

    @Test
    void buildJdbcUrl_shouldUseBaseMysqlUrlAndConnectionProperties() {
        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.MYSQL);
        connection.setHost("127.0.0.1");
        connection.setPort(3306);
        connection.setDatabaseName("demo_db");
        connection.setUsername("root");
        connection.setPasswordValue("123456");
        connection.setJdbcParams(support.normalizeParamsForStorage(
                DatabaseType.MYSQL,
                "useUnicode=true&characterEncoding=utf8"
        ));

        assertThat(support.buildJdbcUrl(connection))
                .isEqualTo("jdbc:mysql://127.0.0.1:3306/demo_db");

        Properties properties = support.dialect(DatabaseType.MYSQL).buildConnectionProperties(connection);
        assertThat(properties.getProperty("user")).isEqualTo("root");
        assertThat(properties.getProperty("password")).isEqualTo("123456");
        assertThat(properties.getProperty("characterEncoding")).isEqualTo("UTF-8");
        assertThat(properties.getProperty("serverTimezone")).isEqualTo("Asia/Shanghai");
    }

    @Test
    void buildJdbcUrl_shouldUseSqlServerFormat() {
        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.SQLSERVER);
        connection.setHost("192.168.1.10");
        connection.setPort(1433);
        connection.setDatabaseName("sales");

        assertThat(support.buildJdbcUrl(connection))
                .isEqualTo("jdbc:sqlserver://192.168.1.10:1433;databaseName=sales");
    }

    @Test
    void buildJdbcUrl_shouldUseOracleFormat() {
        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.ORACLE);
        connection.setHost("10.0.0.5");
        connection.setPort(1521);
        connection.setDatabaseName("xe");

        assertThat(support.buildJdbcUrl(connection))
                .isEqualTo("jdbc:oracle:thin:@//10.0.0.5:1521/xe");
    }

    @Test
    void defaultSchema_shouldFallbackPerDatabaseType() {
        TargetConnection postgresql = new TargetConnection();
        postgresql.setDbType(DatabaseType.POSTGRESQL);

        TargetConnection sqlServer = new TargetConnection();
        sqlServer.setDbType(DatabaseType.SQLSERVER);

        TargetConnection oracle = new TargetConnection();
        oracle.setDbType(DatabaseType.ORACLE);
        oracle.setUsername("app_user");

        assertThat(support.defaultSchema(postgresql)).isEqualTo("public");
        assertThat(support.defaultSchema(sqlServer)).isEqualTo("dbo");
        assertThat(support.defaultSchema(oracle)).isEqualTo("APP_USER");
    }

    @Test
    void defaultSchema_shouldUseConfiguredOracleSchemaInUpperCase() {
        TargetConnection connection = new TargetConnection();
        connection.setDbType(DatabaseType.ORACLE);
        connection.setUsername("app_user");
        connection.setSchemaName("reporting");

        assertThat(support.defaultSchema(connection)).isEqualTo("REPORTING");
    }
}
