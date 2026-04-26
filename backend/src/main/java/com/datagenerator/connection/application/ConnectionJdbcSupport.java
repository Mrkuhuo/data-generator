package com.datagenerator.connection.application;

import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.application.dialect.DatabaseDialectRegistry;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import org.springframework.stereotype.Component;

@Component
public class ConnectionJdbcSupport {

    private final DatabaseDialectRegistry dialectRegistry;
    private final TargetConnectionSecretCodec secretCodec;

    public ConnectionJdbcSupport(DatabaseDialectRegistry dialectRegistry, TargetConnectionSecretCodec secretCodec) {
        this.dialectRegistry = dialectRegistry;
        this.secretCodec = secretCodec;
    }

    public Connection open(TargetConnection connection) throws Exception {
        rejectKafka(connection.getDbType(), "Kafka 连接不支持 JDBC 操作");
        DatabaseDialect dialect = dialect(connection.getDbType());
        TargetConnection runtimeConnection = withDecryptedPassword(connection);
        return DriverManager.getConnection(
                dialect.buildJdbcUrl(runtimeConnection),
                dialect.buildConnectionProperties(runtimeConnection)
        );
    }

    public String buildJdbcUrl(TargetConnection connection) {
        rejectKafka(connection.getDbType(), "Kafka 连接不支持 JDBC URL");
        return dialect(connection.getDbType()).buildJdbcUrl(connection);
    }

    public String normalizeParamsForStorage(DatabaseType dbType, String jdbcParams) {
        if (dbType == DatabaseType.KAFKA) {
            return null;
        }
        return dialect(dbType).normalizeParamsForStorage(jdbcParams);
    }

    public String defaultSchema(TargetConnection connection) {
        rejectKafka(connection.getDbType(), "Kafka 连接没有默认 Schema");
        return dialect(connection.getDbType()).defaultSchema(connection);
    }

    public DatabaseDialect dialect(DatabaseType dbType) {
        rejectKafka(dbType, "Kafka 连接不存在 JDBC 方言");
        return dialectRegistry.get(dbType);
    }

    private void rejectKafka(DatabaseType dbType, String message) {
        if (dbType == DatabaseType.KAFKA) {
            throw new IllegalArgumentException(message);
        }
    }

    private TargetConnection withDecryptedPassword(TargetConnection connection) {
        TargetConnection runtimeConnection = new TargetConnection();
        runtimeConnection.setId(connection.getId());
        runtimeConnection.setName(connection.getName());
        runtimeConnection.setDbType(connection.getDbType());
        runtimeConnection.setHost(connection.getHost());
        runtimeConnection.setPort(connection.getPort());
        runtimeConnection.setDatabaseName(connection.getDatabaseName());
        runtimeConnection.setSchemaName(connection.getSchemaName());
        runtimeConnection.setUsername(connection.getUsername());
        runtimeConnection.setPasswordValue(secretCodec.decryptForUse(connection.getPasswordValue()));
        runtimeConnection.setJdbcParams(connection.getJdbcParams());
        runtimeConnection.setConfigJson(connection.getConfigJson());
        runtimeConnection.setStatus(connection.getStatus());
        return runtimeConnection;
    }
}
