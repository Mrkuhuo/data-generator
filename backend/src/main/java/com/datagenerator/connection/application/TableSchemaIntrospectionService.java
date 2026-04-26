package com.datagenerator.connection.application;

import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.api.DatabaseModelResponse;
import com.datagenerator.connection.api.DatabaseTableResponse;
import com.datagenerator.connection.api.DatabaseTableSchemaResponse;
import com.datagenerator.connection.application.dialect.DatabaseDialect;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import java.sql.Connection;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class TableSchemaIntrospectionService {

    private final ConnectionJdbcSupport jdbcSupport;

    public TableSchemaIntrospectionService(ConnectionJdbcSupport jdbcSupport) {
        this.jdbcSupport = jdbcSupport;
    }

    public List<DatabaseTableResponse> listTables(TargetConnection connection) {
        if (connection.getDbType() == DatabaseType.KAFKA) {
            throw new IllegalArgumentException("Kafka 连接不支持读取数据表列表");
        }

        try (Connection jdbcConnection = jdbcSupport.open(connection)) {
            DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
            return dialect.listTables(jdbcConnection, connection);
        } catch (Exception exception) {
            throw new IllegalArgumentException("读取数据表失败: " + exception.getMessage(), exception);
        }
    }

    public List<DatabaseColumnResponse> listColumns(TargetConnection connection, String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName 不能为空");
        }
        if (connection.getDbType() == DatabaseType.KAFKA) {
            throw new IllegalArgumentException("Kafka 连接不支持读取表字段结构");
        }

        try (Connection jdbcConnection = jdbcSupport.open(connection)) {
            DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
            return dialect.listColumns(jdbcConnection, connection, tableName);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("读取表字段失败: " + exception.getMessage(), exception);
        }
    }

    public DatabaseTableSchemaResponse describeTable(TargetConnection connection, String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName 不能为空");
        }
        if (connection.getDbType() == DatabaseType.KAFKA) {
            throw new IllegalArgumentException("Kafka 连接不支持读取表结构");
        }

        try (Connection jdbcConnection = jdbcSupport.open(connection)) {
            DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
            return dialect.describeTable(jdbcConnection, connection, tableName);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("读取表结构失败: " + exception.getMessage(), exception);
        }
    }

    public DatabaseModelResponse describeModel(TargetConnection connection, List<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) {
            throw new IllegalArgumentException("tableNames 不能为空");
        }
        if (connection.getDbType() == DatabaseType.KAFKA) {
            throw new IllegalArgumentException("Kafka 连接不支持读取多表结构");
        }

        try (Connection jdbcConnection = jdbcSupport.open(connection)) {
            DatabaseDialect dialect = jdbcSupport.dialect(connection.getDbType());
            return dialect.describeModel(jdbcConnection, connection, tableNames);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("读取多表模型失败: " + exception.getMessage(), exception);
        }
    }
}
