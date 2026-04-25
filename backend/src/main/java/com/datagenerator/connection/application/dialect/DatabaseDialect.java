package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.api.DatabaseColumnResponse;
import com.datagenerator.connection.api.DatabaseForeignKeyResponse;
import com.datagenerator.connection.api.DatabaseModelResponse;
import com.datagenerator.connection.api.DatabaseTableSchemaResponse;
import com.datagenerator.connection.api.DatabaseTableResponse;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public interface DatabaseDialect {

    DatabaseType type();

    String buildJdbcUrl(TargetConnection connection);

    Properties buildConnectionProperties(TargetConnection connection);

    String normalizeParamsForStorage(String jdbcParams);

    String defaultSchema(TargetConnection connection);

    List<DatabaseTableResponse> listTables(Connection connection, TargetConnection targetConnection) throws SQLException;

    List<DatabaseColumnResponse> listColumns(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException;

    List<DatabaseForeignKeyResponse> listForeignKeys(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException;

    DatabaseTableSchemaResponse describeTable(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException;

    DatabaseModelResponse describeModel(Connection connection, TargetConnection targetConnection, List<String> tableNames) throws SQLException;

    boolean tableExists(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException;

    Long queryMaxValue(Connection connection, TargetConnection targetConnection, String tableName, String columnName) throws SQLException;

    void createTableIfMissing(Connection connection, TargetConnection targetConnection, WriteTask task) throws SQLException;

    long countRows(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException;

    void clearTargetTable(Connection connection, TargetConnection targetConnection, String tableName) throws SQLException;

    String buildInsertSql(TargetConnection targetConnection, String tableName, List<WriteTaskColumn> columns);

    String quoteQualifiedIdentifier(TargetConnection targetConnection, String identifier);

    String quoteIdentifier(String identifier);
}
