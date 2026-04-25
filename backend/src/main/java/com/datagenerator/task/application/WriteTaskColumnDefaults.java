package com.datagenerator.task.application;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.task.api.WriteTaskColumnUpsertRequest;
import com.datagenerator.task.domain.ColumnGeneratorType;
import java.util.Locale;
import java.util.Set;

final class WriteTaskColumnDefaults {

    private static final Set<String> MYSQL_TYPES = Set.of(
            "BIGINT", "INT", "DECIMAL", "VARCHAR", "CHAR", "TEXT", "BOOLEAN", "DATE", "DATETIME", "TIMESTAMP", "UUID",
            "NVARCHAR", "VARBINARY", "BINARY"
    );

    private static final Set<String> POSTGRESQL_TYPES = Set.of(
            "BIGINT", "INTEGER", "NUMERIC", "VARCHAR", "TEXT", "BOOLEAN", "DATE", "TIMESTAMP", "UUID", "JSON", "JSONB"
    );

    private static final Set<String> SQLSERVER_TYPES = Set.of(
            "BIGINT", "INT", "DECIMAL", "NVARCHAR", "NCHAR", "BIT", "DATE", "DATETIME2", "UNIQUEIDENTIFIER"
    );

    private static final Set<String> ORACLE_TYPES = Set.of(
            "BIGINT", "INT", "NUMBER", "VARCHAR2", "NVARCHAR2", "CHAR", "CLOB", "BOOLEAN", "DATE", "TIMESTAMP", "UUID"
    );

    private static final Set<String> KAFKA_TYPES = Set.of(
            "BIGINT", "INT", "DECIMAL", "VARCHAR", "TEXT", "BOOLEAN", "DATETIME", "UUID"
    );

    private WriteTaskColumnDefaults() {
    }

    static WriteTaskColumnUpsertRequest normalize(DatabaseType databaseType, WriteTaskColumnUpsertRequest request) {
        String dbType = resolveDbType(databaseType, request);
        Integer precisionValue = normalizePrecision(dbType, request.precisionValue(), request.generatorType(), request.scaleValue());
        Integer scaleValue = normalizeScale(dbType, request.scaleValue(), request.generatorType());
        if (precisionValue == null && scaleValue != null) {
            precisionValue = 18;
        }

        return new WriteTaskColumnUpsertRequest(
                request.columnName(),
                dbType,
                normalizeLength(dbType, request.lengthValue()),
                precisionValue,
                scaleValue,
                request.nullableFlag(),
                request.primaryKeyFlag(),
                request.generatorType(),
                request.generatorConfig(),
                request.sortOrder()
        );
    }

    private static String resolveDbType(DatabaseType databaseType, WriteTaskColumnUpsertRequest request) {
        String normalizedType = normalizeType(request.dbType());
        if (!normalizedType.isBlank()) {
            String aliasedType = normalizeAlias(databaseType, normalizedType);
            if (isSupported(databaseType, aliasedType)) {
                return aliasedType;
            }
        }
        return preferredType(databaseType, request.generatorType(), request.primaryKeyFlag(), request.scaleValue());
    }

    private static String normalizeAlias(DatabaseType databaseType, String dbType) {
        return switch (databaseType) {
            case MYSQL, KAFKA -> switch (dbType) {
                case "STRING" -> "VARCHAR";
                case "INTEGER" -> "INT";
                case "NUMERIC", "NUMBER" -> "DECIMAL";
                case "BIT", "BOOL" -> "BOOLEAN";
                case "DATETIME2" -> "DATETIME";
                case "UNIQUEIDENTIFIER" -> "UUID";
                case "VARCHAR2", "NVARCHAR2" -> "VARCHAR";
                case "CLOB" -> "TEXT";
                case "NCHAR" -> "CHAR";
                default -> dbType;
            };
            case POSTGRESQL -> switch (dbType) {
                case "INT" -> "INTEGER";
                case "INT8" -> "BIGINT";
                case "INT4" -> "INTEGER";
                case "DECIMAL", "NUMBER" -> "NUMERIC";
                case "BIT", "BOOL" -> "BOOLEAN";
                case "DATETIME", "DATETIME2" -> "TIMESTAMP";
                case "UNIQUEIDENTIFIER" -> "UUID";
                case "VARCHAR2", "NVARCHAR", "NVARCHAR2" -> "VARCHAR";
                case "CLOB" -> "TEXT";
                default -> dbType;
            };
            case SQLSERVER -> switch (dbType) {
                case "INTEGER" -> "INT";
                case "NUMERIC", "NUMBER" -> "DECIMAL";
                case "VARCHAR", "VARCHAR2", "NVARCHAR2", "TEXT", "CLOB" -> "NVARCHAR";
                case "CHAR" -> "NCHAR";
                case "BOOL", "BOOLEAN" -> "BIT";
                case "DATETIME", "TIMESTAMP" -> "DATETIME2";
                case "UUID" -> "UNIQUEIDENTIFIER";
                default -> dbType;
            };
            case ORACLE -> switch (dbType) {
                case "INTEGER" -> "INT";
                case "DECIMAL", "NUMERIC" -> "NUMBER";
                case "VARCHAR" -> "VARCHAR2";
                case "NVARCHAR" -> "NVARCHAR2";
                case "TEXT" -> "CLOB";
                case "BIT", "BOOL" -> "BOOLEAN";
                case "DATETIME", "DATETIME2" -> "TIMESTAMP";
                case "UNIQUEIDENTIFIER" -> "UUID";
                default -> dbType;
            };
        };
    }

    private static boolean isSupported(DatabaseType databaseType, String dbType) {
        return switch (databaseType) {
            case MYSQL -> MYSQL_TYPES.contains(dbType);
            case POSTGRESQL -> POSTGRESQL_TYPES.contains(dbType);
            case SQLSERVER -> SQLSERVER_TYPES.contains(dbType);
            case ORACLE -> ORACLE_TYPES.contains(dbType);
            case KAFKA -> KAFKA_TYPES.contains(dbType);
        };
    }

    private static String preferredType(
            DatabaseType databaseType,
            ColumnGeneratorType generatorType,
            boolean primaryKeyFlag,
            Integer scaleValue
    ) {
        if (primaryKeyFlag || generatorType == ColumnGeneratorType.SEQUENCE) {
            return switch (databaseType) {
                case POSTGRESQL, MYSQL, SQLSERVER, ORACLE, KAFKA -> "BIGINT";
            };
        }
        if (generatorType == ColumnGeneratorType.UUID) {
            return switch (databaseType) {
                case SQLSERVER -> "UNIQUEIDENTIFIER";
                case POSTGRESQL, MYSQL, ORACLE, KAFKA -> "UUID";
            };
        }
        if (generatorType == ColumnGeneratorType.BOOLEAN) {
            return switch (databaseType) {
                case SQLSERVER -> "BIT";
                case POSTGRESQL, MYSQL, ORACLE, KAFKA -> "BOOLEAN";
            };
        }
        if (generatorType == ColumnGeneratorType.DATETIME) {
            return switch (databaseType) {
                case POSTGRESQL, ORACLE -> "TIMESTAMP";
                case SQLSERVER -> "DATETIME2";
                case MYSQL, KAFKA -> "DATETIME";
            };
        }
        if (generatorType == ColumnGeneratorType.RANDOM_DECIMAL || (scaleValue != null && scaleValue > 0)) {
            return switch (databaseType) {
                case POSTGRESQL -> "NUMERIC";
                case ORACLE -> "NUMBER";
                case MYSQL, SQLSERVER, KAFKA -> "DECIMAL";
            };
        }
        if (generatorType == ColumnGeneratorType.RANDOM_INT) {
            return switch (databaseType) {
                case POSTGRESQL -> "INTEGER";
                case MYSQL, SQLSERVER, ORACLE, KAFKA -> "INT";
            };
        }
        return switch (databaseType) {
            case SQLSERVER -> "NVARCHAR";
            case ORACLE -> "VARCHAR2";
            case POSTGRESQL, MYSQL, KAFKA -> "VARCHAR";
        };
    }

    private static Integer normalizeLength(String dbType, Integer value) {
        Integer normalizedValue = positive(value);
        if (normalizedValue != null) {
            return normalizedValue;
        }
        return switch (dbType) {
            case "VARCHAR", "NVARCHAR", "VARCHAR2", "NVARCHAR2" -> 255;
            case "CHAR", "NCHAR" -> 1;
            case "VARBINARY" -> 255;
            case "BINARY" -> 16;
            default -> null;
        };
    }

    private static Integer normalizePrecision(
            String dbType,
            Integer value,
            ColumnGeneratorType generatorType,
            Integer scaleValue
    ) {
        Integer normalizedValue = positive(value);
        if (normalizedValue != null) {
            return normalizedValue;
        }
        if ("DECIMAL".equals(dbType) || "NUMERIC".equals(dbType)) {
            return 18;
        }
        if ("NUMBER".equals(dbType) && (generatorType == ColumnGeneratorType.RANDOM_DECIMAL || (scaleValue != null && scaleValue > 0))) {
            return 18;
        }
        return null;
    }

    private static Integer normalizeScale(String dbType, Integer value, ColumnGeneratorType generatorType) {
        Integer normalizedValue = nonNegative(value);
        if (normalizedValue != null) {
            return normalizedValue;
        }
        if ("DECIMAL".equals(dbType) || "NUMERIC".equals(dbType)) {
            return 2;
        }
        if ("NUMBER".equals(dbType) && generatorType == ColumnGeneratorType.RANDOM_DECIMAL) {
            return 2;
        }
        return null;
    }

    private static Integer positive(Integer value) {
        if (value == null || value < 1) {
            return null;
        }
        return value;
    }

    private static Integer nonNegative(Integer value) {
        if (value == null || value < 0) {
            return null;
        }
        return value;
    }

    private static String normalizeType(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
