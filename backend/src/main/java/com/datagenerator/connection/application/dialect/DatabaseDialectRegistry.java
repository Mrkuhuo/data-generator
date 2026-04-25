package com.datagenerator.connection.application.dialect;

import com.datagenerator.connection.domain.DatabaseType;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class DatabaseDialectRegistry {

    private final Map<DatabaseType, DatabaseDialect> dialects = new EnumMap<>(DatabaseType.class);

    public DatabaseDialectRegistry(List<DatabaseDialect> dialects) {
        for (DatabaseDialect dialect : dialects) {
            this.dialects.put(dialect.type(), dialect);
        }
    }

    public DatabaseDialect get(DatabaseType databaseType) {
        DatabaseDialect dialect = dialects.get(databaseType);
        if (dialect == null) {
            throw new IllegalArgumentException("未配置数据库方言: " + databaseType);
        }
        return dialect;
    }
}
