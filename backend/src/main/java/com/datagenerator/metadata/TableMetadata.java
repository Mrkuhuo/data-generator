package com.datagenerator.metadata;

import com.datagenerator.entity.DataSource;
import lombok.Data;
import java.util.*;

@Data
public class TableMetadata {
    private String tableName;
    private Map<String, ColumnMetadata> columns = new HashMap<>();
    private String primaryKeyColumn;
    private boolean autoIncrement;
    private List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
    private Set<String> uniqueColumns = new HashSet<>();
    private DataSource dataSource;
} 