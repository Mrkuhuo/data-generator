package com.datagenerator.generator.impl;

import com.datagenerator.entity.DataSource;
import com.datagenerator.entity.DataTask;
import com.datagenerator.metadata.ForeignKeyMetadata;
import com.datagenerator.metadata.TableMetadata;
import com.datagenerator.metadata.ColumnMetadata;
import com.datagenerator.generator.rule.DataRuleFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class RelationalDatabaseGenerator {

    private final ObjectMapper objectMapper;
    private final DataRuleFactory ruleFactory;
    
    // 添加配置常量
    private static final int DEFAULT_BATCH_SIZE = Integer.parseInt(System.getProperty("data.generator.batch.size", "1000"));
    private static final int DEFAULT_PRECISION = Integer.parseInt(System.getProperty("data.generator.default.precision", "10"));
    private static final int DEFAULT_SCALE = Integer.parseInt(System.getProperty("data.generator.default.scale", "2"));
    private static final int DEFAULT_STRING_LENGTH = Integer.parseInt(System.getProperty("data.generator.default.string.length", "50"));
    private static final int DEFAULT_FOREIGN_KEY_COUNT = Integer.parseInt(System.getProperty("data.generator.foreign.key.count", "10"));
    private static final int MAX_FOREIGN_KEY_COUNT = Integer.parseInt(System.getProperty("data.generator.foreign.key.max.count", "20"));

    // 数据库元数据字段常量
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String IS_NULLABLE = "IS_NULLABLE";
    private static final String COLUMN_SIZE = "COLUMN_SIZE";
    private static final String COLUMN_DEF = "COLUMN_DEF";
    private static final String REMARKS = "REMARKS";
    private static final String FIELD = "Field";
    private static final String TYPE = "Type";
    private static final String KEY = "Key";
    private static final String EXTRA = "Extra";
    private static final String DEFAULT = "Default";
    
    // 外键元数据字段常量
    private static final String FK_COLUMN_NAME = "FKCOLUMN_NAME";
    private static final String PK_TABLE_NAME = "PKTABLE_NAME";
    private static final String PK_COLUMN_NAME = "PKCOLUMN_NAME";
    
    // 数据库特殊值常量
    private static final String YES = "YES";
    private static final String NULL = "NULL";
    private static final String AUTO_INCREMENT = "auto_increment";
    private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

    // 日期时间格式常量
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final String TIME_MIN = "00:00:00";
    private static final String TIME_MAX = "23:59:59";
    
    // 数据类型常量
    private static final String TYPE_INT = "int";
    private static final String TYPE_BIT = "bit";
    private static final String TYPE_TINYINT = "tinyint";
    private static final String TYPE_SMALLINT = "smallint";
    private static final String TYPE_MEDIUMINT = "mediumint";
    private static final String TYPE_BIGINT = "bigint";
    private static final String TYPE_DECIMAL = "decimal";
    private static final String TYPE_NUMERIC = "numeric";
    private static final String TYPE_FLOAT = "float";
    private static final String TYPE_DOUBLE = "double";
    private static final String TYPE_CHAR = "char";
    private static final String TYPE_TEXT = "text";
    private static final String TYPE_DATETIME = "datetime";
    private static final String TYPE_TIMESTAMP = "timestamp";
    private static final String TYPE_DATE = "date";
    private static final String TYPE_TIME = "time";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_ENUM = "enum";
    private static final String TYPE_UNSIGNED = "unsigned";
    
    // JSON 字段名常量
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_PARAMS = "params";
    private static final String FIELD_VALUES = "values";
    private static final String FIELD_INTEGER = "integer";
    private static final String FIELD_MIN = "min";
    private static final String FIELD_MAX = "max";
    private static final String FIELD_PRECISION = "precision";
    private static final String FIELD_PATTERN = "pattern";
    private static final String FIELD_LENGTH = "length";
    private static final String FIELD_FORMAT = "format";
    
    // 模式常量
    private static final String PATTERN_UUID = "${uuid}";
    private static final String PATTERN_NUMBER = "${number}";
    private static final String PATTERN_SHORT_STRING = "${shortString}";
    private static final String PATTERN_MEDIUM_STRING = "${mediumString}";
    private static final String PATTERN_LONG_STRING = "${longString}";
    
    // SQL 常量
    private static final String SQL_FOREIGN_KEY_CHECKS_OFF = "SET FOREIGN_KEY_CHECKS = 0";
    private static final String SQL_FOREIGN_KEY_CHECKS_ON = "SET FOREIGN_KEY_CHECKS = 1";
    private static final String SQL_TRUNCATE = "TRUNCATE TABLE %s";
    private static final String SQL_SHOW_COLUMNS = "SHOW COLUMNS FROM %s WHERE %s = '%s'";
    private static final String SQL_SELECT_DISTINCT = "SELECT DISTINCT %s FROM %s";
    private static final String SQL_INSERT = "INSERT INTO %s (%s) VALUES (%s)";

    private final Random random = new Random();

    public RelationalDatabaseGenerator(DataRuleFactory ruleFactory) {
        this.objectMapper = new ObjectMapper();
        this.ruleFactory = ruleFactory;
    }

    /**
     * 生成数据
     */
    public void generateData(DataTask task, DataSource dataSource, String[] tables) throws SQLException {
        log.info("开始生成关系型数据库数据，数据源：{}，表：{}", dataSource.getName(), Arrays.toString(tables));
        
        try (Connection conn = getConnection(dataSource)) {
            // 1. 获取所有表的元数据信息
            Map<String, TableMetadata> tablesMetadata = loadTablesMetadata(dataSource, tables);
            
            // 2. 构建表依赖关系图并排序
            List<String> sortedTables = sortTablesByDependencies(tablesMetadata);
            log.info("表处理顺序：{}", sortedTables);
            
            // 3. 如果是追加模式，预先加载现有主键值
            Map<String, Set<Object>> existingPrimaryKeys = new HashMap<>();
            Map<String, Map<String, Set<Object>>> existingForeignKeys = new HashMap<>();
            if ("APPEND".equalsIgnoreCase(task.getWriteMode())) {
                for (String tableName : sortedTables) {
                    TableMetadata metadata = tablesMetadata.get(tableName);
                    // 加载主键值
                    existingPrimaryKeys.put(tableName, loadExistingPrimaryKeys(conn, metadata));
                    // 加载外键值
                    existingForeignKeys.put(tableName, loadExistingForeignKeys(conn, metadata));
                }
            }
            
            // 4. 根据排序后的顺序生成数据
            for (String tableName : sortedTables) {
                TableMetadata metadata = tablesMetadata.get(tableName);
                generateTableData(task, dataSource, metadata, tablesMetadata, 
                    existingPrimaryKeys.get(tableName),
                    existingForeignKeys.get(tableName));
            }
            
        } catch (Exception e) {
            log.error("生成数据失败：{}", e.getMessage(), e);
            throw new RuntimeException("生成数据失败：" + e.getMessage(), e);
        }
    }

    /**
     * 加载表的现有主键值
     */
    private Set<Object> loadExistingPrimaryKeys(Connection conn, TableMetadata metadata) throws SQLException {
        Set<Object> keys = new HashSet<>();
        if (metadata.getPrimaryKeyColumn() != null) {
            String sql = String.format("SELECT DISTINCT %s FROM %s", 
                metadata.getPrimaryKeyColumn(), metadata.getTableName());
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    keys.add(rs.getObject(1));
                }
            }
        }
        return keys;
    }

    /**
     * 获取表中主键的最大值
     */
    private Object getMaxPrimaryKey(Connection conn, TableMetadata metadata) {
        if (metadata.getPrimaryKeyColumn() == null) {
            return null;
        }
        
        String dataType = metadata.getColumns().get(metadata.getPrimaryKeyColumn()).getDataType().toLowerCase();
        if (!dataType.contains("int") && !dataType.contains("decimal") && 
            !dataType.contains("numeric") && !dataType.contains("float") && 
            !dataType.contains("double")) {
            return null; // 只对数值类型的主键获取最大值
        }
        
        try {
            String sql = String.format("SELECT MAX(%s) FROM %s", 
                metadata.getPrimaryKeyColumn(), metadata.getTableName());
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    Object maxValue = rs.getObject(1);
                    log.info("表 {} 的主键 {} 最大值为: {}", 
                        metadata.getTableName(), metadata.getPrimaryKeyColumn(), maxValue);
                    return maxValue;
                }
            }
        } catch (SQLException e) {
            log.error("获取表 {} 的主键最大值失败: {}", metadata.getTableName(), e.getMessage());
        }
        return null;
    }

    /**
     * 加载表的现有外键值
     */
    private Map<String, Set<Object>> loadExistingForeignKeys(Connection conn, TableMetadata metadata) throws SQLException {
        Map<String, Set<Object>> foreignKeys = new HashMap<>();
        for (ForeignKeyMetadata fk : metadata.getForeignKeys()) {
            Set<Object> values = new HashSet<>();
            String sql = String.format("SELECT DISTINCT %s FROM %s", 
                fk.getColumnName(), metadata.getTableName());
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    values.add(rs.getObject(1));
                }
            }
            foreignKeys.put(fk.getColumnName(), values);
        }
        return foreignKeys;
    }

    /**
     * 加载表的元数据信息
     */
    private Map<String, TableMetadata> loadTablesMetadata(DataSource dataSource, String[] tables) throws SQLException {
        Map<String, TableMetadata> metadata = new HashMap<>();
        
        try (Connection conn = getConnection(dataSource)) {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            
            // 获取当前数据库名
            String catalog = conn.getCatalog();
            String schema = conn.getSchema();
            
            for (String tableName : tables) {
                TableMetadata tableMetadata = new TableMetadata();
                tableMetadata.setTableName(tableName);
                
                // 加载列信息
                try (ResultSet columns = dbMetaData.getColumns(catalog, schema, tableName, null)) {
                    while (columns.next()) {
                        ColumnMetadata columnMetadata = new ColumnMetadata();
                        columnMetadata.setName(columns.getString(COLUMN_NAME));
                        columnMetadata.setDataType(columns.getString(TYPE_NAME));
                        columnMetadata.setNullable(YES.equals(columns.getString(IS_NULLABLE)));
                        columnMetadata.setMaxLength(columns.getString(COLUMN_SIZE));
                        columnMetadata.setDefaultValue(columns.getString(COLUMN_DEF));
                        columnMetadata.setComment(columns.getString(REMARKS));
                        
                        // 获取列的其他属性
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(
                                 String.format("SHOW COLUMNS FROM %s WHERE %s = '%s'", 
                                     tableName, FIELD, columnMetadata.getName()))) {
                            if (rs.next()) {
                                columnMetadata.setColumnKey(rs.getString(KEY));
                                columnMetadata.setExtra(rs.getString(EXTRA));
                            }
                        }
                        
                        tableMetadata.getColumns().put(columnMetadata.getName(), columnMetadata);
                    }
                }
                
                // 加载主键信息
                try (ResultSet primaryKeys = dbMetaData.getPrimaryKeys(catalog, schema, tableName)) {
                    while (primaryKeys.next()) {
                        String columnName = primaryKeys.getString(COLUMN_NAME);
                        tableMetadata.setPrimaryKeyColumn(columnName);
                        
                        // 检查是否为自增主键
                        ColumnMetadata columnMetadata = tableMetadata.getColumns().get(columnName);
                        if (columnMetadata != null) {
                            tableMetadata.setAutoIncrement(columnMetadata.getExtra() != null && 
                                columnMetadata.getExtra().toLowerCase().contains(AUTO_INCREMENT));
                        }
                    }
                }
                
                // 加载外键信息
                try (ResultSet foreignKeys = dbMetaData.getImportedKeys(catalog, schema, tableName)) {
                    while (foreignKeys.next()) {
                        ForeignKeyMetadata fkMetadata = new ForeignKeyMetadata();
                        fkMetadata.setColumnName(foreignKeys.getString(FK_COLUMN_NAME));
                        fkMetadata.setReferencedTable(foreignKeys.getString(PK_TABLE_NAME));
                        fkMetadata.setReferencedColumn(foreignKeys.getString(PK_COLUMN_NAME));
                        tableMetadata.getForeignKeys().add(fkMetadata);
                    }
                }
                
                // 加载唯一约束信息
                try (ResultSet uniqueKeys = dbMetaData.getIndexInfo(catalog, schema, tableName, true, false)) {
                    while (uniqueKeys.next()) {
                        String columnName = uniqueKeys.getString("COLUMN_NAME");
                        if (columnName != null) {
                            tableMetadata.getUniqueColumns().add(columnName);
                        }
                    }
                }
                
                metadata.put(tableName, tableMetadata);
            }
        }
        
        return metadata;
    }

    /**
     * 根据表依赖关系进行排序
     */
    private List<String> sortTablesByDependencies(Map<String, TableMetadata> tablesMetadata) {
        // 构建依赖图
        Map<String, Set<String>> dependencyGraph = new HashMap<>();
        
        // 记录所有表的外键依赖关系
        for (TableMetadata table : tablesMetadata.values()) {
            Set<String> dependencies = new HashSet<>();
            for (ForeignKeyMetadata fk : table.getForeignKeys()) {
                // 只有当引用的表也在生成范围内时，才添加依赖
                if (tablesMetadata.containsKey(fk.getReferencedTable())) {
                    dependencies.add(fk.getReferencedTable());
                    log.info("表 {} 依赖于表 {}", table.getTableName(), fk.getReferencedTable());
                }
            }
            dependencyGraph.put(table.getTableName(), dependencies);
        }
        
        // 使用拓扑排序
        List<String> sortedTables = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> visiting = new HashSet<>();
        
        // 记录所有表的入度（被引用次数）
        Map<String, Integer> inDegree = new HashMap<>();
        for (String tableName : tablesMetadata.keySet()) {
            inDegree.put(tableName, 0);
        }
        
        for (Set<String> deps : dependencyGraph.values()) {
            for (String dep : deps) {
                inDegree.put(dep, inDegree.getOrDefault(dep, 0) + 1);
            }
        }
        
        // 记录入度信息
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            log.info("表 {} 的入度（被引用次数）: {}", entry.getKey(), entry.getValue());
        }
        
        try {
            // 尝试拓扑排序
            for (String table : tablesMetadata.keySet()) {
                if (!visited.contains(table)) {
                    topologicalSort(table, dependencyGraph, visited, visiting, sortedTables);
                }
            }
            
            // 反转结果，使被依赖的表在前面
            Collections.reverse(sortedTables);
        } catch (RuntimeException e) {
            log.error("排序表依赖关系时发生错误: {}", e.getMessage());
            // 如果发现循环依赖，使用基于入度的排序
            if (e.getMessage().contains("循环依赖")) {
                log.warn("检测到循环依赖，将使用基于入度的排序");
                sortedTables.clear();
                
                // 按入度排序，入度高的表（被多个表引用的）先处理
                List<String> tablesByInDegree = new ArrayList<>(tablesMetadata.keySet());
                tablesByInDegree.sort((t1, t2) -> inDegree.getOrDefault(t2, 0) - inDegree.getOrDefault(t1, 0));
                
                return tablesByInDegree;
            }
        }
        
        // 记录排序结果
        log.info("表依赖关系排序结果: {}", sortedTables);
        for (String table : sortedTables) {
            Set<String> deps = dependencyGraph.get(table);
            if (deps != null && !deps.isEmpty()) {
                log.info("表 {} 依赖于: {}", table, deps);
            }
        }
        
        // 验证排序结果是否满足依赖关系
        boolean valid = true;
        Map<String, Integer> tableIndex = new HashMap<>();
        for (int i = 0; i < sortedTables.size(); i++) {
            tableIndex.put(sortedTables.get(i), i);
        }
        
        for (String table : sortedTables) {
            Set<String> deps = dependencyGraph.get(table);
            if (deps != null) {
                for (String dep : deps) {
                    if (tableIndex.containsKey(dep) && tableIndex.get(dep) > tableIndex.get(table)) {
                        log.error("排序结果不满足依赖关系: 表 {} 依赖于表 {}, 但在排序结果中 {} 排在 {} 之后",
                            table, dep, dep, table);
                        valid = false;
                    }
                }
            }
        }
        
        if (!valid) {
            log.warn("排序结果不满足所有依赖关系，将使用基于入度的排序");
            List<String> tablesByInDegree = new ArrayList<>(tablesMetadata.keySet());
            tablesByInDegree.sort((t1, t2) -> inDegree.getOrDefault(t2, 0) - inDegree.getOrDefault(t1, 0));
            
            log.info("基于入度的排序结果: {}", tablesByInDegree);
            return tablesByInDegree;
        }
        
        return sortedTables;
    }

    /**
     * 拓扑排序的递归实现
     */
    private void topologicalSort(String table, Map<String, Set<String>> dependencyGraph,
                               Set<String> visited, Set<String> visiting,
                               List<String> sortedTables) {
        visiting.add(table);
        
        for (String dependency : dependencyGraph.getOrDefault(table, new HashSet<>())) {
            if (visiting.contains(dependency)) {
                throw new RuntimeException("检测到循环依赖：" + dependency);
            }
            if (!visited.contains(dependency)) {
                topologicalSort(dependency, dependencyGraph, visited, visiting, sortedTables);
            }
        }
        
        visiting.remove(table);
        visited.add(table);
        sortedTables.add(table);
    }

    /**
     * 为单个表生成数据
     */
    private void generateTableData(DataTask task, DataSource dataSource,
                                 TableMetadata tableMetadata,
                                 Map<String, TableMetadata> allTablesMetadata,
                                 Set<Object> existingPrimaryKeys,
                                 Map<String, Set<Object>> existingForeignKeys) throws SQLException {
        log.info("开始为表 {} 生成数据", tableMetadata.getTableName());
        
        // 1. 准备外键值
        Map<String, ForeignKeyMetadata> foreignKeyMetadataMap = new HashMap<>();
        for (ForeignKeyMetadata fk : tableMetadata.getForeignKeys()) {
            String key = fk.getReferencedTable() + "." + fk.getReferencedColumn();
            foreignKeyMetadataMap.put(key, fk);
        }
        
        // 2. 准备有效的外键值
        try {
            prepareValidForeignKeyValues(dataSource, tableMetadata, allTablesMetadata);
        } catch (SQLException e) {
            log.error("准备表 {} 的外键值时发生错误: {}", tableMetadata.getTableName(), e.getMessage());
            // 如果是外键约束错误，尝试继续
            if (e.getMessage().contains("foreign key constraint")) {
                log.warn("由于外键约束错误，将跳过表 {} 的数据生成", tableMetadata.getTableName());
                return;
            }
            throw e;
        }
        
        // 3. 创建数据生成器并设置外键元数据
        TemplateDataGenerator generator = new TemplateDataGenerator(ruleFactory);
        generator.setForeignKeyMetadata(foreignKeyMetadataMap);
        
        // 4. 获取主键最大值（如果是数值类型）
        Object maxPrimaryKey = null;
        if (tableMetadata.getPrimaryKeyColumn() != null) {
            // 设置数据源到表元数据，用于获取主键最大值
            tableMetadata.setDataSource(dataSource);
            maxPrimaryKey = getMaxPrimaryKey(getConnection(dataSource), tableMetadata);
            log.info("表 {} 的主键最大值: {}", tableMetadata.getTableName(), maxPrimaryKey);
        }
        
        // 5. 生成或验证数据模板
        String template;
        try {
            template = validateAndUpdateTemplate(task.getTemplate(), tableMetadata, dataSource);
        } catch (Exception e) {
            log.error("验证表 {} 的模板时发生错误: {}", tableMetadata.getTableName(), e.getMessage());
            throw new RuntimeException("验证模板失败: " + e.getMessage(), e);
        }
        
        // 6. 生成数据
        List<Map<String, Object>> rawData;
        try {
            rawData = generator.generate(template, task.getBatchSize());
            
            // 特殊处理enum类型的字段
            for (Map.Entry<String, ColumnMetadata> entry : tableMetadata.getColumns().entrySet()) {
                String columnName = entry.getKey();
                ColumnMetadata columnMetadata = entry.getValue();
                
                if (columnMetadata.getDataType().toLowerCase().contains("enum")) {
                    // 获取enum类型的允许值
                    List<String> allowedValues = getEnumAllowedValues(
                        columnMetadata, tableMetadata.getTableName(), dataSource);
                    
                    if (!allowedValues.isEmpty()) {
                        log.info("为表 {} 的enum字段 {} 预处理数据，允许的值: {}", 
                            tableMetadata.getTableName(), columnName, allowedValues);
                        
                        // 检查并修正每一行的enum值
                        for (Map<String, Object> row : rawData) {
                            Object value = row.get(columnName);
                            if (value != null) {
                                String strValue = value.toString();
                                boolean valueAllowed = false;
                                
                                for (String allowedValue : allowedValues) {
                                    if (strValue.equalsIgnoreCase(allowedValue)) {
                                        // 使用正确的大小写
                                        row.put(columnName, allowedValue);
                                        valueAllowed = true;
                                        break;
                                    }
                                }
                                
                                // 如果值不在允许范围内，使用第一个允许的值
                                if (!valueAllowed) {
                                    log.warn("表 {} 字段 {} 的枚举值 {} 不在允许的值 {} 范围内，将使用第一个允许的值", 
                                        tableMetadata.getTableName(), columnName, strValue, allowedValues);
                                    row.put(columnName, allowedValues.get(0));
                                }
                            } else if (!columnMetadata.isNullable()) {
                                // 如果字段不允许为空，设置默认值
                                log.warn("表 {} 字段 {} 的值为空但不允许为空，将使用第一个允许的值", 
                                    tableMetadata.getTableName(), columnName);
                                row.put(columnName, allowedValues.get(0));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("生成表 {} 的原始数据时发生错误: {}", tableMetadata.getTableName(), e.getMessage());
            throw new RuntimeException("生成原始数据失败: " + e.getMessage(), e);
        }
        
        // 7. 处理和验证数据
        List<Map<String, Object>> processedData;
        try {
            processedData = processGeneratedData(rawData, tableMetadata, 
                existingPrimaryKeys, existingForeignKeys, maxPrimaryKey);
        } catch (Exception e) {
            log.error("处理表 {} 的生成数据时发生错误: {}", tableMetadata.getTableName(), e.getMessage());
            throw new RuntimeException("处理生成数据失败: " + e.getMessage(), e);
        }
        
        // 8. 插入数据
        int retryCount = 0;
        final int MAX_RETRIES = 3;
        
        while (retryCount < MAX_RETRIES) {
            try {
                insertDataIntoTable(dataSource, tableMetadata, processedData, task.getWriteMode());
                log.info("成功为表 {} 插入 {} 条数据", tableMetadata.getTableName(), processedData.size());
                break;
            } catch (SQLException e) {
                retryCount++;
                
                if (e.getMessage().contains("Duplicate entry") && e.getMessage().contains("PRIMARY")) {
                    log.error("插入数据时发生主键冲突: {}", e.getMessage());
                    
                    if (retryCount < MAX_RETRIES) {
                        log.warn("尝试重新生成主键值并重试 (尝试 {}/{})", retryCount, MAX_RETRIES);
                        // 重新处理数据，生成新的主键值
                        processedData = processGeneratedData(rawData, tableMetadata, 
                            existingPrimaryKeys, existingForeignKeys, maxPrimaryKey);
                    } else {
                        log.warn("达到最大重试次数，跳过表 {} 的数据插入", tableMetadata.getTableName());
                        break;
                    }
                } else if (e.getMessage().contains("foreign key constraint")) {
                    log.error("插入数据时发生外键约束错误: {}", e.getMessage());
                    
                    // 提取外键约束名称和引用表信息
                    String errorMsg = e.getMessage();
                    String constraintName = "";
                    String referencedTable = "";
                    
                    // 尝试从错误消息中提取约束名称
                    Pattern pattern = Pattern.compile("CONSTRAINT `([^`]+)`");
                    Matcher matcher = pattern.matcher(errorMsg);
                    if (matcher.find()) {
                        constraintName = matcher.group(1);
                    }
                    
                    // 查找相关的外键定义
                    for (ForeignKeyMetadata fk : tableMetadata.getForeignKeys()) {
                        if (errorMsg.contains(fk.getReferencedTable())) {
                            referencedTable = fk.getReferencedTable();
                            log.error("外键约束错误涉及引用表: {}, 引用列: {}", 
                                fk.getReferencedTable(), fk.getReferencedColumn());
                            
                            // 检查引用表中是否有数据
                            try (Connection conn = getConnection(dataSource)) {
                                String countSql = String.format("SELECT COUNT(*) FROM %s", fk.getReferencedTable());
                                try (Statement stmt = conn.createStatement();
                                     ResultSet rs = stmt.executeQuery(countSql)) {
                                    if (rs.next()) {
                                        int count = rs.getInt(1);
                                        log.info("引用表 {} 中有 {} 条数据", fk.getReferencedTable(), count);
                                    }
                                }
                            } catch (Exception ex) {
                                log.error("检查引用表数据时发生错误: {}", ex.getMessage());
                            }
                            
                            // 检查外键值是否有效
                            log.info("检查外键 {} 的有效值集合大小: {}", 
                                fk.getColumnName(), fk.getValidValues().size());
                            if (fk.getValidValues().isEmpty()) {
                                log.error("外键 {} 没有有效的引用值", fk.getColumnName());
                            }
                        }
                    }
                    
                    if (retryCount < MAX_RETRIES) {
                        log.warn("尝试重新准备外键值并重试 (尝试 {}/{})", retryCount, MAX_RETRIES);
                        // 重新准备外键值
                        prepareValidForeignKeyValues(dataSource, tableMetadata, allTablesMetadata);
                        // 重新处理数据
                        processedData = processGeneratedData(rawData, tableMetadata, 
                            existingPrimaryKeys, existingForeignKeys, maxPrimaryKey);
                    } else {
                        log.warn("达到最大重试次数，跳过表 {} 的数据插入", tableMetadata.getTableName());
                        log.error("表 {} 的数据插入失败，请检查外键约束 {} 和引用表 {}", 
                            tableMetadata.getTableName(), constraintName, referencedTable);
                        break;
                    }
                } else if (e.getMessage().contains("Data truncated")) {
                    log.error("插入数据时发生数据截断错误: {}", e.getMessage());
                    
                    // 尝试从错误消息中提取列名
                    String columnName = "";
                    Pattern pattern = Pattern.compile("Data truncated for column '([^']+)'");
                    Matcher matcher = pattern.matcher(e.getMessage());
                    if (matcher.find()) {
                        columnName = matcher.group(1);
                        log.error("数据截断错误发生在列: {}", columnName);
                        
                        // 检查列的定义
                        ColumnMetadata column = tableMetadata.getColumns().get(columnName);
                        if (column != null) {
                            log.info("列 {} 的定义: 类型={}, 可空={}, 默认值={}", 
                                columnName, column.getDataType(), column.isNullable(), column.getDefaultValue());
                            
                            // 如果是enum类型，检查允许的值
                            if (column.getDataType().toLowerCase().contains("enum")) {
                                List<String> allowedValues = getEnumAllowedValues(
                                    column, tableMetadata.getTableName(), dataSource);
                                log.info("列 {} 的允许值: {}", columnName, allowedValues);
                            }
                        }
                    }
                    
                    if (retryCount < MAX_RETRIES) {
                        log.warn("尝试修复数据截断问题并重试 (尝试 {}/{})", retryCount, MAX_RETRIES);
                        // 重新处理数据，特别注意数据类型转换
                        processedData = processGeneratedData(rawData, tableMetadata, 
                            existingPrimaryKeys, existingForeignKeys, maxPrimaryKey);
                    } else {
                        log.warn("达到最大重试次数，跳过表 {} 的数据插入", tableMetadata.getTableName());
                        break;
                    }
                } else {
                    // 其他SQL异常，继续抛出
                    log.error("插入数据时发生未处理的SQL异常: {}", e.getMessage());
                    throw e;
                }
            }
        }
    }

    /**
     * 准备有效的外键值
     */
    private void prepareValidForeignKeyValues(DataSource dataSource, 
                                            TableMetadata tableMetadata,
                                            Map<String, TableMetadata> allTablesMetadata) throws SQLException {
        log.info("准备表 {} 的外键值", tableMetadata.getTableName());
        
        try (Connection conn = getConnection(dataSource)) {
            for (ForeignKeyMetadata fk : tableMetadata.getForeignKeys()) {
                TableMetadata referencedTable = allTablesMetadata.get(fk.getReferencedTable());
                
                log.info("处理表 {} 的外键 {}，引用表 {}，引用列 {}", 
                    tableMetadata.getTableName(), fk.getColumnName(), 
                    fk.getReferencedTable(), fk.getReferencedColumn());
                
                // 查询引用表的现有数据
                String sql = String.format("SELECT DISTINCT %s FROM %s", 
                    fk.getReferencedColumn(), fk.getReferencedTable());
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        Object value = rs.getObject(1);
                        if (value != null) {
                            fk.getValidValues().add(value);
                        }
                    }
                    
                    log.info("从表 {} 中查询到 {} 个有效的外键值", 
                        fk.getReferencedTable(), fk.getValidValues().size());
                }
                
                // 如果没有有效的外键值，并且引用的表不在生成范围内，才创建默认值
                if (fk.getValidValues().isEmpty() && referencedTable == null) {
                    log.warn("表 {} 的外键 {} 没有有效的引用值，将创建默认值", 
                        tableMetadata.getTableName(), fk.getColumnName());
                    
                    // 获取引用列的数据类型
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                             String.format("SHOW COLUMNS FROM %s WHERE Field = '%s'",
                                 fk.getReferencedTable(), fk.getReferencedColumn()))) {
                        if (rs.next()) {
                            String dataType = rs.getString("Type").toLowerCase();
                            // 根据数据类型生成一些默认值
                            Set<Object> defaultValues = generateDefaultForeignKeyValues(dataType);
                            fk.getValidValues().addAll(defaultValues);
                            
                            // 将这些值插入到引用表中
                            for (Object value : defaultValues) {
                                try {
                                    String insertSql = String.format(
                                        "INSERT INTO %s (%s) VALUES (%s)",
                                        fk.getReferencedTable(),
                                        fk.getReferencedColumn(),
                                        formatSqlValue(value)
                                    );
                                    log.info("插入默认外键值: {}", insertSql);
                                    stmt.execute(insertSql);
                                } catch (SQLException e) {
                                    log.warn("插入默认外键值失败：{}", e.getMessage());
                                }
                            }
                        }
                    }
                }
                
                // 如果引用的表在生成范围内但还没有数据，记录警告并尝试生成默认数据
                if (fk.getValidValues().isEmpty() && referencedTable != null) {
                    log.error("表 {} 的外键 {} 引用的表 {} 还没有生成数据，这可能导致外键约束错误", 
                        tableMetadata.getTableName(), fk.getColumnName(), fk.getReferencedTable());
                    
                    // 检查引用表是否有主键
                    String referencedPrimaryKey = referencedTable.getPrimaryKeyColumn();
                    if (referencedPrimaryKey != null && fk.getReferencedColumn().equals(referencedPrimaryKey)) {
                        log.info("引用列 {} 是表 {} 的主键", fk.getReferencedColumn(), fk.getReferencedTable());
                        
                        // 尝试为引用表生成一些默认主键数据
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(
                                 String.format("SHOW COLUMNS FROM %s WHERE Field = '%s'",
                                     fk.getReferencedTable(), fk.getReferencedColumn()))) {
                            if (rs.next()) {
                                String dataType = rs.getString("Type").toLowerCase();
                                // 根据数据类型生成一些默认值
                                Set<Object> defaultValues = generateDefaultForeignKeyValues(dataType);
                                
                                // 将这些值插入到引用表中
                                for (Object value : defaultValues) {
                                    try {
                                        String insertSql = String.format(
                                            "INSERT INTO %s (%s) VALUES (%s)",
                                            fk.getReferencedTable(),
                                            fk.getReferencedColumn(),
                                            formatSqlValue(value)
                                        );
                                        log.info("为引用表插入默认主键值: {}", insertSql);
                                        stmt.execute(insertSql);
                                        fk.getValidValues().add(value);
                                    } catch (SQLException e) {
                                        log.warn("插入引用表默认主键值失败：{}", e.getMessage());
                                    }
                                }
                            }
                        } catch (SQLException e) {
                            log.error("为引用表生成默认主键数据失败: {}", e.getMessage());
                        }
                    } else {
                        // 如果引用列不是主键，尝试查询引用表的所有列
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(
                                 String.format("SHOW COLUMNS FROM %s", fk.getReferencedTable()))) {
                            
                            // 收集引用表的所有列信息
                            Map<String, String> columnTypes = new HashMap<>();
                            String primaryKeyColumn = null;
                            
                            while (rs.next()) {
                                String field = rs.getString("Field");
                                String type = rs.getString("Type");
                                String key = rs.getString("Key");
                                
                                columnTypes.put(field, type);
                                if ("PRI".equals(key)) {
                                    primaryKeyColumn = field;
                                }
                            }
                            
                            // 如果找到了主键列，尝试插入一条最小化的记录
                            if (primaryKeyColumn != null) {
                                // 生成主键值
                                Object pkValue = null;
                                String pkType = columnTypes.get(primaryKeyColumn);
                                
                                if (pkType.contains("int")) {
                                    // 对于整数类型主键，使用一个大值
                                    pkValue = 10000 + new Random().nextInt(90000);
                                } else {
                                    // 对于其他类型，使用UUID
                                    pkValue = UUID.randomUUID().toString();
                                }
                                
                                // 构建插入语句
                                StringBuilder columns = new StringBuilder(primaryKeyColumn);
                                StringBuilder values = new StringBuilder(formatSqlValue(pkValue));
                                
                                // 如果引用列不是主键，也需要包含它
                                if (!primaryKeyColumn.equals(fk.getReferencedColumn())) {
                                    // 生成引用列的值
                                    Object refValue = null;
                                    String refType = columnTypes.get(fk.getReferencedColumn());
                                    
                                    if (refType.contains("int")) {
                                        refValue = 1 + new Random().nextInt(100);
                                    } else if (refType.contains("varchar") || refType.contains("char")) {
                                        refValue = "default_" + UUID.randomUUID().toString().substring(0, 8);
                                    } else {
                                        refValue = "default";
                                    }
                                    
                                    columns.append(", ").append(fk.getReferencedColumn());
                                    values.append(", ").append(formatSqlValue(refValue));
                                    
                                    // 添加到有效值集合
                                    fk.getValidValues().add(refValue);
                                } else {
                                    // 如果引用列就是主键，将主键值添加到有效值集合
                                    fk.getValidValues().add(pkValue);
                                }
                                
                                // 执行插入
                                try {
                                    String insertSql = String.format(
                                        "INSERT INTO %s (%s) VALUES (%s)",
                                        fk.getReferencedTable(),
                                        columns.toString(),
                                        values.toString()
                                    );
                                    log.info("为引用表插入最小化记录: {}", insertSql);
                                    stmt.execute(insertSql);
                                } catch (SQLException e) {
                                    log.warn("插入引用表最小化记录失败：{}", e.getMessage());
                                }
                            }
                        } catch (SQLException e) {
                            log.error("查询引用表列信息失败: {}", e.getMessage());
                        }
                    }
                }
                
                // 最终检查是否有有效的外键值
                if (fk.getValidValues().isEmpty()) {
                    log.error("表 {} 的外键 {} 没有有效的引用值，这将导致外键约束错误", 
                        tableMetadata.getTableName(), fk.getColumnName());
                } else {
                    log.info("表 {} 的外键 {} 有 {} 个有效的引用值", 
                        tableMetadata.getTableName(), fk.getColumnName(), fk.getValidValues().size());
                    
                    // 记录前10个有效值用于调试
                    List<Object> sampleValues = new ArrayList<>(fk.getValidValues());
                    if (sampleValues.size() > 10) {
                        sampleValues = sampleValues.subList(0, 10);
                    }
                    log.info("表 {} 的外键 {} 的部分有效值: {}", 
                        tableMetadata.getTableName(), fk.getColumnName(), sampleValues);
                }
            }
        }
    }

    /**
     * 生成默认的外键值
     */
    private Set<Object> generateDefaultForeignKeyValues(String dataType) {
        Set<Object> values = new HashSet<>();
        String type = dataType.toLowerCase();
        
        // 根据数据类型生成合适范围的值
        if (type.contains("int") || type.contains("bit")) {
            int max;
            if (type.contains("tinyint")) {
                max = type.contains("unsigned") ? 255 : 127;
            } else if (type.contains("smallint")) {
                max = type.contains("unsigned") ? 65535 : 32767;
            } else if (type.contains("mediumint")) {
                max = type.contains("unsigned") ? 16777215 : 8388607;
            } else if (type.contains("bigint")) {
                max = Integer.MAX_VALUE;
            } else {
                max = type.contains("unsigned") ? Integer.MAX_VALUE : 2147483647;
            }
            
            // 根据最大值计算合适的生成数量
            int count = Math.min(MAX_FOREIGN_KEY_COUNT, Math.max(DEFAULT_FOREIGN_KEY_COUNT, (int)Math.ceil(Math.log10(max))));
            for (int i = 0; i < count; i++) {
                values.add(new Random().nextInt(max) + 1);
            }
        } else if (type.contains("varchar") || type.contains("char")) {
            // 从字段定义中提取长度
            int length = DEFAULT_STRING_LENGTH;
            if (type.contains("(")) {
                try {
                    length = Integer.parseInt(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
                } catch (Exception e) {
                    // 使用默认长度
                }
            }
            
            // 根据字段长度计算合适的生成数量
            int count = Math.min(DEFAULT_FOREIGN_KEY_COUNT, Math.max(3, length / 10));
            for (int i = 0; i < count; i++) {
                String uuid = UUID.randomUUID().toString();
                values.add(uuid.substring(0, Math.min(uuid.length(), length)));
            }
        } else if (type.contains("decimal") || type.contains("numeric") || 
                   type.contains("float") || type.contains("double")) {
            // 从类型定义中解析精度
            int precision = DEFAULT_PRECISION;
            int scale = DEFAULT_SCALE;
            if (type.contains("(")) {
                try {
                    String[] parts = type.substring(type.indexOf("(") + 1, type.indexOf(")")).split(",");
                    precision = Integer.parseInt(parts[0].trim());
                    if (parts.length > 1) {
                        scale = Integer.parseInt(parts[1].trim());
                    }
                } catch (Exception e) {
                    // 使用默认值
                }
            }
            
            double max = Math.pow(10, precision - scale) - Math.pow(10, -scale);
            int count = Math.min(10, precision);
            for (int i = 0; i < count; i++) {
                double value = new Random().nextDouble() * max;
                values.add(Math.round(value * Math.pow(10, scale)) / Math.pow(10, scale));
            }
        } else {
            // 对于其他类型，生成少量通用值
            values.add(generateRandomValue(type));
        }
        
        return values;
    }

    /**
     * 生成随机值
     */
    private Object generateRandomValue(String dataType) {
        String type = dataType.toLowerCase();
        
        if (type.contains("date")) {
            return new java.sql.Date(System.currentTimeMillis());
        } else if (type.contains("time")) {
            return new Time(System.currentTimeMillis());
        } else if (type.contains("bool")) {
            return new Random().nextBoolean();
        } else if (dataType.equals("enum")) {
            // 枚举类型在调用处已经处理
            return 1;
        } else {
            return UUID.randomUUID().toString();
        }
    }

    /**
     * 格式化SQL值
     */
    private String formatSqlValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof Number) {
            return value.toString();
        } else {
            return "'" + value.toString().replace("'", "''") + "'";
        }
    }

    /**
     * 验证和更新数据模板
     */
    private String validateAndUpdateTemplate(String originalTemplate, TableMetadata tableMetadata, DataSource dataSource) {
        try {
            log.debug("开始验证和更新模板，表：{}，原始模板：{}", tableMetadata.getTableName(), originalTemplate);
            
            // 创建一个新的模板节点
            ObjectNode finalTemplate = objectMapper.createObjectNode();
            
            // 解析原始模板（如果存在）
            JsonNode templateNode = null;
            if (originalTemplate != null && !originalTemplate.trim().isEmpty()) {
                try {
                    templateNode = objectMapper.readTree(originalTemplate);
                    log.debug("成功解析原始模板：{}", templateNode);
                } catch (Exception e) {
                    log.warn("解析原始模板失败，将使用空模板：{}", e.getMessage());
                    templateNode = objectMapper.createObjectNode();
                }
            } else {
                log.debug("原始模板为空，将使用空模板");
                templateNode = objectMapper.createObjectNode();
            }
            
            // 检查表元数据
            if (tableMetadata == null) {
                throw new IllegalArgumentException("表元数据不能为空");
            }
            if (tableMetadata.getColumns() == null) {
                throw new IllegalArgumentException("表列信息不能为空");
            }
            
            // 只处理表中实际存在的字段
            for (Map.Entry<String, ColumnMetadata> entry : tableMetadata.getColumns().entrySet()) {
                String columnName = entry.getKey();
                ColumnMetadata column = entry.getValue();
                
                try {
                    // 跳过自增主键
                    if (columnName.equals(tableMetadata.getPrimaryKeyColumn()) && tableMetadata.isAutoIncrement()) {
                        log.debug("跳过自增主键字段：{}", columnName);
                        continue;
                    }
                    
                    // 如果原模板中有该字段的定义，则使用原定义
                    if (templateNode.has(columnName)) {
                        log.debug("使用原模板中的字段定义：{} -> {}", columnName, templateNode.get(columnName));
                        finalTemplate.set(columnName, templateNode.get(columnName));
                    } else {
                        // 否则生成默认模板
                        log.debug("为字段 {} 生成默认模板", columnName);
                        JsonNode fieldTemplate = generateDefaultFieldTemplate(column, tableMetadata, dataSource);
                        finalTemplate.set(columnName, fieldTemplate);
                        log.debug("字段 {} 的默认模板: {}", columnName, fieldTemplate);
                    }
                } catch (Exception e) {
                    log.error("处理字段 {} 时发生错误：{}", columnName, e.getMessage());
                    throw new RuntimeException("处理字段 " + columnName + " 时发生错误：" + e.getMessage());
                }
            }
            
            String template = objectMapper.writeValueAsString(finalTemplate);
            log.debug("最终生成的模板: {}", template);
            return template;
            
        } catch (Exception e) {
            log.error("验证模板失败：{}", e.getMessage(), e);
            throw new RuntimeException("验证模板失败：" + e.getMessage(), e);
        }
    }

    /**
     * 根据字段元数据获取字段的最大长度限制
     */
    private FieldLengthLimit getFieldLengthLimit(ColumnMetadata column) {
        FieldLengthLimit limit = new FieldLengthLimit();
        String dataType = column.getDataType().toLowerCase();
        
        // 解析数据类型中的长度信息，如 varchar(255)
        Pattern pattern = Pattern.compile("\\((\\d+)(?:,(\\d+))?\\)");
        Matcher matcher = pattern.matcher(dataType);
        
        if (matcher.find()) {
            limit.setMaxLength(Long.parseLong(matcher.group(1)));
            if (matcher.group(2) != null) {
                limit.setDecimalPlaces(Integer.parseInt(matcher.group(2)));
            }
        } else {
            // 设置默认长度限制
            if (dataType.contains("tinyint")) {
                limit.setMaxLength(3);
            } else if (dataType.contains("smallint")) {
                limit.setMaxLength(5);
            } else if (dataType.contains("mediumint")) {
                limit.setMaxLength(7);
            } else if (dataType.contains("int")) {
                limit.setMaxLength(10);
            } else if (dataType.contains("bigint")) {
                limit.setMaxLength(19);
            } else if (dataType.contains("char")) {
                limit.setMaxLength(255);
            } else if (dataType.contains("text")) {
                limit.setMaxLength(65535);
            } else if (dataType.contains("datetime") || dataType.contains("timestamp")) {
                limit.setMaxLength(19); // yyyy-MM-dd HH:mm:ss
                limit.setPattern(DATETIME_FORMAT);
            } else if (dataType.contains("date")) {
                limit.setMaxLength(10); // yyyy-MM-dd
                limit.setPattern(DATE_FORMAT);
            } else if (dataType.contains("time")) {
                limit.setMaxLength(8); // HH:mm:ss
                limit.setPattern(TIME_FORMAT);
            }
        }
        
        // 设置数值类型的范围限制
        if (dataType.contains("int") || dataType.contains("bit")) {
            if (dataType.contains("tinyint")) {
                limit.setMaxValue(dataType.contains("unsigned") ? 255 : 127);
            } else if (dataType.contains("smallint")) {
                limit.setMaxValue(dataType.contains("unsigned") ? 65535 : 32767);
            } else if (dataType.contains("mediumint")) {
                limit.setMaxValue(dataType.contains("unsigned") ? 16777215 : 8388607);
            } else if (dataType.contains("bigint")) {
                limit.setMaxValue(dataType.contains("unsigned") ? Long.MAX_VALUE : Long.MAX_VALUE / 2);
            } else {
                limit.setMaxValue(dataType.contains("unsigned") ? Integer.MAX_VALUE : Integer.MAX_VALUE / 2);
            }
        } else if (dataType.contains("decimal") || dataType.contains("numeric")) {
            // 设置精度和小数位数
            if (matcher.find()) {
                limit.setPrecision(Integer.parseInt(matcher.group(1)));
                if (matcher.group(2) != null) {
                    limit.setScale(Integer.parseInt(matcher.group(2)));
                }
            }
            limit.setMaxValue(Math.pow(10, limit.getPrecision() - limit.getScale()) - Math.pow(10, -limit.getScale()));
        }
        
        return limit;
    }

    /**
     * 字段长度限制类
     */
    private static class FieldLengthLimit {
        private long maxLength = 255; // 默认最大长度
        private int decimalPlaces = 0; // 小数位数
        private double maxValue = Double.MAX_VALUE;
        private int precision = 10;
        private int scale = 2;
        private String pattern;
        
        public long getMaxLength() {
            return maxLength;
        }
        
        public void setMaxLength(long maxLength) {
            this.maxLength = maxLength;
        }
        
        public int getDecimalPlaces() {
            return decimalPlaces;
        }
        
        public void setDecimalPlaces(int decimalPlaces) {
            this.decimalPlaces = decimalPlaces;
        }

        public double getMaxValue() {
            return maxValue;
        }

        public void setMaxValue(double maxValue) {
            this.maxValue = maxValue;
        }

        public int getPrecision() {
            return precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public int getScale() {
            return scale;
        }

        public void setScale(int scale) {
            this.scale = scale;
        }

        public String getPattern() {
            return pattern;
        }

        public void setPattern(String pattern) {
            this.pattern = pattern;
        }
    }

    /**
     * 生成默认的字段模板
     */
    private JsonNode generateDefaultFieldTemplate(ColumnMetadata column, TableMetadata tableMetadata, DataSource dataSource) {
        ObjectNode fieldNode = objectMapper.createObjectNode();
        ObjectNode paramsNode = objectMapper.createObjectNode();
        
        String dataType = column.getDataType().toLowerCase();
        String comment = column.getComment();
        
        // 获取字段长度限制
        FieldLengthLimit lengthLimit = getFieldLengthLimit(column);
        
        // 处理MySQL的enum类型
        if (dataType.contains("enum")) {
            fieldNode.put(FIELD_TYPE, "enum");
            ArrayNode valuesNode = objectMapper.createArrayNode();
            
            // 获取enum类型的允许值
            List<String> enumValues = getEnumAllowedValues(column);
            
            // 如果仍然没有枚举值，使用默认值
            if (enumValues.isEmpty()) {
                enumValues.add("default");
            }
            
            // 添加枚举值到模板
            for (String value : enumValues) {
                valuesNode.add(value);
            }
            
            paramsNode.set(FIELD_VALUES, valuesNode);
            fieldNode.set(FIELD_PARAMS, paramsNode);
            return fieldNode;
        }
        
        // 如果字段不允许为空，设置默认值
        if (!column.isNullable()) {
            // 如果有默认值，优先使用默认值
            if (column.getDefaultValue() != null && !column.getDefaultValue().equals("NULL")) {
                // 对于CURRENT_TIMESTAMP这样的特殊默认值，使用date类型而不是constant
                if (CURRENT_TIMESTAMP.equals(column.getDefaultValue())) {
                    fieldNode.put(FIELD_TYPE, "date");
                    paramsNode.put(FIELD_FORMAT, DATETIME_FORMAT);
                    paramsNode.put("useCurrentTimestamp", true);
                } else {
                    fieldNode.put(FIELD_TYPE, "fixed");  // 使用fixed替代constant
                    paramsNode.put("value", column.getDefaultValue());
                }
                fieldNode.set(FIELD_PARAMS, paramsNode);
                return fieldNode;
            }
            
            // 处理注释中的枚举值
            if (comment != null && comment.contains(":")) {
                List<String> enumValues = parseEnumValuesFromComment(comment);
                if (!enumValues.isEmpty()) {
                    // 验证枚举值是否符合长度限制
                    List<String> validEnumValues = enumValues.stream()
                        .filter(value -> value.length() <= lengthLimit.getMaxLength())
                        .collect(Collectors.toList());
                    
                    if (!validEnumValues.isEmpty()) {
                        fieldNode.put(FIELD_TYPE, "enum");
                        ArrayNode valuesNode = objectMapper.createArrayNode();
                        validEnumValues.forEach(valuesNode::add);
                        paramsNode.set(FIELD_VALUES, valuesNode);
                        fieldNode.set(FIELD_PARAMS, paramsNode);
                        return fieldNode;
                    }
                }
            }
        }
        
        // 根据数据类型生成模板
        if (dataType.contains(TYPE_CHAR) || dataType.contains("varchar")) {
            // 对于字符类型，根据长度限制选择合适的生成策略
            if (lengthLimit.getMaxLength() <= 2) {
                // 对于很短的字段，使用1-9的数字字符串
                fieldNode.put(FIELD_TYPE, "enum");
                ArrayNode valuesNode = objectMapper.createArrayNode();
                for (int i = 1; i <= 9; i++) {
                    valuesNode.add(String.valueOf(i));
                }
                paramsNode.set(FIELD_VALUES, valuesNode);
            } else if (lengthLimit.getMaxLength() <= 5) {
                // 对于较短的字段，使用数字字符串
                fieldNode.put(FIELD_TYPE, "string");
                paramsNode.put(FIELD_PATTERN, "${number}");
            } else if (lengthLimit.getMaxLength() <= 10) {
                // 对于中等长度字段，使用短单词
                fieldNode.put(FIELD_TYPE, "string");
                paramsNode.put(FIELD_PATTERN, "${shortString}");
            } else {
                // 对于较长字段，根据是否唯一选择不同的模式
                fieldNode.put(FIELD_TYPE, "string");
                if (tableMetadata.getUniqueColumns().contains(column.getName())) {
                    paramsNode.put(FIELD_PATTERN, lengthLimit.getMaxLength() >= 32 ? PATTERN_UUID : PATTERN_NUMBER);
                } else {
                    paramsNode.put(FIELD_PATTERN, inferPatternByType(dataType, (int)lengthLimit.getMaxLength()));
                }
            }
            
            // 设置长度限制
            paramsNode.put(FIELD_LENGTH, lengthLimit.getMaxLength());
            
            // 如果字段不允许为空，设置默认值
            if (!column.isNullable()) {
                paramsNode.put("defaultValue", generateDefaultStringValue(lengthLimit.getMaxLength()));
            }
        }
        else if (dataType.contains(TYPE_INT) || dataType.contains(TYPE_BIT)) {
            fieldNode.put(FIELD_TYPE, "random");
            paramsNode.put(FIELD_INTEGER, true);
            paramsNode.put(FIELD_MIN, 1); // 确保非空字段至少为1
            paramsNode.put(FIELD_MAX, lengthLimit.getMaxValue());
        }
        else if (dataType.contains(TYPE_DECIMAL) || dataType.contains(TYPE_NUMERIC) ||
                 dataType.contains(TYPE_FLOAT) || dataType.contains(TYPE_DOUBLE)) {
            fieldNode.put(FIELD_TYPE, "random");
            paramsNode.put(FIELD_MIN, 1.0); // 确保非空字段至少为1.0
            paramsNode.put(FIELD_MAX, lengthLimit.getMaxValue());
            paramsNode.put(FIELD_PRECISION, lengthLimit.getScale());
        }
        else if (dataType.contains(TYPE_DATETIME) || dataType.contains(TYPE_TIMESTAMP) ||
                 dataType.contains(TYPE_DATE) || dataType.contains(TYPE_TIME)) {
            fieldNode.put(FIELD_TYPE, "date");
            paramsNode.put(FIELD_FORMAT, lengthLimit.getPattern());
            // 设置时间范围
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.YEAR, -1);
            paramsNode.put(FIELD_MIN, new SimpleDateFormat(lengthLimit.getPattern()).format(cal.getTime()));
            cal.add(Calendar.YEAR, 2);
            paramsNode.put(FIELD_MAX, new SimpleDateFormat(lengthLimit.getPattern()).format(cal.getTime()));
        }
        else if (dataType.equals(TYPE_BOOLEAN)) {
            fieldNode.put(FIELD_TYPE, "random");
            ArrayNode valuesNode = objectMapper.createArrayNode();
            valuesNode.add(true).add(false);
            paramsNode.set(FIELD_VALUES, valuesNode);
        }
        else {
            // 默认作为字符串处理
            fieldNode.put(FIELD_TYPE, "string");
            paramsNode.put(FIELD_PATTERN, PATTERN_UUID);
            // 如果字段不允许为空，设置默认值
            if (!column.isNullable()) {
                paramsNode.put("defaultValue", "default");
            }
        }
        
        fieldNode.set(FIELD_PARAMS, paramsNode);
        return fieldNode;
    }

    /**
     * 生成默认字符串值
     */
    private String generateDefaultStringValue(long maxLength) {
        if (maxLength <= 2) {
            return "1";
        } else if (maxLength <= 5) {
            return "12345".substring(0, (int)maxLength);
        } else {
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
            return "def_" + uuid.substring(0, Math.min(uuid.length(), (int)maxLength - 4));
        }
    }

    /**
     * 从注释中解析枚举值
     */
    private List<String> parseEnumValuesFromComment(String comment) {
        List<String> enumValues = new ArrayList<>();
        if (comment == null || comment.trim().isEmpty()) {
            return enumValues;
        }

        // 查找枚举值格式 [value1,value2,value3]
        Pattern pattern = Pattern.compile("\\[(.*?)\\]");
        Matcher matcher = pattern.matcher(comment);
        
        if (matcher.find()) {
            String[] values = matcher.group(1).split(",");
            for (String value : values) {
                String trimmedValue = value.trim();
                if (!trimmedValue.isEmpty()) {
                    enumValues.add(trimmedValue);
                }
            }
        }
        
        return enumValues;
    }

    /**
     * 根据字段类型和长度推断生成模式
     */
    private String inferPatternByType(String columnType, int maxLength) {
        if (maxLength <= 2) {
            return "${number}";  // 1-9的数字
        } else if (maxLength <= 5) {
            return "${number}";  // 多位数字
        } else if (maxLength <= 10) {
            return "${shortString}";  // 短字符串
        } else if (maxLength <= 20) {
            return "${mediumString}";  // 中等长度字符串
        } else if (maxLength <= 50) {
            return "${longString}";  // 长字符串
        } else {
            return "${text}";  // 文本
        }
    }

    /**
     * 处理生成的数据
     */
    private List<Map<String, Object>> processGeneratedData(List<Map<String, Object>> rawData,
                                                         TableMetadata tableMetadata,
                                                         Set<Object> existingPrimaryKeys,
                                                         Map<String, Set<Object>> existingForeignKeys) {
        return processGeneratedData(rawData, tableMetadata, existingPrimaryKeys, existingForeignKeys, null);
    }

    /**
     * 处理生成的数据（带主键最大值）
     */
    private List<Map<String, Object>> processGeneratedData(List<Map<String, Object>> rawData,
                                                         TableMetadata tableMetadata,
                                                         Set<Object> existingPrimaryKeys,
                                                         Map<String, Set<Object>> existingForeignKeys,
                                                         Object maxPrimaryKey) {
        List<Map<String, Object>> processedData = new ArrayList<>();
        Set<Object> generatedPrimaryKeys = new HashSet<>();
        Map<String, Set<Object>> uniqueValues = new HashMap<>();
        
        // 初始化唯一值集合
        for (String uniqueColumn : tableMetadata.getUniqueColumns()) {
            uniqueValues.put(uniqueColumn, new HashSet<>());
        }
        
        // 获取主键列信息
        String primaryKeyColumn = tableMetadata.getPrimaryKeyColumn();
        ColumnMetadata primaryKeyMetadata = primaryKeyColumn != null ? 
            tableMetadata.getColumns().get(primaryKeyColumn) : null;
            
        for (Map<String, Object> row : rawData) {
            Map<String, Object> processedRow = new HashMap<>();
            boolean skipRow = false;
            
            // 处理每一列的数据
            for (Map.Entry<String, ColumnMetadata> entry : tableMetadata.getColumns().entrySet()) {
                String columnName = entry.getKey();
                ColumnMetadata columnMetadata = entry.getValue();
                Object value = row.get(columnName);
                
                try {
                    // 1. 检查非空约束
                    if (value == null && !columnMetadata.isNullable()) {
                        // 如果是自增主键，跳过
                        if (columnName.equals(primaryKeyColumn) && tableMetadata.isAutoIncrement()) {
                            continue;
                        }
                        value = generateDefaultValue(columnMetadata);
                        if (value == null) {
                            log.error("无法为非空字段 {} 生成有效的默认值", columnName);
                            skipRow = true;
                            break;
                        }
                    }
                    
                    // 2. 处理主键
                    if (columnName.equals(primaryKeyColumn)) {
                        value = processKeyValue(value, primaryKeyMetadata, existingPrimaryKeys, 
                            generatedPrimaryKeys, maxPrimaryKey);
                    }
                    
                    // 3. 处理外键
                    if (existingForeignKeys != null && existingForeignKeys.containsKey(columnName)) {
                        value = processForeignKeyValue(value, columnMetadata, 
                            existingForeignKeys.get(columnName));
                        if (value == null && !columnMetadata.isNullable()) {
                            log.error("无法为外键字段 {} 找到有效的引用值", columnName);
                            skipRow = true;
                            break;
                        }
                    }
                    
                    // 4. 处理默认值
                    if (value == null && columnMetadata.getDefaultValue() != null && 
                        !columnMetadata.getDefaultValue().equals("NULL")) {
                        value = columnMetadata.getDefaultValue();
                    }
                    
                    // 5. 转换数据类型并验证
                    if (value != null) {
                        try {
                            // 特殊处理enum类型
                            if (columnMetadata.getDataType().toLowerCase().contains("enum")) {
                                // 获取enum类型的允许值
                                List<String> allowedValues = getEnumAllowedValues(
                                    columnMetadata, tableMetadata.getTableName(), tableMetadata.getDataSource());
                                
                                // 检查值是否在允许范围内
                                String strValue = value.toString();
                                boolean valueAllowed = false;
                                
                                for (String allowedValue : allowedValues) {
                                    if (strValue.equalsIgnoreCase(allowedValue)) {
                                        value = allowedValue; // 使用正确的大小写
                                        valueAllowed = true;
                                        break;
                                    }
                                }
                                
                                // 如果值不在允许范围内，使用第一个允许的值
                                if (!valueAllowed && !allowedValues.isEmpty()) {
                                    log.warn("字段 {} 的枚举值 {} 不在允许的值 {} 范围内，将使用第一个允许的值", 
                                        columnName, strValue, allowedValues);
                                    value = allowedValues.get(0);
                                }
                            } else {
                                value = convertToCorrectType(value, columnMetadata);
                            }
                            
                            // 验证长度限制
                            FieldLengthLimit lengthLimit = getFieldLengthLimit(columnMetadata);
                            if (!validateValueLength(value, lengthLimit)) {
                                // 修改：不跳过整行，而是调整值使其符合长度限制
                                Object adjustedValue = adjustValueToFitLength(value, columnMetadata, lengthLimit);
                                log.warn("字段 {} 的值 {} 超出长度限制 {}，已调整为: {}", 
                                    columnName, value, lengthLimit.getMaxLength(), adjustedValue);
                                value = adjustedValue;
                            }
                            
                            // 验证数值范围
                            if (value instanceof Number && !validateNumberRange(value, lengthLimit)) {
                                // 修改：不跳过整行，而是调整值使其符合范围限制
                                Object adjustedValue = adjustNumberToFitRange(value, columnMetadata, lengthLimit);
                                log.warn("字段 {} 的值 {} 超出范围限制，已调整为: {}", 
                                    columnName, value, adjustedValue);
                                value = adjustedValue;
                            }
                        } catch (Exception e) {
                            log.error("处理字段 {} 的值 {} 时发生错误: {}", columnName, value, e.getMessage());
                            value = generateDefaultValue(columnMetadata);
                            log.info("已为字段 {} 生成默认值: {}", columnName, value);
                        }
                    }
                    
                    // 6. 处理唯一约束
                    if (tableMetadata.getUniqueColumns().contains(columnName)) {
                        Set<Object> existingUniqueValues = uniqueValues.get(columnName);
                        if (value != null && !existingUniqueValues.add(value)) {
                            // 尝试生成新的唯一值
                            value = generateUniqueValue(columnMetadata, existingUniqueValues);
                            if (value == null) {
                                log.error("无法为唯一字段 {} 生成有效的值", columnName);
                                skipRow = true;
                                break;
                            }
                            existingUniqueValues.add(value);
                        }
                    }
                    
                    processedRow.put(columnName, value);
                    
                } catch (Exception e) {
                    log.error("处理字段 {} 时发生错误: {}", columnName, e.getMessage());
                    skipRow = true;
                    break;
                }
            }
            
            if (!skipRow) {
                processedData.add(processedRow);
            }
        }
        
        return processedData;
    }

    /**
     * 验证值的长度是否符合限制
     */
    private boolean validateValueLength(Object value, FieldLengthLimit limit) {
        if (value == null) return true;
        
        String strValue = value.toString();
        if (value instanceof Number) {
            // 对于数值类型，检查整数位和小数位的长度
            if (value instanceof Double || value instanceof Float) {
                String[] parts = strValue.split("\\.");
                int integerLength = parts[0].length();
                int decimalLength = parts.length > 1 ? parts[1].length() : 0;
                return integerLength <= (limit.getPrecision() - limit.getScale()) &&
                       decimalLength <= limit.getScale();
            }
            // 对于整数类型，检查数字长度
            return strValue.length() <= limit.getMaxLength();
        }
        // 对于字符串类型，直接检查长度
        return strValue.length() <= limit.getMaxLength();
    }

    /**
     * 验证数值是否在范围内
     */
    private boolean validateNumberRange(Object value, FieldLengthLimit limit) {
        if (value == null) return true;
        
        if (value instanceof Number) {
            double doubleValue = ((Number) value).doubleValue();
            return doubleValue <= limit.getMaxValue() && doubleValue >= -limit.getMaxValue();
        }
        return true;
    }

    /**
     * 生成唯一值
     */
    private Object generateUniqueValue(ColumnMetadata metadata, Set<Object> existingValues) {
        String dataType = metadata.getDataType().toLowerCase();
        int maxAttempts = 100; // 最大尝试次数
        
        for (int i = 0; i < maxAttempts; i++) {
            Object value = generateDefaultValue(metadata);
            if (value != null && !existingValues.contains(value)) {
                return value;
            }
        }
        
        return null;
    }

    /**
     * 插入数据到表中
     */
    private void insertDataIntoTable(DataSource dataSource, TableMetadata tableMetadata,
                                   List<Map<String, Object>> data, String writeMode) throws SQLException {
        if (data.isEmpty()) {
            log.warn("没有数据需要插入到表 {}", tableMetadata.getTableName());
            return;
        }
        
        // 获取第一行数据，用于构建SQL
        Map<String, Object> firstRow = data.get(0);
        
        try (Connection conn = getConnection(dataSource)) {
            // 在覆盖模式下清空表，需要先禁用外键约束
            if ("OVERWRITE".equals(writeMode)) {
                try (Statement stmt = conn.createStatement()) {
                    // 禁用外键约束检查
                    stmt.execute("SET FOREIGN_KEY_CHECKS = 0");
                    stmt.execute("TRUNCATE TABLE " + tableMetadata.getTableName());
                    // 重新启用外键约束检查
                    stmt.execute("SET FOREIGN_KEY_CHECKS = 1");
                }
            }
            
            // 构建SQL语句
            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            
            for (String column : firstRow.keySet()) {
                if (columns.length() > 0) {
                    columns.append(", ");
                    placeholders.append(", ");
                }
                columns.append(column);
                placeholders.append("?");
            }
            
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableMetadata.getTableName(), columns, placeholders);
            
            log.debug("插入数据SQL: {}", sql);
            
            // 特殊处理enum类型的字段
            Map<String, List<String>> enumColumns = new HashMap<>();
            for (Map.Entry<String, ColumnMetadata> entry : tableMetadata.getColumns().entrySet()) {
                String columnName = entry.getKey();
                ColumnMetadata columnMetadata = entry.getValue();
                
                if (columnMetadata.getDataType().toLowerCase().contains("enum")) {
                    // 获取enum类型的允许值
                    List<String> allowedValues = getEnumAllowedValues(
                        columnMetadata, tableMetadata.getTableName(), dataSource);
                    
                    if (!allowedValues.isEmpty()) {
                        enumColumns.put(columnName, allowedValues);
                        log.info("表 {} 的enum字段 {} 允许的值: {}", 
                            tableMetadata.getTableName(), columnName, allowedValues);
                    }
                }
            }
            
            // 批量插入数据
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int batchCount = 0;
                
                // 使用第一行的列顺序
                List<String> columnList = new ArrayList<>(firstRow.keySet());
                
                for (Map<String, Object> row : data) {
                    int paramIndex = 1;
                    
                    for (String column : columnList) {
                        Object value = row.get(column);
                        
                        // 特殊处理enum类型
                        if (enumColumns.containsKey(column) && value != null) {
                            String strValue = value.toString();
                            List<String> allowedValues = enumColumns.get(column);
                            boolean valueAllowed = false;
                            
                            for (String allowedValue : allowedValues) {
                                if (strValue.equalsIgnoreCase(allowedValue)) {
                                    // 使用正确的大小写
                                    value = allowedValue;
                                    valueAllowed = true;
                                    break;
                                }
                            }
                            
                            // 如果值不在允许范围内，使用第一个允许的值
                            if (!valueAllowed) {
                                log.warn("插入数据时，表 {} 字段 {} 的枚举值 {} 不在允许的值 {} 范围内，将使用第一个允许的值", 
                                    tableMetadata.getTableName(), column, strValue, allowedValues);
                                value = allowedValues.get(0);
                            }
                        }
                        
                        pstmt.setObject(paramIndex++, value);
                    }
                    
                    pstmt.addBatch();
                    batchCount++;
                    
                    // 每1000条数据执行一次批处理
                    if (batchCount % 1000 == 0) {
                        pstmt.executeBatch();
                        log.debug("已插入 {} 条数据", batchCount);
                    }
                }
                
                // 执行剩余的批处理
                if (batchCount % 1000 != 0) {
                    pstmt.executeBatch();
                }
                
                log.info("成功插入 {} 条数据到表 {}", data.size(), tableMetadata.getTableName());
            }
        }
    }

    /**
     * 获取数据库连接
     */
    private Connection getConnection(DataSource dataSource) throws SQLException {
        return DriverManager.getConnection(
            dataSource.getUrl(),
            dataSource.getUsername(),
            dataSource.getPassword()
        );
    }

    /**
     * 转换数据类型
     */
    private Object convertToCorrectType(Object value, ColumnMetadata metadata) {
        if (value == null) return null;
        
        String type = metadata.getDataType().toLowerCase();
        try {
            if (type.contains("varchar") || type.contains("char")) {
                String strValue = value.toString();
                
                // 如果值仍然包含模板标记，说明模板没有被正确处理
                if (strValue.contains("${")) {
                    log.warn("检测到未处理的模板标记: {}, 将使用默认值", strValue);
                    strValue = "1";  // 使用安全的默认值
                }
                
                // 从类型定义中提取长度
                int maxLength = DEFAULT_STRING_LENGTH;
                if (metadata.getMaxLength() != null) {
                    try {
                        maxLength = Integer.parseInt(metadata.getMaxLength());
                    } catch (Exception e) {
                        log.warn("无法解析字符串长度限制，使用默认值: {}", DEFAULT_STRING_LENGTH);
                    }
                }
                
                // 如果是数值，确保转换为字符串
                if (value instanceof Number) {
                    strValue = String.valueOf(((Number)value).intValue());
                }
                
                // 确保不超过最大长度
                if (strValue.length() > maxLength) {
                    log.warn("字符串值 {} 超过最大长度 {}，将被截断", strValue, maxLength);
                }
                return strValue.substring(0, Math.min(strValue.length(), maxLength));
            }
            else if (type.contains("enum")) {
                // 处理MySQL的enum类型
                String strValue = value.toString();
                
                // 获取enum类型的允许值
                List<String> allowedValues = getEnumAllowedValues(metadata);
                
                // 如果有允许的值，检查当前值是否在允许范围内
                if (!allowedValues.isEmpty()) {
                    boolean valueAllowed = false;
                    for (String allowedValue : allowedValues) {
                        if (strValue.equalsIgnoreCase(allowedValue)) {
                            valueAllowed = true;
                            // 使用正确的大小写
                            strValue = allowedValue;
                            break;
                        }
                    }
                    
                    if (!valueAllowed) {
                        log.warn("枚举值 {} 不在允许的值 {} 范围内，将使用第一个允许的值", strValue, allowedValues);
                        strValue = allowedValues.get(0);
                    }
                }
                
                return strValue;
            }
            else if (type.contains("int") || type.contains("bit")) {
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.parseLong(value.toString());
            } else if (type.contains("decimal") || type.contains("numeric") || 
                      type.contains("float") || type.contains("double")) {
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.parseDouble(value.toString());
            } else if (type.contains("date")) {
                if (value instanceof java.util.Date) {
                    return new java.sql.Date(((java.util.Date) value).getTime());
                }
                return java.sql.Date.valueOf(value.toString());
            } else if (type.contains("timestamp") || type.contains("datetime")) {
                if (value instanceof java.util.Date) {
                    return new Timestamp(((java.util.Date) value).getTime());
                }
                return Timestamp.valueOf(value.toString());
            } else if (type.contains("time")) {
                if (value instanceof java.util.Date) {
                    return new Time(((java.util.Date) value).getTime());
                }
                return Time.valueOf(value.toString());
            } else if (type.contains("bool")) {
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            }
            // 其他类型直接转为字符串
            return value.toString();
        } catch (Exception e) {
            log.error("类型转换失败: {} -> {}, 错误: {}", value, type, e.getMessage());
            throw new IllegalArgumentException("无法将值 " + value + " 转换为 " + type + " 类型");
        }
    }

    /**
     * 生成默认值
     */
    private Object generateDefaultValue(ColumnMetadata column) {
        String dataType = column.getDataType().toLowerCase();
        
        // 优先使用字段默认值
        if (column.getDefaultValue() != null && !column.getDefaultValue().equals("NULL")) {
            return column.getDefaultValue();
        }
        
        // 从注释中提取枚举值
        List<String> enumValues = parseEnumValuesFromComment(column.getComment());
        if (!enumValues.isEmpty()) {
            return enumValues.get(0); // 使用第一个枚举值作为默认值
        }
        
        // 获取字段长度限制
        FieldLengthLimit lengthLimit = getFieldLengthLimit(column);
        
        // 根据数据类型生成默认值
        if (dataType.contains("char") || dataType.contains("varchar")) {
            if (lengthLimit.getMaxLength() <= 2) {
                return "1";
            } else if (lengthLimit.getMaxLength() <= 5) {
                return "12345".substring(0, (int)lengthLimit.getMaxLength());
            } else {
                return "def_" + UUID.randomUUID().toString().substring(0, 
                    Math.min(32, (int)lengthLimit.getMaxLength() - 4));
            }
        } else if (dataType.contains("int") || dataType.contains("bit")) {
            if (dataType.contains("tinyint")) {
                return 1;
            } else if (dataType.contains("smallint")) {
                return 1;
            } else if (dataType.contains("mediumint")) {
                return 1;
            } else if (dataType.contains("bigint")) {
                return 1L;
            } else {
                return 1;
            }
        } else if (dataType.contains("decimal") || dataType.contains("numeric") || 
                   dataType.contains("float") || dataType.contains("double")) {
            return 1.0;
        } else if (dataType.contains("date")) {
            return new java.sql.Date(System.currentTimeMillis());
        } else if (dataType.contains("time")) {
            return new Time(System.currentTimeMillis());
        } else if (dataType.contains("timestamp") || dataType.contains("datetime")) {
            return new Timestamp(System.currentTimeMillis());
        } else if (dataType.contains("bool")) {
            return true;
        } else {
            return "default";
        }
    }

    /**
     * 处理主键值
     */
    private Object processKeyValue(Object value, ColumnMetadata metadata,
                                 Set<Object> existingKeys, Set<Object> generatedKeys) {
        return processKeyValue(value, metadata, existingKeys, generatedKeys, null);
    }

    /**
     * 处理主键值（带最大值）
     */
    private Object processKeyValue(Object value, ColumnMetadata metadata,
                                 Set<Object> existingKeys, Set<Object> generatedKeys,
                                 Object maxPrimaryKey) {
        // 如果existingKeys为null，初始化为空集合
        if (existingKeys == null) {
            existingKeys = new HashSet<>();
        }
        
        // 如果generatedKeys为null，初始化为空集合
        if (generatedKeys == null) {
            generatedKeys = new HashSet<>();
        }
        
        // 检查是否是数值类型的主键
        boolean isNumericKey = metadata.getDataType().toLowerCase().contains("int") || 
                              metadata.getDataType().toLowerCase().contains("decimal") ||
                              metadata.getDataType().toLowerCase().contains("numeric") ||
                              metadata.getDataType().toLowerCase().contains("float") ||
                              metadata.getDataType().toLowerCase().contains("double");
        
        // 如果是数值类型的主键，并且有最大值，使用最大值+1作为起点
        if (isNumericKey && maxPrimaryKey != null) {
            try {
                long nextValue = Long.parseLong(maxPrimaryKey.toString()) + 1;
                log.info("使用主键最大值 {} 作为起点生成新主键", nextValue);
                value = nextValue + generatedKeys.size(); // 确保每个生成的主键都不同
                
                // 确保主键值唯一
                while (existingKeys.contains(value) || generatedKeys.contains(value)) {
                    value = (long)value + 1;
                }
                
                generatedKeys.add(value);
                return value;
            } catch (NumberFormatException e) {
                log.warn("无法将主键最大值 {} 转换为数值，将使用默认生成方式", maxPrimaryKey);
                // 继续使用默认生成方式
            }
        }
        
        // 如果是数值类型的主键，并且值小于10000，强制重新生成
        if (isNumericKey && value != null) {
            try {
                double numValue = Double.parseDouble(value.toString());
                if (numValue < 10000) {
                    log.warn("主键值 {} 太小，将重新生成", value);
                    value = null; // 强制重新生成
                }
            } catch (NumberFormatException e) {
                // 如果转换失败，忽略错误，继续使用原值
                log.warn("无法将主键值 {} 转换为数值", value);
            }
        }
        
        if (value == null) {
            value = generateUniqueKey(metadata, existingKeys, generatedKeys);
        } else {
            // 确保主键值唯一
            while (existingKeys.contains(value) || generatedKeys.contains(value)) {
                log.warn("主键值 {} 已存在，将重新生成", value);
                value = generateUniqueKey(metadata, existingKeys, generatedKeys);
            }
        }
        
        generatedKeys.add(value);
        return value;
    }

    /**
     * 生成唯一主键值
     */
    private Object generateUniqueKey(ColumnMetadata metadata, 
                                   Set<Object> existingKeys, 
                                   Set<Object> generatedKeys) {
        Object value;
        int attempts = 0;
        final int MAX_ATTEMPTS = 1000; // 最大尝试次数，防止无限循环
        
        do {
            attempts++;
            // 根据数据类型生成主键值
            if (metadata.getDataType().toLowerCase().contains("int")) {
                // 避免使用小值作为主键，从10000开始生成
                value = 10000 + random.nextInt(Integer.MAX_VALUE - 10000);
            } else if (metadata.getDataType().toLowerCase().contains("bigint")) {
                // 避免使用小值作为主键，从10000开始生成
                value = 10000 + Math.abs(random.nextLong() % (Long.MAX_VALUE - 10000));
            } else {
                // 对于字符串类型的主键，生成UUID
                value = UUID.randomUUID().toString();
            }
            
            // 如果尝试次数过多，使用时间戳+随机数作为备选方案
            if (attempts > MAX_ATTEMPTS) {
                log.warn("生成唯一主键值尝试次数过多，使用备选方案");
                value = System.currentTimeMillis() + "_" + random.nextInt(1000000);
                break;
            }
        } while (existingKeys.contains(value) || generatedKeys.contains(value));
        
        return value;
    }

    /**
     * 处理外键值
     */
    private Object processForeignKeyValue(Object value, ColumnMetadata metadata,
                                        Set<Object> validValues) {
        if (validValues == null || validValues.isEmpty()) {
            log.warn("字段 {} 没有有效的外键引用值", metadata.getName());
            return null;
        }
        
        // 如果值不在有效值集合中，随机选择一个有效值
        if (value == null || !validValues.contains(value)) {
            List<Object> valuesList = new ArrayList<>(validValues);
            int randomIndex = random.nextInt(valuesList.size());
            Object newValue = valuesList.get(randomIndex);
            
            if (value != null) {
                log.debug("外键值 {} 不在有效值集合中，已替换为: {}", value, newValue);
            } else {
                log.debug("为空外键值随机选择有效值: {}", newValue);
            }
            
            value = newValue;
        } else {
            log.debug("使用有效的外键值: {}", value);
        }
        
        return value;
    }

    /**
     * 调整值使其符合长度限制
     */
    private Object adjustValueToFitLength(Object value, ColumnMetadata columnMetadata, FieldLengthLimit lengthLimit) {
        if (value == null) return null;
        
        String dataType = columnMetadata.getDataType().toLowerCase();
        String strValue = value.toString();
        
        // 处理字符串类型
        if (dataType.contains("char") || dataType.contains("varchar") || dataType.contains("text")) {
            if (strValue.length() > lengthLimit.getMaxLength()) {
                // 截断字符串
                return strValue.substring(0, (int)lengthLimit.getMaxLength());
            }
        } 
        // 处理数值类型
        else if (value instanceof Number) {
            if (dataType.contains("decimal") || dataType.contains("numeric")) {
                // 处理小数
                String[] parts = strValue.split("\\.");
                int integerLength = parts[0].length();
                int decimalLength = parts.length > 1 ? parts[1].length() : 0;
                
                // 如果整数部分超长，截断为最大值
                if (integerLength > (lengthLimit.getPrecision() - lengthLimit.getScale())) {
                    // 生成最大值 (例如: 999.99 对于 precision=5, scale=2)
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < (lengthLimit.getPrecision() - lengthLimit.getScale()); i++) {
                        sb.append('9');
                    }
                    sb.append('.');
                    for (int i = 0; i < lengthLimit.getScale(); i++) {
                        sb.append('9');
                    }
                    return Double.parseDouble(sb.toString());
                }
                
                // 如果小数部分超长，截断小数部分
                if (decimalLength > lengthLimit.getScale()) {
                    double truncated = Math.round(((Number)value).doubleValue() * Math.pow(10, lengthLimit.getScale())) 
                                     / Math.pow(10, lengthLimit.getScale());
                    return truncated;
                }
            } else {
                // 处理整数
                if (strValue.length() > lengthLimit.getMaxLength()) {
                    // 返回该类型的最大值
                    boolean isUnsigned = isUnsignedType(columnMetadata.getDataType());
                    if (dataType.contains("tinyint")) {
                        return isUnsigned ? 255 : 127;
                    } else if (dataType.contains("smallint")) {
                        return isUnsigned ? 65535 : 32767;
                    } else if (dataType.contains("mediumint")) {
                        return isUnsigned ? 16777215 : 8388607;
                    } else if (dataType.contains("int")) {
                        return isUnsigned ? 4294967295L : 2147483647;
                    } else if (dataType.contains("bigint")) {
                        return isUnsigned ? Long.MAX_VALUE : Long.MAX_VALUE / 2;
                    }
                }
            }
        }
        // 处理日期时间类型
        else if (dataType.contains("date") || dataType.contains("time")) {
            // 日期时间类型通常有固定格式，不需要截断
            return value;
        }
        
        // 如果没有特殊处理，返回原值
        return value;
    }

    /**
     * 调整数值使其符合范围限制
     */
    private Object adjustNumberToFitRange(Object value, ColumnMetadata columnMetadata, FieldLengthLimit lengthLimit) {
        if (!(value instanceof Number)) return value;
        
        String dataType = columnMetadata.getDataType().toLowerCase();
        double doubleValue = ((Number)value).doubleValue();
        
        // 根据数据类型设置最大值和最小值
        double maxValue = lengthLimit.getMaxValue();
        boolean isUnsigned = isUnsignedType(columnMetadata.getDataType());
        double minValue = isUnsigned ? 0 : -maxValue;
        
        // 调整值到范围内
        if (doubleValue > maxValue) {
            if (dataType.contains("int") || dataType.contains("bigint")) {
                return (long)maxValue;
            } else {
                return maxValue;
            }
        } else if (doubleValue < minValue) {
            if (dataType.contains("int") || dataType.contains("bigint")) {
                return (long)minValue;
            } else {
                return minValue;
            }
        }
        
        return value;
    }
    
    /**
     * 检查数据类型是否为无符号类型
     */
    private boolean isUnsignedType(String dataType) {
        if (dataType == null) return false;
        return dataType.toLowerCase().contains("unsigned");
    }

    /**
     * 获取MySQL enum类型的允许值
     */
    private List<String> getEnumAllowedValues(ColumnMetadata metadata) {
        List<String> allowedValues = new ArrayList<>();
        String dataType = metadata.getDataType().toLowerCase();
        
        // 从类型定义中提取枚举值，例如 enum('value1','value2')
        if (dataType.contains("enum") && dataType.contains("(")) {
            try {
                String enumDef = dataType.substring(dataType.indexOf("(") + 1, dataType.lastIndexOf(")"));
                String[] values = enumDef.split(",");
                for (String val : values) {
                    // 移除引号
                    String cleanVal = val.trim().replaceAll("^'|'$", "");
                    allowedValues.add(cleanVal);
                }
            } catch (Exception e) {
                log.warn("无法从类型定义 {} 中提取枚举值: {}", dataType, e.getMessage());
            }
        }
        
        // 如果从数据类型中没有提取到枚举值，尝试从注释中提取
        if (allowedValues.isEmpty() && metadata.getComment() != null) {
            List<String> commentValues = parseEnumValuesFromComment(metadata.getComment());
            allowedValues.addAll(commentValues);
        }
        
        // 如果仍然没有枚举值，尝试从数据库中查询
        if (allowedValues.isEmpty() && metadata.getName() != null) {
            // 这里需要表名，但ColumnMetadata没有表名信息
            // 只有在处理生成数据时，我们才能获取表名
            log.warn("无法从数据库中查询字段 {} 的枚举值，因为没有表名信息", metadata.getName());
        }
        
        // 记录找到的枚举值
        if (!allowedValues.isEmpty()) {
            log.info("字段 {} 的枚举允许值: {}", metadata.getName(), allowedValues);
        } else {
            log.warn("未能找到字段 {} 的枚举允许值", metadata.getName());
        }
        
        return allowedValues;
    }

    /**
     * 获取MySQL enum类型的允许值（带表名和数据源）
     */
    private List<String> getEnumAllowedValues(ColumnMetadata metadata, String tableName, DataSource dataSource) {
        List<String> allowedValues = new ArrayList<>();
        
        // 先从基本方法获取可能的枚举值
        allowedValues.addAll(getEnumAllowedValues(metadata));
        
        // 如果没有找到枚举值，并且有表名和数据源，尝试从数据库中查询
        if (allowedValues.isEmpty() && tableName != null && dataSource != null) {
            try {
                try (Connection conn = getConnection(dataSource);
                     Statement stmt = conn.createStatement()) {
                    
                    // 查询字段的类型信息
                    String sql = String.format("SHOW COLUMNS FROM %s WHERE Field = '%s'",
                        tableName, metadata.getName());
                    
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) {
                            String type = rs.getString("Type");
                            if (type != null && type.startsWith("enum(")) {
                                // 提取枚举值
                                String enumValues = type.substring(5, type.length() - 1);
                                String[] values = enumValues.split(",");
                                for (String val : values) {
                                    // 移除引号
                                    String cleanVal = val.trim().replaceAll("^'|'$", "");
                                    allowedValues.add(cleanVal);
                                }
                            }
                        }
                    }
                }
                
                // 记录找到的枚举值
                if (!allowedValues.isEmpty()) {
                    log.info("表 {} 字段 {} 的枚举允许值: {}", tableName, metadata.getName(), allowedValues);
                }
            } catch (Exception e) {
                log.warn("无法从数据库中查询枚举值: {}", e.getMessage());
            }
        }
        
        return allowedValues;
    }
} 