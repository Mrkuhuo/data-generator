package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.DataSource;
import com.datagenerator.mapper.DataSourceMapper;
import com.datagenerator.service.DataSourceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Duration;
import javax.annotation.PreDestroy;

@Slf4j
@Service
public class DataSourceServiceImpl extends ServiceImpl<DataSourceMapper, DataSource> implements DataSourceService {

    private final Map<Long, AdminClient> adminClientMap = new ConcurrentHashMap<>();

    @PreDestroy
    public void cleanup() {
        log.info("正在关闭所有Kafka AdminClient连接...");
        adminClientMap.forEach((id, client) -> {
            try {
                client.close(Duration.ofSeconds(5));
                log.info("成功关闭数据源ID={}的Kafka连接", id);
            } catch (Exception e) {
                log.error("关闭数据源ID={}的Kafka连接时发生错误", id, e);
            }
        });
        adminClientMap.clear();
    }

    private AdminClient getOrCreateAdminClient(DataSource dataSource) {
        Long dataSourceId = dataSource.getId();
        if (dataSourceId == null) {
            log.info("DataSource ID 不能为 null");
            // 对于未保存的数据源，使用hashCode作为临时标识
            dataSourceId = (long) dataSource.hashCode();
            log.info("使用临时ID进行连接测试: {}", dataSourceId);
        }
        return adminClientMap.computeIfAbsent(dataSourceId, id -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", dataSource.getUrl());
            props.put("request.timeout.ms", "5000");
            props.put("connections.max.idle.ms", "10000");
            
            if (dataSource.getUsername() != null && !dataSource.getUsername().isEmpty()) {
                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("sasl.mechanism", "PLAIN");
                String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
                String jaasConfig = String.format(jaasTemplate, dataSource.getUsername(), dataSource.getPassword());
                props.put("sasl.jaas.config", jaasConfig);
            }
            
            return AdminClient.create(props);
        });
    }

    public void closeKafkaConnection(Long dataSourceId) {
        AdminClient client = adminClientMap.remove(dataSourceId);
        if (client != null) {
            try {
                client.close(Duration.ofSeconds(5));
                log.info("成功关闭数据源ID={}的Kafka连接", dataSourceId);
            } catch (Exception e) {
                log.error("关闭数据源ID={}的Kafka连接时发生错误", dataSourceId, e);
            }
        }
    }

    @Override
    public void testConnection(DataSource dataSource) throws SQLException {
        log.info("开始测试数据源连接, 类型: {}, URL: {}", dataSource.getType(), dataSource.getUrl());
        
        if ("KAFKA".equalsIgnoreCase(dataSource.getType())) {
            log.info("正在测试Kafka连接...");
            try {
                AdminClient adminClient = getOrCreateAdminClient(dataSource);
                adminClient.listTopics(new ListTopicsOptions().timeoutMs(5000)).names().get();
                log.info("Kafka连接测试成功");
            } catch (Exception e) {
                log.error("Kafka连接测试失败", e);
                closeKafkaConnection(dataSource.getId());
                throw new SQLException("Kafka连接失败: " + e.getMessage());
            }
        } else {
            log.info("正在测试数据库连接...");
            try {
                log.info("加载数据库驱动: {}", dataSource.getDriverClassName());
                Class.forName(dataSource.getDriverClassName());
                log.info("尝试建立数据库连接...");
                Connection conn = DriverManager.getConnection(
                    dataSource.getUrl(),
                    dataSource.getUsername(),
                    dataSource.getPassword()
                );
                
                // 获取并记录当前数据库名称
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT DATABASE()")) {
                    if (rs.next()) {
                        String dbName = rs.getString(1);
                        log.info("成功连接到数据库: {}", dbName);
                    }
                } catch (Exception e) {
                    log.warn("无法获取当前数据库名称: {}", e.getMessage());
                }
                
                conn.close();
                log.info("数据库连接测试成功");
            } catch (SQLException e) {
                log.error("数据库连接测试失败", e);
                throw e;
            } catch (ClassNotFoundException e) {
                log.error("数据库驱动加载失败", e);
                throw new SQLException("数据库驱动加载失败: " + e.getMessage());
            }
        }
    }

    @Override
    public List<String> getTables(Long dataSourceId) {
        DataSource dataSource = getById(dataSourceId);
        if (dataSource == null) {
            throw new RuntimeException("数据源不存在");
        }

        if ("KAFKA".equalsIgnoreCase(dataSource.getType())) {
            return getTopics(dataSourceId);
        }

        List<String> tables = new ArrayList<>();
        try {
            Class.forName(dataSource.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    dataSource.getUrl(),
                    dataSource.getUsername(),
                    dataSource.getPassword())) {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (Exception e) {
            log.error("获取表列表失败", e);
            throw new RuntimeException("获取表列表失败: " + e.getMessage(), e);
        }
        return tables;
    }

    @Override
    public List<String> getTopics(Long dataSourceId) {
        DataSource dataSource = getById(dataSourceId);
        if (dataSource == null) {
            throw new RuntimeException("数据源不存在");
        }

        if (!"KAFKA".equalsIgnoreCase(dataSource.getType())) {
            throw new RuntimeException("不支持的数据源类型：" + dataSource.getType());
        }

        List<String> topics = new ArrayList<>();
        try {
            AdminClient adminClient = getOrCreateAdminClient(dataSource);
            Set<String> topicNames = adminClient.listTopics(new ListTopicsOptions().timeoutMs(5000))
                .names()
                .get();
            topics.addAll(topicNames);
            log.info("成功获取到 {} 个主题", topics.size());
        } catch (Exception e) {
            log.error("获取Kafka主题列表失败", e);
            closeKafkaConnection(dataSourceId);
            throw new RuntimeException("获取Kafka主题列表失败: " + e.getMessage(), e);
        }
        
        return topics;
    }

    @Override
    public List<Map<String, String>> getTableColumns(Long dataSourceId, String tableName) {
        DataSource dataSource = getById(dataSourceId);
        if (dataSource == null) {
            throw new RuntimeException("数据源不存在");
        }

        List<Map<String, String>> columns = new ArrayList<>();
        try {
            Class.forName(dataSource.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    dataSource.getUrl(),
                    dataSource.getUsername(),
                    dataSource.getPassword())) {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs = metaData.getColumns(null, null, tableName, null);
                while (rs.next()) {
                    Map<String, String> column = new HashMap<>();
                    column.put("name", rs.getString("COLUMN_NAME"));
                    column.put("type", rs.getString("TYPE_NAME"));
                    column.put("comment", rs.getString("REMARKS"));
                    columns.add(column);
                }
            }
        } catch (Exception e) {
            log.error("获取表结构失败", e);
            throw new RuntimeException("获取表结构失败", e);
        }
        return columns;
    }

    @Override
    public Map<String, List<String>> getTableDependencies(Long dataSourceId, String[] tables) {
        log.info("获取表依赖关系，dataSourceId={}, tables={}", dataSourceId, String.join(",", tables));
        Map<String, List<String>> dependencies = new HashMap<>();
        
        // 初始化依赖图
        for (String table : tables) {
            dependencies.put(table, new ArrayList<>());
        }
        
        DataSource dataSource = getById(dataSourceId);
        if (dataSource == null) {
            log.error("数据源不存在，id={}", dataSourceId);
            return dependencies;
        }
        
        try (Connection conn = getConnection(dataSource)) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            // 获取每个表的外键依赖
            for (String table : tables) {
                try (ResultSet foreignKeys = metaData.getImportedKeys(conn.getCatalog(), null, table)) {
                    while (foreignKeys.next()) {
                        String pkTable = foreignKeys.getString("PKTABLE_NAME");
                        String fkColumn = foreignKeys.getString("FKCOLUMN_NAME");
                        String pkColumn = foreignKeys.getString("PKCOLUMN_NAME");
                        
                        log.info("发现依赖关系: {}.{} 依赖于 {}.{}", table, fkColumn, pkTable, pkColumn);
                        
                        // 只考虑在当前处理列表中的表
                        if (containsIgnoreCase(tables, pkTable) && !dependencies.get(table).contains(pkTable)) {
                            dependencies.get(table).add(pkTable);
                        }
                    }
                }
            }
            
            return dependencies;
        } catch (Exception e) {
            log.error("获取表依赖关系时发生错误", e);
            return dependencies;
        }
    }
    
    /**
     * 忽略大小写检查数组中是否包含某个字符串
     */
    private boolean containsIgnoreCase(String[] array, String target) {
        for (String item : array) {
            if (item.equalsIgnoreCase(target)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 获取数据库连接
     */
    private Connection getConnection(DataSource dataSource) throws SQLException {
        String url = dataSource.getUrl();
        String username = dataSource.getUsername();
        String password = dataSource.getPassword();
        
        if ("MYSQL".equals(dataSource.getType())) {
            return DriverManager.getConnection(url, username, password);
        } else if ("POSTGRESQL".equals(dataSource.getType())) {
            return DriverManager.getConnection(url, username, password);
        } else if ("ORACLE".equals(dataSource.getType())) {
            return DriverManager.getConnection(url, username, password);
        } else {
            throw new SQLException("不支持的数据库类型: " + dataSource.getType());
        }
    }
} 