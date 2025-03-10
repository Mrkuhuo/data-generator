package com.datagenerator.service;

import com.datagenerator.entity.DataTask;
import com.datagenerator.entity.DataSource;
import com.datagenerator.entity.ExecutionRecord;
import com.datagenerator.generator.DataGenerator;
import com.datagenerator.generator.DataGeneratorFactory;
import com.datagenerator.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Iterator;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import com.datagenerator.mapper.TaskMapper;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.PostConstruct;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Data;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataGenerateService {

    private final DataSourceService dataSourceService;
    private static DataSourceService staticDataSourceService;
    private final ExecutionRecordService executionRecordService;
    private final KafkaTemplate<String, String> kafkaProducer;
    private final DataGeneratorFactory dataGeneratorFactory;
    private final TaskMapper taskMapper;
    
    // 存储每个任务的Kafka生产者工厂
    private final Map<Long, ProducerFactory<String, String>> producerFactories = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        staticDataSourceService = dataSourceService;
    }

    /**
     * 获取数据库连接
     */
    private static Connection getConnection(DataSource dataSource) throws SQLException {
        String url = dataSource.getUrl();
        String username = dataSource.getUsername();
        String password = dataSource.getPassword();
        
        log.info("开始连接数据库: {}", url);
        
        try {
            // 根据数据库类型获取驱动类
            String driverClass = null;
        if ("MYSQL".equals(dataSource.getType())) {
                driverClass = "com.mysql.cj.jdbc.Driver";
        } else if ("POSTGRESQL".equals(dataSource.getType())) {
                driverClass = "org.postgresql.Driver";
        } else if ("ORACLE".equals(dataSource.getType())) {
                driverClass = "oracle.jdbc.driver.OracleDriver";
        } else {
            throw new SQLException("不支持的数据库类型: " + dataSource.getType());
        }
            
            Class.forName(driverClass);
            Connection conn = DriverManager.getConnection(url, username, password);
            
            // 获取并记录当前数据库名称
            String dbName = getCurrentDatabaseName(conn);
            log.info("成功连接到数据库: {}, 当前数据库名称: {}", url, dbName);
            
            return conn;
        } catch (ClassNotFoundException e) {
            log.error("数据库驱动加载失败: {}", e.getMessage());
            throw new SQLException("数据库驱动加载失败: " + e.getMessage());
        }
    }

    /**
     * 获取当前数据库名称
     */
    private static String getCurrentDatabaseName(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT DATABASE()")) {
            if (rs.next()) {
                String dbName = rs.getString(1);
                log.info("当前连接的数据库名称: {}", dbName);
                return dbName;
            }
        } catch (SQLException e) {
            log.warn("获取当前数据库名称失败: {}", e.getMessage());
        }
        return null;
    }

    /**
     * 关闭指定任务的Kafka连接
     */
    public void closeKafkaConnections(Long taskId) {
        log.info("开始关闭任务的Kafka连接，taskId={}", taskId);
        ProducerFactory<String, String> factory = producerFactories.remove(taskId);
        if (factory instanceof DefaultKafkaProducerFactory) {
            try {
                DefaultKafkaProducerFactory<String, String> kafkaFactory = (DefaultKafkaProducerFactory<String, String>) factory;
                
                // 使用反射获取内部生产者缓存
                try {
                    Field producersCacheField = DefaultKafkaProducerFactory.class.getDeclaredField("cache");
                    producersCacheField.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    Map<String, org.apache.kafka.clients.producer.Producer<String, String>> producersCache = 
                        (Map<String, org.apache.kafka.clients.producer.Producer<String, String>>) producersCacheField.get(kafkaFactory);
                    
                    if (producersCache != null) {
                        log.info("找到{}个活跃的Kafka生产者，准备关闭", producersCache.size());
                        for (org.apache.kafka.clients.producer.Producer<String, String> producer : producersCache.values()) {
                            try {
                                producer.flush();
                                producer.close();
                                log.info("成功关闭一个Kafka生产者");
                            } catch (Exception e) {
                                log.warn("关闭Kafka生产者时发生错误", e);
                            }
                        }
                        producersCache.clear();
                    }
                } catch (Exception e) {
                    log.warn("通过反射关闭Kafka生产者时发生错误", e);
                }
                
                // 先重置工厂,确保所有生产者都被关闭
                kafkaFactory.reset();
                log.info("已重置Kafka生产者工厂");
                
                // 然后销毁工厂
                kafkaFactory.destroy();
                log.info("已销毁Kafka生产者工厂");
                
                log.info("成功关闭任务的Kafka连接，taskId={}", taskId);
            } catch (Exception e) {
                log.error("关闭任务的Kafka连接时发生错误，taskId={}", taskId, e);
                throw new RuntimeException("关闭Kafka连接失败: " + e.getMessage(), e);
            }
        } else {
            log.info("任务没有活跃的Kafka连接，taskId={}", taskId);
        }
    }

    /**
     * 强制关闭Kafka连接,包括使用反射和系统级别的资源清理
     */
    public void forceCloseKafkaConnections(Long taskId) {
        log.info("开始强制关闭任务的Kafka连接，taskId={}", taskId);
        
        try {
            // 1. 先尝试正常关闭
            closeKafkaConnections(taskId);
        } catch (Exception e) {
            log.warn("正常关闭Kafka连接失败,将尝试强制关闭: {}", e.getMessage());
        }
        
        try {
            // 2. 强制清理所有可能的Kafka连接
            // 从Map中移除但不关闭,因为前面已经尝试关闭了
            producerFactories.remove(taskId);
            
            // 3. 请求垃圾回收,尝试释放资源
            System.gc();
            
            log.info("已强制清理Kafka连接资源，taskId={}", taskId);
        } catch (Exception e) {
            log.error("强制清理Kafka连接资源失败: {}", e.getMessage(), e);
            throw new RuntimeException("强制关闭Kafka连接失败: " + e.getMessage(), e);
        }
    }

    public void executeTask(DataTask task) {
        log.info("开始执行任务: {}", task.getName());
        ExecutionRecord record = null;
        DataSource dataSource = null;
        
        try {
            // 获取任务状态
            String status = task.getStatus();
            if (!"RUNNING".equals(status) || "STOPPING".equals(status) || "STOPPED".equals(status)) {
                log.info("任务状态不是RUNNING或正在停止中，跳过执行: {}", status);
                return;
            }
            
            // 创建执行记录
            record = executionRecordService.createRecord(task.getId());
            
            // 获取数据源
            dataSource = dataSourceService.getById(task.getDataSourceId());
            if (dataSource == null) {
                throw new RuntimeException("数据源不存在");
            }
            log.info("获取到数据源: {}", dataSource.getName());
            
            // 如果数据格式为空，根据数据源类型设置默认格式
            if (task.getDataFormat() == null || task.getDataFormat().trim().isEmpty()) {
                if ("MYSQL".equals(dataSource.getType()) || "POSTGRESQL".equals(dataSource.getType())) {
                    task.setDataFormat("JSON");
                    log.info("设置默认数据格式为JSON");
                } else if ("KAFKA".equals(dataSource.getType())) {
                    task.setDataFormat("JSON");
                    log.info("设置Kafka默认数据格式为JSON");
                } else {
                    throw new IllegalArgumentException("数据格式不能为空");
                }
            }
            
            // 处理目标表列表
            String[] targetTables = task.getTargetName().split(",");
            log.info("需要处理的目标: {}", String.join(", ", targetTables));
            
            // 获取任务频率（秒）
            long frequency = task.getFrequency();
            if (frequency <= 0) {
                frequency = 60; // 默认60秒
            }
            log.info("任务执行频率: {} 秒", frequency);
            
            // 再次检查任务状态,确保在准备阶段没有被停止
            if (!"RUNNING".equals(task.getStatus()) || "STOPPING".equals(task.getStatus()) || "STOPPED".equals(task.getStatus())) {
                log.info("任务状态已变更,不再执行数据生成: {}", task.getStatus());
                return;
            }
            
            // 执行数据生成
            try {
                // 根据数据源类型选择不同的处理逻辑
                if ("KAFKA".equals(dataSource.getType())) {
                    processKafkaTopics(task, dataSource, targetTables);
                } else {
                    // 对于数据库表，如果没有提供模板，自动生成
                    if (task.getTemplate() == null || task.getTemplate().trim().isEmpty()) {
                        for (String tableName : targetTables) {
                            String template = generateTemplateFromMetadata(dataSource, tableName.trim());
                            task.setTemplate(template);
                            log.info("为表 {} 自动生成模板: {}", tableName, template);
                            processDatabaseTables(task, dataSource, new String[]{tableName.trim()});
                        }
                    } else {
                        processDatabaseTables(task, dataSource, targetTables);
                    }
                }
                
                log.info("本次数据生成完成，等待下次执行");
                
                // 更新执行记录状态为成功
                if (record != null) {
                    executionRecordService.updateStatus(record.getId(), 1, null, task.getBatchSize().longValue());
                    log.info("已更新执行记录状态为成功，记录ID: {}", record.getId());
                }
            } catch (Exception e) {
                log.error("数据生成失败: {}", e.getMessage(), e);
                // 更新执行记录状态为失败
                if (record != null) {
                    executionRecordService.updateStatus(record.getId(), 0, e.getMessage(), 0L);
                    log.info("已更新执行记录状态为失败，记录ID: {}", record.getId());
                }
                throw e;
            }
        } catch (Exception e) {
            log.error("任务执行失败: {}", task.getName(), e);
            // 确保执行记录状态被更新为失败
            if (record != null) {
                executionRecordService.updateStatus(record.getId(), 0, e.getMessage(), 0L);
                log.info("已更新执行记录状态为失败，记录ID: {}", record.getId());
            }
            throw new RuntimeException("任务执行失败: " + e.getMessage(), e);
        } finally {
            // 确保在任务执行完成后解锁所有表
            if (dataSource != null && ("MYSQL".equals(dataSource.getType()) || "POSTGRESQL".equals(dataSource.getType()))) {
                log.info("任务执行完成，解锁所有表");
                unlockAllTables(dataSource);
            }
        }
    }

    /**
     * 处理Kafka主题的数据生成
     */
    private void processKafkaTopics(DataTask task, DataSource dataSource, String[] topics) {
        try {
            // 再次检查任务状态,确保在处理Kafka主题前没有被停止
            if (!"RUNNING".equals(task.getStatus()) || "STOPPING".equals(task.getStatus()) || "STOPPED".equals(task.getStatus())) {
                log.info("任务状态已变更,不再处理Kafka主题: {}", task.getStatus());
                return;
            }
            
            // 获取数据生成器
            DataGenerator generator = dataGeneratorFactory.createGenerator(task.getDataFormat());
            log.info("使用数据生成器: {}", generator.getClass().getSimpleName());
            
            // 获取或生成模板
            String template = task.getTemplate();
            if (template == null || template.trim().isEmpty()) {
                if ("ODS_BASE_LOG".equals(task.getTargetName())) {
                    // 为ODS_BASE_LOG生成特定模板
                    JsonNode templateNode = generateOdsBaseLogTemplate();
                    template = objectMapper.writeValueAsString(templateNode);
                    log.info("为ODS_BASE_LOG生成模板: {}", template);
                } else {
                    throw new IllegalArgumentException("数据生成模板不能为空");
                }
            }

            // 验证模板是否为有效的JSON
            try {
                JsonNode jsonNode = objectMapper.readTree(template);
                log.info("模板验证通过，模板结构: {}", jsonNode.toString());
                validateNestedJsonTemplate(jsonNode);
            } catch (Exception e) {
                log.error("模板格式无效: {}", template);
                throw new IllegalArgumentException("数据生成模板格式无效: " + e.getMessage());
            }

            log.info("开始生成数据，使用模板: {}", template);
            List<Map<String, Object>> data = generator.generate(template, task.getBatchSize());
            log.info("生成的原始数据: {}", data);

            // 转换生成的数据为嵌套的JSON结构
            List<Map<String, Object>> transformedData = new ArrayList<>();
            for (Map<String, Object> item : data) {
                try {
                    Map<String, Object> transformedItem = new HashMap<>();
                    
                    // 转换 common 字段
                    if (item.containsKey("common")) {
                        String commonStr = String.valueOf(item.get("common"));
                        log.debug("原始common字段值: {}", commonStr);
                        if (commonStr != null && !commonStr.equals("null")) {
                            try {
                                Map<String, Object> commonMap = objectMapper.readValue(commonStr, Map.class);
                                transformedItem.put("common", commonMap);
                                log.debug("转换后的common字段: {}", commonMap);
                            } catch (Exception e) {
                                log.error("转换common字段失败: {}", e.getMessage(), e);
                                transformedItem.put("common", commonStr);
                            }
                        }
                    }
                    
                    // 转换 start 字段
                    if (item.containsKey("start")) {
                        String startStr = String.valueOf(item.get("start"));
                        log.debug("原始start字段值: {}", startStr);
                        if (startStr != null && !startStr.equals("null")) {
                            try {
                                Map<String, Object> startMap = objectMapper.readValue(startStr, Map.class);
                                transformedItem.put("start", startMap);
                                log.debug("转换后的start字段: {}", startMap);
                            } catch (Exception e) {
                                log.error("转换start字段失败: {}", e.getMessage(), e);
                                transformedItem.put("start", startStr);
                            }
                        }
                    }
                    
                    // 转换 page 字段
                    if (item.containsKey("page")) {
                        String pageStr = String.valueOf(item.get("page"));
                        log.debug("原始page字段值: {}", pageStr);
                        if (pageStr != null && !pageStr.equals("null")) {
                            try {
                                Map<String, Object> pageMap = objectMapper.readValue(pageStr, Map.class);
                                transformedItem.put("page", pageMap);
                                log.debug("转换后的page字段: {}", pageMap);
                            } catch (Exception e) {
                                log.error("转换page字段失败: {}", e.getMessage(), e);
                                transformedItem.put("page", pageStr);
                            }
                        }
                    }
                    
                    // 转换 actions 字段
                    if (item.containsKey("actions")) {
                        String actionsStr = String.valueOf(item.get("actions"));
                        log.debug("原始actions字段值: {}", actionsStr);
                        if (actionsStr != null && !actionsStr.equals("null")) {
                            try {
                                List<Map<String, Object>> actionsList = objectMapper.readValue(actionsStr, List.class);
                                transformedItem.put("actions", actionsList);
                                log.debug("转换后的actions字段: {}", actionsList);
            } catch (Exception e) {
                                log.error("转换actions字段失败: {}", e.getMessage(), e);
                                transformedItem.put("actions", Collections.emptyList());
                            }
                        } else {
                            transformedItem.put("actions", Collections.emptyList());
                        }
                    } else {
                        transformedItem.put("actions", Collections.emptyList());
                    }
                    
                    // 转换 displays 字段
                    if (item.containsKey("displays")) {
                        String displaysStr = String.valueOf(item.get("displays"));
                        log.debug("原始displays字段值: {}", displaysStr);
                        if (displaysStr != null && !displaysStr.equals("null")) {
                            try {
                                List<Map<String, Object>> displaysList = objectMapper.readValue(displaysStr, List.class);
                                transformedItem.put("displays", displaysList);
                                log.debug("转换后的displays字段: {}", displaysList);
                            } catch (Exception e) {
                                log.error("转换displays字段失败: {}", e.getMessage(), e);
                                transformedItem.put("displays", Collections.emptyList());
                            }
                        } else {
                            transformedItem.put("displays", Collections.emptyList());
                        }
                    } else {
                        transformedItem.put("displays", Collections.emptyList());
                    }
                    
                    // 转换 err 字段
                    if (item.containsKey("err")) {
                        String errStr = String.valueOf(item.get("err"));
                        log.debug("原始err字段值: {}", errStr);
                        if (errStr != null && !errStr.equals("null")) {
                            try {
                                Map<String, Object> errMap = objectMapper.readValue(errStr, Map.class);
                                transformedItem.put("err", errMap);
                                log.debug("转换后的err字段: {}", errMap);
                            } catch (Exception e) {
                                log.error("转换err字段失败: {}", e.getMessage(), e);
                                transformedItem.put("err", errStr);
                            }
                        }
                    }
                    
                    // 保留 ts 字段
                    if (item.containsKey("ts")) {
                        transformedItem.put("ts", item.get("ts"));
                        log.debug("ts字段值: {}", item.get("ts"));
                    }
                    
                    transformedData.add(transformedItem);
                    log.debug("转换后的完整数据项: {}", transformedItem);
                } catch (Exception e) {
                    log.error("转换数据项失败: {}", e.getMessage(), e);
                    transformedData.add(item);
                }
            }

            log.info("转换后的数据示例: {}", transformedData.isEmpty() ? "无数据" : transformedData.get(0));

            // 再次检查任务状态,确保在发送Kafka消息前没有被停止
            if (!"RUNNING".equals(task.getStatus()) || "STOPPING".equals(task.getStatus()) || "STOPPED".equals(task.getStatus())) {
                log.info("任务状态已变更,不再发送Kafka消息: {}", task.getStatus());
                return;
            }

            // 获取或创建Kafka生产者工厂
            ProducerFactory<String, String> factory = producerFactories.computeIfAbsent(task.getId(), id -> {
                Map<String, Object> props = new HashMap<>();
                props.put("bootstrap.servers", dataSource.getUrl());
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("request.timeout.ms", "5000");
                props.put("connections.max.idle.ms", "10000");
                props.put("max.block.ms", "3000"); // 最多阻塞3秒
                props.put("delivery.timeout.ms", "5000"); // 最多等待5秒
                
                if (dataSource.getUsername() != null && !dataSource.getUsername().isEmpty()) {
                    props.put("security.protocol", "SASL_PLAINTEXT");
                    props.put("sasl.mechanism", "PLAIN");
                    String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
                    String jaasConfig = String.format(jaasTemplate, dataSource.getUsername(), dataSource.getPassword());
                    props.put("sasl.jaas.config", jaasConfig);
                }
                
                return new DefaultKafkaProducerFactory<>(props);
            });

            // 使用工厂创建新的KafkaTemplate
            KafkaTemplate<String, String> kafkaProducer = new KafkaTemplate<>(factory);
            
            // 再次检查任务状态
            if (!"RUNNING".equals(task.getStatus()) || "STOPPING".equals(task.getStatus()) || "STOPPED".equals(task.getStatus())) {
                log.info("任务状态已变更,不再发送Kafka消息: {}", task.getStatus());
                // 关闭Kafka连接
                closeKafkaConnections(task.getId());
                return;
            }

            try {
            // 写入Kafka主题
            for (String topic : topics) {
                if (topic.trim().isEmpty()) continue;
                
                log.info("开始写入主题: {}", topic);
                    for (Map<String, Object> item : transformedData) {
                        try {
                            // 从数据库获取最新的任务状态
                            DataTask latestTask = getLatestTaskStatus(task.getId());
                            if (latestTask == null || !"RUNNING".equals(latestTask.getStatus())) {
                                log.info("任务状态已变更,停止发送Kafka消息: {}", 
                                    latestTask != null ? latestTask.getStatus() : "任务不存在");
                                return;
                            }
                            
                            String message = objectMapper.writeValueAsString(item);
                            log.debug("发送消息到主题 {}: {}", topic, message);
                            // 使用带超时的发送方法
                            kafkaProducer.send(topic.trim(), message).get(3, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            log.error("序列化或发送消息失败: {}", e.getMessage(), e);
                            // 如果发送失败,检查任务状态
                            DataTask latestTask = getLatestTaskStatus(task.getId());
                            if (latestTask == null || !"RUNNING".equals(latestTask.getStatus())) {
                                log.info("任务状态已变更,停止发送Kafka消息: {}", 
                                    latestTask != null ? latestTask.getStatus() : "任务不存在");
                                return;
                            }
                        }
                    }
                    log.info("主题 {} 写入完成，数据量: {}", topic, transformedData.size());
                }
                
                // 确保所有消息都已发送
                kafkaProducer.flush();
            } finally {
                // 确保在方法结束时关闭Kafka连接
                try {
                    // 直接关闭当前任务的所有Kafka连接
                    closeKafkaConnections(task.getId());
                } catch (Exception e) {
                    log.error("关闭Kafka连接失败: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("处理Kafka主题数据生成时发生错误", e);
            throw new RuntimeException("处理Kafka主题失败: " + e.getMessage(), e);
        }
    }

    /**
     * 验证嵌套的JSON模板结构
     */
    private void validateNestedJsonTemplate(JsonNode node) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                JsonNode fieldConfig = entry.getValue();
                if (!fieldConfig.isObject()) {
                    throw new IllegalArgumentException(
                        String.format("字段 %s 的配置无效，应为对象类型", entry.getKey()));
                }
                
                if (fieldConfig.has("type") && fieldConfig.has("params")) {
                    // 基本类型配置，验证通过
                    return;
                }
                
                if (fieldConfig.has("fields")) {
                    // 嵌套对象配置，递归验证子字段
                    validateNestedJsonTemplate(fieldConfig.get("fields"));
                } else {
                    throw new IllegalArgumentException(
                        String.format("字段 %s 配置无效，缺少 type 和 params 或 fields 属性", entry.getKey()));
                }
            });
        }
    }

    /**
     * 生成ODS_BASE_LOG主题的默认模板
     */
    private JsonNode generateOdsBaseLogTemplate() {
        ObjectNode template = objectMapper.createObjectNode();
        
        // common 字段
        ObjectNode common = template.putObject("common");
        common.put("type", "string");
        ObjectNode commonParams = common.putObject("params");
        commonParams.put("pattern", "{\"ar\":\"${enum:xiaomi|huawei|oppo|vivo}\",\"ba\":\"${enum:google|baidu|bing}\",\"ch\":\"${enum:xiaomi|huawei|oppo|vivo}\",\"is_new\":\"${enum:0|1}\",\"md\":\"${enum:Redmi K30|Huawei P40|OPPO Reno|vivo X60}\",\"mid\":\"${string:[a-zA-Z0-9]{8,16}}\",\"os\":\"${enum:android|ios}\",\"uid\":\"${string:[a-zA-Z0-9]{8,16}}\",\"vc\":${random:100-999},\"vn\":\"${enum:1.0}\"}");

        // start 字段
        ObjectNode start = template.putObject("start");
        start.put("type", "string");
        ObjectNode startParams = start.putObject("params");
        startParams.put("pattern", "{\"entry\":\"${enum:icon|notice|install}\",\"loading_time\":${random:1000-5000},\"open_ad_id\":\"${string:ad[a-zA-Z0-9]{6}}\",\"open_ad_ms\":${random:1000-3000},\"open_ad_skip_ms\":${random:0-2000}}");

        // page 字段
        ObjectNode page = template.putObject("page");
        page.put("type", "string");
        ObjectNode pageParams = page.putObject("params");
        pageParams.put("pattern", "{\"during_time\":${random:5000-20000},\"item\":\"${string:sku[0-9]{6}}\",\"item_type\":\"${enum:sku_id|keyword}\",\"last_page_id\":\"${string:page[0-9]{3}}\",\"page_id\":\"${string:page[0-9]{3}}\",\"source_type\":\"${enum:promotion|recommend|query}\"}");

        // actions 字段 - 数组类型
        ObjectNode actions = template.putObject("actions");
        actions.put("type", "string");
        ObjectNode actionParams = actions.putObject("params");
        actionParams.put("pattern", "[{\"action_id\":\"${string:act[0-9]{3}}\",\"item\":\"${string:sku[0-9]{3}}\",\"item_type\":\"${enum:sku_id|keyword}\",\"ts\":${random:1704067200000-1735689599000}}]");

        // displays 字段 - 数组类型
        ObjectNode displays = template.putObject("displays");
        displays.put("type", "string");
        ObjectNode displayParams = displays.putObject("params");
        displayParams.put("pattern", "[{\"display_type\":\"${enum:query|promotion|recommend}\",\"item\":\"${string:sku[0-9]{3}}\",\"item_type\":\"${enum:sku_id|keyword}\",\"order\":${random:1-10},\"pos_id\":\"${string:pos[0-9]{3}}\"}]");

        // err 字段
        ObjectNode err = template.putObject("err");
        err.put("type", "string");
        ObjectNode errParams = err.putObject("params");
        errParams.put("pattern", "{\"error_code\":${random:1000-9999},\"msg\":\"${string:err[a-zA-Z0-9]{6}}\"}");

        // ts 字段
        ObjectNode ts = template.putObject("ts");
        ts.put("type", "random");
        ObjectNode tsParams = ts.putObject("params");
        tsParams.put("min", 1704067200000L);  // 2024-01-01 00:00:00
        tsParams.put("max", 1735689599000L);  // 2024-12-31 23:59:59
        tsParams.put("integer", true);

        return template;
    }

    private String generateEnumValues(String... values) {
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) pattern.append("|");
            pattern.append(values[i]);
        }
        return pattern.toString();
    }

    private String generateRandomString() {
        return "[a-zA-Z0-9]{8,16}";
    }

    private String generateRandomNumber(long min, long max) {
        return String.format("%d-%d", min, max);
    }

    /**
     * 处理数据库表的数据生成
     */
    private void processDatabaseTables(DataTask task, DataSource dataSource, String[] tables) {
        log.info("开始处理数据库表数据生成");
        
        // 在开始处理前解锁所有表
        unlockAllTables(dataSource);
        
        // 在覆盖模式下，临时禁用外键约束检查
        boolean foreignKeyChecksDisabled = false;
        Connection conn = null;
        
        try {
            // 获取表之间的依赖关系并排序
            List<String> sortedTables = sortTablesByDependencies(dataSource, tables, task.getWriteMode());
            log.info("表处理顺序: {}", sortedTables);
            
            // 创建一个缓存，用于存储已生成的主键值，以便在处理从表时使用
            Map<String, Set<Object>> generatedPrimaryKeys = new HashMap<>();
            
            // 在覆盖模式下，临时禁用外键约束检查
            if ("OVERWRITE".equals(task.getWriteMode()) && "MYSQL".equals(dataSource.getType())) {
                try {
                    conn = getConnection(dataSource);
                    try (Statement stmt = conn.createStatement()) {
                        log.info("覆盖模式：临时禁用外键约束检查");
                        stmt.execute("SET FOREIGN_KEY_CHECKS = 0");
                        foreignKeyChecksDisabled = true;
                    }
                } catch (Exception e) {
                    log.warn("禁用外键约束检查失败: {}", e.getMessage());
                }
            }
            
            for (String tableName : sortedTables) {
                tableName = tableName.trim();
                if (tableName.isEmpty()) {
                    continue;
                }
                
                try {
                    log.info("开始处理表: {}", tableName);
                    // 传递已生成的主键缓存
                    Set<Object> primaryKeys = processTable(task, dataSource, tableName, generatedPrimaryKeys);
                    // 将生成的主键添加到缓存中
                    generatedPrimaryKeys.put(tableName, primaryKeys);
                    log.info("表 {} 处理完成，生成了 {} 个主键值", tableName, primaryKeys.size());
                    
                    // 在每个表处理完后解锁所有表
                    unlockAllTables(dataSource);
                } catch (Exception e) {
                    log.error("处理表 {} 时发生错误: {}", tableName, e.getMessage(), e);
                    
                    // 在出错时也解锁所有表
                    unlockAllTables(dataSource);
                    
                    throw new RuntimeException("处理表失败: " + e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("处理表时发生错误: {}", e.getMessage(), e);
            
            // 在出错时也解锁所有表
            unlockAllTables(dataSource);
            
            throw new RuntimeException("处理表失败: " + e.getMessage(), e);
        } finally {
            // 确保在方法结束时解锁所有表
            unlockAllTables(dataSource);
            
            // 重新启用外键约束检查
            if (foreignKeyChecksDisabled && conn != null) {
                try (Statement stmt = conn.createStatement()) {
                    log.info("重新启用外键约束检查");
                    stmt.execute("SET FOREIGN_KEY_CHECKS = 1");
                } catch (Exception e) {
                    log.warn("重新启用外键约束检查失败: {}", e.getMessage());
                } finally {
                    try {
                        conn.close();
                    } catch (Exception e) {
                        log.warn("关闭连接失败: {}", e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * 根据表之间的依赖关系对表进行排序
     * 确保先处理主表，再处理从表
     * 在覆盖模式下，顺序相反，先处理从表，再处理主表
     */
    private List<String> sortTablesByDependencies(DataSource dataSource, String[] tables, String writeMode) {
        log.info("开始分析表依赖关系");
        
        // 创建表依赖图
        Map<String, Set<String>> dependencies = new HashMap<>();
        Map<String, Set<String>> reverseDependencies = new HashMap<>();
        
        // 初始化依赖图
        for (String table : tables) {
            dependencies.put(table, new HashSet<>());
            reverseDependencies.put(table, new HashSet<>());
        }
        
        try (Connection conn = getConnection(dataSource)) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            // 获取每个表的外键依赖
            for (String table : tables) {
                try (ResultSet foreignKeys = metaData.getImportedKeys(conn.getCatalog(), null, table)) {
                    while (foreignKeys.next()) {
                        String pkTable = foreignKeys.getString("PKTABLE_NAME");
                        
                        // 只考虑在当前处理列表中的表
                        if (Arrays.asList(tables).contains(pkTable)) {
                            dependencies.get(table).add(pkTable);
                            reverseDependencies.get(pkTable).add(table);
                            log.info("发现依赖关系: {} 依赖于 {}", table, pkTable);
                        } else {
                            // 如果依赖的表不在当前处理列表中，记录警告
                            log.warn("表 {} 依赖于表 {}，但后者不在当前处理列表中，可能会导致外键约束失败", 
                                table, pkTable);
                        }
                    }
                }
            }
            
            // 使用拓扑排序
            List<String> sortedTables = new ArrayList<>();
            Set<String> visited = new HashSet<>();
            Set<String> temp = new HashSet<>();
            
            // 从没有依赖的表开始
            for (String table : tables) {
                if (!visited.contains(table)) {
                    topologicalSort(table, dependencies, visited, temp, sortedTables);
                }
            }
            
            // 在覆盖模式下，顺序相反，先处理从表，再处理主表
            if ("OVERWRITE".equals(writeMode)) {
                log.info("覆盖模式：反转表处理顺序，先处理从表，再处理主表");
                // 不需要反转，因为我们的拓扑排序已经是从叶子节点到根节点的顺序
            } else {
                // 在其他模式下，先处理主表，再处理从表
                log.info("非覆盖模式：先处理主表，再处理从表");
                Collections.reverse(sortedTables);
            }
            
            // 确保所有表都在结果中
            List<String> finalSortedTables = new ArrayList<>();
            for (String table : sortedTables) {
                if (!finalSortedTables.contains(table)) {
                    finalSortedTables.add(table);
                }
            }
            
            // 添加任何可能遗漏的表
            for (String table : tables) {
                if (!finalSortedTables.contains(table)) {
                    finalSortedTables.add(table);
                }
            }
            
            // 记录最终的表处理顺序
            log.info("最终表处理顺序: {}", finalSortedTables);
            
            return finalSortedTables;
        } catch (Exception e) {
            log.error("分析表依赖关系时发生错误: {}", e.getMessage(), e);
            // 如果出错，返回原始表顺序
            return Arrays.asList(tables);
        }
    }

    /**
     * 拓扑排序辅助方法
     */
    private void topologicalSort(String table, Map<String, Set<String>> dependencies, 
                                Set<String> visited, Set<String> temp, List<String> result) {
        // 如果已经访问过，直接返回
        if (visited.contains(table)) {
            return;
        }
        
        // 检测循环依赖
        if (temp.contains(table)) {
            log.warn("检测到循环依赖，涉及表: {}", table);
            return;
        }
        
        temp.add(table);
        
        // 递归访问依赖的表
        for (String dependency : dependencies.getOrDefault(table, Collections.emptySet())) {
            topologicalSort(dependency, dependencies, visited, temp, result);
        }
        
        temp.remove(table);
        visited.add(table);
        result.add(table);
    }

    /**
     * 处理单个表的数据生成
     * @return 生成的主键值集合
     */
    private Set<Object> processTable(DataTask task, DataSource dataSource, String tableName, 
                                   Map<String, Set<Object>> generatedPrimaryKeys) throws SQLException {
        log.info("开始处理表: {}", tableName);
        
        // 用于存储生成的主键值
        Set<Object> generatedKeys = new HashSet<>();
        
        try (Connection conn = getConnection(dataSource)) {
            // 获取表结构
            Map<String, Map<String, String>> tableColumns = getTableDefinition(conn, tableName);
            log.info("表 {} 结构: {}", tableName, tableColumns.keySet());
            
            if (tableColumns.isEmpty()) {
                throw new SQLException("无法获取表结构信息");
            }
            
            // 获取表的外键信息
            Map<String, ForeignKeyInfo> foreignKeys = getForeignKeyInfo(tableName);
                
            // 获取当前最大ID
            Long currentMaxId = null;
            String primaryKeyColumn = null;
            boolean isAutoIncrement = false;
                
            for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                if ("PRI".equals(entry.getValue().get("COLUMN_KEY"))) {
                    primaryKeyColumn = entry.getKey();
                    isAutoIncrement = "auto_increment".equalsIgnoreCase(entry.getValue().get("EXTRA"));
                    break;
                }
            }
                
            if (primaryKeyColumn != null && !isAutoIncrement) {
                try (Statement stmt = conn.createStatement()) {
                    String sql = String.format("SELECT MAX(%s) FROM %s", primaryKeyColumn, tableName);
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) {
                            Object maxId = rs.getObject(1);
                            if (maxId != null) {
                                if (maxId instanceof Number) {
                                    currentMaxId = ((Number) maxId).longValue();
                                } else {
                                    currentMaxId = 0L;
                                }
                            } else {
                                currentMaxId = 0L;
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("获取表 {} 的最大ID失败: {}", tableName, e.getMessage());
                    currentMaxId = 0L;
                }
            }
                
            // 生成数据模板
            DataGenerator generator = dataGeneratorFactory.createGenerator(task.getDataFormat());
            String template = task.getTemplate();
            if (template == null || template.trim().isEmpty()) {
                if ("MYSQL".equals(dataSource.getType()) || "POSTGRESQL".equals(dataSource.getType())) {
                    template = generateDefaultTemplate(task, tableColumns, currentMaxId, tableName);
                    task.setTemplate(template);
                    log.info("使用生成的默认模板: {}", template);
                } else {
                    throw new IllegalArgumentException("数据生成模板不能为空");
                }
            } else {
                // 如果有自定义模板，验证并确保它包含表中的字段
                try {
                    JsonNode templateNode = objectMapper.readTree(template);
                    ObjectNode validatedTemplate = objectMapper.createObjectNode();
                    
                    // 只保留表中实际存在的字段
                    for (String columnName : tableColumns.keySet()) {
                        if (templateNode.has(columnName)) {
                            validatedTemplate.set(columnName, templateNode.get(columnName));
                        }
                    }
                    
                    // 如果有字段缺失，使用默认模板补充
                    for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                        String columnName = entry.getKey();
                        if (!validatedTemplate.has(columnName)) {
                            Map<String, String> columnInfo = entry.getValue();
                            
                            // 跳过自增主键
                            if ("PRI".equals(columnInfo.get("COLUMN_KEY")) && 
                                "auto_increment".equalsIgnoreCase(columnInfo.get("EXTRA"))) {
                                continue;
                            }
                            
                            // 为缺失的字段生成默认模板
                            ObjectNode fieldNode = generateFieldTemplate(columnName, columnInfo);
                            if (fieldNode != null) {
                                validatedTemplate.set(columnName, fieldNode);
                            }
                        }
                    }
                    
                    template = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(validatedTemplate);
                    task.setTemplate(template);
                    log.info("验证并更新模板: {}", template);
                } catch (Exception e) {
                    log.error("验证模板失败，将使用默认模板: {}", e.getMessage());
                    template = generateDefaultTemplate(task, tableColumns, currentMaxId, tableName);
                    task.setTemplate(template);
                }
            }
                
            // 生成数据
            log.info("开始生成数据，使用模板: {}", template);
            List<Map<String, Object>> data = generator.generate(template, task.getBatchSize());
                
            // 验证生成的数据
            List<Map<String, Object>> validData = new ArrayList<>();
            for (Map<String, Object> row : data) {
                Map<String, Object> validRow = new HashMap<>();
                boolean hasValidFields = false;
                
                // 只保留表中实际存在的字段
                for (String columnName : tableColumns.keySet()) {
                    if (row.containsKey(columnName)) {
                        Object value = row.get(columnName);
                        if (value != null) {
                            validRow.put(columnName, value);
                            hasValidFields = true;
                        }
                    }
                }
                
                if (hasValidFields) {
                    validData.add(validRow);
                }
            }
            
            if (validData.isEmpty()) {
                throw new SQLException("没有有效的字段可以插入");
            }
                
            // 处理生成的数据，传递已生成的主键缓存
            validData = processGeneratedData(validData, tableColumns, currentMaxId, tableName, foreignKeys, generatedPrimaryKeys);
                
            // 验证生成的数据
            validateGeneratedData(validData, tableColumns);
                
            log.info("表 {} 生成数据完成，数量: {}", tableName, validData.size());
            
            // 记录第一条数据的所有字段值，用于调试
            if (!validData.isEmpty()) {
                Map<String, Object> firstRow = validData.get(0);
                for (Map.Entry<String, Object> entry : firstRow.entrySet()) {
                    log.debug("字段 {} 的值: {}", entry.getKey(), entry.getValue());
                }
            }
                
            // 执行插入操作
            if ("TABLE".equals(task.getTargetType())) {
                // 如果是MySQL，获取表的写锁
                if ("MYSQL".equals(dataSource.getType())) {
                    try (Statement stmt = conn.createStatement()) {
                        log.info("获取表 {} 的写锁", tableName);
                        stmt.execute("LOCK TABLES " + tableName + " WRITE");
                    } catch (Exception e) {
                        log.warn("获取表 {} 的锁失败: {}", tableName, e.getMessage());
                        // 继续执行，不中断流程
                    }
                }
                
                try {
                    // 如果是覆盖模式，先清空表
                    if ("OVERWRITE".equals(task.getWriteMode())) {
                        try {
                            // 临时禁用外键约束检查
                            try (Statement stmt = conn.createStatement()) {
                                log.info("临时禁用外键约束检查");
                                stmt.execute("SET FOREIGN_KEY_CHECKS = 0");
                            }
                            
                            // 清空表
                            try (Statement stmt = conn.createStatement()) {
                                log.info("清空表 {}", tableName);
                                stmt.execute("TRUNCATE TABLE " + tableName);
                            }
                            
                            // 重新启用外键约束检查
                            try (Statement stmt = conn.createStatement()) {
                                log.info("重新启用外键约束检查");
                                stmt.execute("SET FOREIGN_KEY_CHECKS = 1");
                            }
                        } catch (Exception e) {
                            log.warn("清空表 {} 失败: {}", tableName, e.getMessage());
                            // 如果TRUNCATE失败，尝试使用DELETE
                            try (Statement stmt = conn.createStatement()) {
                                log.info("尝试使用DELETE清空表 {}", tableName);
                                stmt.execute("DELETE FROM " + tableName);
                            } catch (Exception ex) {
                                log.warn("使用DELETE清空表 {} 也失败: {}", tableName, ex.getMessage());
                                // 继续执行，不中断流程
                            }
                        }
                    }
                    
                    // 构建插入SQL
                    if (!validData.isEmpty()) {
                        Map<String, Object> firstRow = validData.get(0);
                        
                        // 过滤出表中实际存在的字段
                        List<String> validColumns = new ArrayList<>();
                        for (String column : firstRow.keySet()) {
                            if (tableColumns.containsKey(column)) {
                                validColumns.add(column);
                            } else {
                                log.warn("表 {} 中不存在字段 {}，将被忽略", tableName, column);
                            }
                        }
                        
                        if (validColumns.isEmpty()) {
                            log.error("表 {} 没有有效的字段可以插入", tableName);
                            throw new SQLException("没有有效的字段可以插入");
                        }
                        
                        StringBuilder insertSql = new StringBuilder();
                        insertSql.append("INSERT INTO ").append(tableName).append(" (");
                        insertSql.append(String.join(", ", validColumns));
                        insertSql.append(") VALUES (");
                        insertSql.append(String.join(", ", Collections.nCopies(validColumns.size(), "?")));
                        insertSql.append(")");
                        
                        log.info("执行插入SQL: {}", insertSql);
                        
                        // 批量插入数据
                        try (PreparedStatement pstmt = conn.prepareStatement(insertSql.toString(), Statement.RETURN_GENERATED_KEYS)) {
                            int batchCount = 0;
                            int successCount = 0;
                            List<Map<String, Object>> failedRows = new ArrayList<>();
                            
                            // 第一轮：尝试批量插入所有数据
                            for (Map<String, Object> row : validData) {
                                int paramIndex = 1;
                                for (String column : validColumns) {
                                    pstmt.setObject(paramIndex++, row.get(column));
                                }
                                pstmt.addBatch();
                                batchCount++;
                                
                                // 每1000条提交一次
                                if (batchCount % 1000 == 0) {
                                    try {
                                        pstmt.executeBatch();
                                        pstmt.clearBatch();
                                        successCount += 1000;
                                        log.info("已插入 {} 条数据", successCount);
                                    } catch (BatchUpdateException e) {
                                        log.warn("批量插入部分失败: {}", e.getMessage());
                                        // 收集失败的行，稍后单独处理
                                        int[] updateCounts = e.getUpdateCounts();
                                        int startIndex = batchCount - 1000;
                                        for (int i = 0; i < updateCounts.length; i++) {
                                            if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                                                failedRows.add(validData.get(startIndex + i));
                                            } else {
                                                successCount++;
                                            }
                                        }
                                        pstmt.clearBatch();
                                    }
                                }
                            }
                            
                            // 提交剩余的批次
                            if (batchCount % 1000 != 0) {
                                try {
                                    pstmt.executeBatch();
                                    successCount += batchCount % 1000;
                                } catch (BatchUpdateException e) {
                                    log.warn("批量插入部分失败: {}", e.getMessage());
                                    // 收集失败的行，稍后单独处理
                                    int[] updateCounts = e.getUpdateCounts();
                                    int startIndex = batchCount - (batchCount % 1000);
                                    for (int i = 0; i < updateCounts.length; i++) {
                                        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                                            failedRows.add(validData.get(startIndex + i));
                                        } else {
                                            successCount++;
                                        }
                                    }
                                }
                            }
                            
                            log.info("第一轮插入完成，成功: {}, 失败: {}", successCount, failedRows.size());
                            
                            // 第二轮：处理失败的行，尝试修复唯一约束冲突
                            if (!failedRows.isEmpty()) {
                                log.info("开始处理 {} 条失败的数据", failedRows.size());
                                int retrySuccessCount = 0;
                                
                                // 对于每一行失败的数据，最多重试3次
                                int maxRetries = 3;
                                
                                for (Map<String, Object> row : failedRows) {
                                    boolean success = false;
                                    Map<String, Object> currentRow = row;
                                    
                                    for (int retryCount = 0; retryCount < maxRetries && !success; retryCount++) {
                                        // 检查并修复可能的约束冲突
                                        Set<String> uniqueColumns = new HashSet<>();
                                        for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                                            if ("UNI".equals(entry.getValue().get("COLUMN_KEY"))) {
                                                uniqueColumns.add(entry.getKey());
                                            }
                                        }
                                        currentRow = fixUniqueConstraintViolation(currentRow, tableName, tableColumns, uniqueColumns);
                                        
                                        try {
                                            // 单独插入修复后的行
                                            int paramIndex = 1;
                                            for (String column : validColumns) {
                                                pstmt.setObject(paramIndex++, currentRow.get(column));
                                            }
                                            
                                            int result = pstmt.executeUpdate();
                                            if (result > 0) {
                                                retrySuccessCount++;
                                                success = true;
                                                
                                                // 如果是自增主键，获取生成的主键值
                                                if (primaryKeyColumn != null && isAutoIncrement) {
                                                    try (ResultSet rs = pstmt.getGeneratedKeys()) {
                                                        if (rs.next()) {
                                                            Object generatedKey = rs.getObject(1);
                                                            generatedKeys.add(generatedKey);
                                                        }
                                                    }
                                                } else if (primaryKeyColumn != null && currentRow.containsKey(primaryKeyColumn)) {
                                                    // 如果不是自增主键，从数据中获取主键值
                                                    generatedKeys.add(currentRow.get(primaryKeyColumn));
                                                }
                                                
                                                log.info("第 {} 次重试成功", retryCount + 1);
                                                break;
                                            }
                                        } catch (SQLException e) {
                                            String errorMessage = e.getMessage();
                                            log.warn("第 {} 次重试插入失败: {}", retryCount + 1, errorMessage);
                                            
                                            // 如果是最后一次重试，记录详细错误信息
                                            if (retryCount == maxRetries - 1) {
                                                log.error("在 {} 次重试后仍然失败，错误: {}", maxRetries, errorMessage);
                                                
                                                // 记录行数据，帮助调试
                                                log.error("失败的行数据: {}", currentRow);
                                                
                                                // 如果是外键约束错误，记录相关表和字段
                                                if (errorMessage.contains("foreign key constraint fails")) {
                                                    // 尝试从错误消息中提取表名和字段名
                                                    String constraintInfo = errorMessage.substring(
                                                        errorMessage.indexOf("CONSTRAINT") + "CONSTRAINT".length());
                                                    log.error("外键约束信息: {}", constraintInfo);
                                                    
                                                    // 获取表的外键信息
                                                    log.error("表 {} 的外键信息: {}", tableName, foreignKeys);
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                log.info("重试插入完成，成功: {}, 最终失败: {}", retrySuccessCount, failedRows.size() - retrySuccessCount);
                                successCount += retrySuccessCount;
                            }
                            
                            log.info("表 {} 插入完成，共 {} 条数据，成功: {}", tableName, batchCount, successCount);
                            
                            // 收集生成的主键
                            if (primaryKeyColumn != null && generatedKeys.isEmpty()) {
                                // 如果是自增主键，获取生成的主键值
                                if (isAutoIncrement) {
                                    try (Statement stmt = conn.createStatement();
                                         ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM %s", primaryKeyColumn, tableName))) {
                                        while (rs.next()) {
                                            Object generatedKey = rs.getObject(1);
                                            generatedKeys.add(generatedKey);
                                        }
                                    }
                                } else {
                                    // 如果不是自增主键，从数据中获取主键值
                                    for (Map<String, Object> row : validData) {
                                        if (row.containsKey(primaryKeyColumn)) {
                                            generatedKeys.add(row.get(primaryKeyColumn));
                                        }
                                    }
                                }
                            }
                        } catch (SQLException e) {
                            log.error("插入数据时发生错误: {}", e.getMessage());
                            throw e;
                        }
                    } else {
                        log.warn("表 {} 没有生成任何数据", tableName);
                    }
                } finally {
                    // 解锁表
                    if ("MYSQL".equals(dataSource.getType())) {
                        try (Statement stmt = conn.createStatement()) {
                            log.info("解锁表");
                            stmt.execute("UNLOCK TABLES");
                        } catch (Exception e) {
                            log.warn("解锁表失败: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        
        return generatedKeys;
    }

    /**
     * 检查表是否有数据
     */
    private boolean hasData(DataSource dataSource, String tableName) {
        try (Connection conn = getConnection(dataSource)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = String.format("SELECT 1 FROM %s LIMIT 1", tableName);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    return rs.next();
                }
            }
        } catch (Exception e) {
            log.error("检查表 {} 是否有数据时发生错误: {}", tableName, e.getMessage());
            return false;
        }
    }

    /**
     * 获取表的完整定义信息
     */
    private Map<String, Map<String, String>> getTableDefinition(Connection conn, String tableName) throws SQLException {
        Map<String, Map<String, String>> tableDefinition = new HashMap<>();
        
        // 获取表的列信息
        String columnSql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, " +
                          "CHARACTER_MAXIMUM_LENGTH, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, " +
                          "COLUMN_DEFAULT, COLUMN_COMMENT " +
                          "FROM INFORMATION_SCHEMA.COLUMNS " +
                          "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? " +
                          "ORDER BY ORDINAL_POSITION";
                          
        try (PreparedStatement pstmt = conn.prepareStatement(columnSql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, String> columnInfo = new HashMap<>();
                    columnInfo.put("DATA_TYPE", rs.getString("DATA_TYPE"));
                    columnInfo.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
                    columnInfo.put("COLUMN_KEY", rs.getString("COLUMN_KEY"));
                    columnInfo.put("EXTRA", rs.getString("EXTRA"));
                    columnInfo.put("CHARACTER_MAXIMUM_LENGTH", rs.getString("CHARACTER_MAXIMUM_LENGTH"));
                    columnInfo.put("COLUMN_TYPE", rs.getString("COLUMN_TYPE"));
                    columnInfo.put("NUMERIC_PRECISION", rs.getString("NUMERIC_PRECISION"));
                    columnInfo.put("NUMERIC_SCALE", rs.getString("NUMERIC_SCALE"));
                    columnInfo.put("COLUMN_DEFAULT", rs.getString("COLUMN_DEFAULT"));
                    columnInfo.put("COLUMN_COMMENT", rs.getString("COLUMN_COMMENT"));
                    tableDefinition.put(rs.getString("COLUMN_NAME"), columnInfo);
                }
            }
        }
        
        // 获取表的约束信息
        String constraintSql = "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE " +
                              "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " +
                              "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
                              
        try (PreparedStatement pstmt = conn.prepareStatement(constraintSql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String constraintType = rs.getString("CONSTRAINT_TYPE");
                    
                    // 如果是外键约束，获取引用信息
                    if ("FOREIGN KEY".equals(constraintType)) {
                        String fkSql = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
                                     "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                                     "WHERE CONSTRAINT_NAME = ? AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
                        try (PreparedStatement fkStmt = conn.prepareStatement(fkSql)) {
                            fkStmt.setString(1, constraintName);
                            fkStmt.setString(2, tableName);
                            try (ResultSet fkRs = fkStmt.executeQuery()) {
                                if (fkRs.next()) {
                                    String columnName = fkRs.getString("COLUMN_NAME");
                                    Map<String, String> columnInfo = tableDefinition.get(columnName);
                                    if (columnInfo != null) {
                                        columnInfo.put("REFERENCED_TABLE", fkRs.getString("REFERENCED_TABLE_NAME"));
                                        columnInfo.put("REFERENCED_COLUMN", fkRs.getString("REFERENCED_COLUMN_NAME"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return tableDefinition;
    }

    private String generateDefaultTemplate(DataTask task, Map<String, Map<String, String>> metadata, Long currentMaxId, String tableName) {
        log.info("为表 {} 生成默认模板", tableName);
        
        ObjectNode rootNode = objectMapper.createObjectNode();
        
        // 遍历表的所有字段
        for (Map.Entry<String, Map<String, String>> entry : metadata.entrySet()) {
            String columnName = entry.getKey();
            Map<String, String> columnInfo = entry.getValue();
            
            // 跳过自增主键
            if ("PRI".equals(columnInfo.get("COLUMN_KEY")) && 
                "auto_increment".equalsIgnoreCase(columnInfo.get("EXTRA"))) {
                continue;
            }
            
            String dataType = columnInfo.get("DATA_TYPE");
            if (dataType == null) {
                log.warn("字段 {} 没有数据类型信息，将被跳过", columnName);
                continue;
            }
            
            // 创建字段节点
            ObjectNode fieldNode = objectMapper.createObjectNode();
            ObjectNode paramsNode = objectMapper.createObjectNode();
            
            // 获取字段的元数据信息
            String columnComment = columnInfo.get("COLUMN_COMMENT");
            String columnType = columnInfo.get("COLUMN_TYPE");
            String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
            String isNullable = columnInfo.get("IS_NULLABLE");
            String columnKey = columnInfo.get("COLUMN_KEY");
            String numericPrecision = columnInfo.get("NUMERIC_PRECISION");
            String numericScale = columnInfo.get("NUMERIC_SCALE");
            
            // 根据字段类型和元数据动态生成模板
            switch (dataType.toLowerCase()) {
                case "varchar":
                case "char":
                case "text":
                    // 根据字段注释和名称推断数据类型
                    String fieldNameLower = columnName.toLowerCase();
                    String commentLower = columnComment != null ? columnComment.toLowerCase() : "";
                    
                    // 从注释中提取数据类型提示
                    String dataTypeHint = inferDataTypeFromComment(commentLower);
                    if (dataTypeHint != null) {
                        fieldNode.put("type", dataTypeHint);
                    } else if (isEnumField(columnType, columnComment)) {
                        fieldNode.put("type", "enum");
                        Set<String> enumValues = extractEnumValues(columnType, columnComment);
                        if (!enumValues.isEmpty()) {
                            ArrayNode valuesNode = objectMapper.createArrayNode();
                            enumValues.forEach(valuesNode::add);
                            paramsNode.set("values", valuesNode);
                        }
                    } else {
                        fieldNode.put("type", "string");
                        if (maxLength != null) {
                            paramsNode.put("length", Math.min(Integer.parseInt(maxLength), 20));
                        }
                    }
                    break;
                    
                case "bigint":
                case "int":
                case "integer":
                case "tinyint":
                case "smallint":
                case "mediumint":
                    // 检查是否是外键
                    String referencedTable = columnInfo.get("REFERENCED_TABLE");
                    if (referencedTable != null) {
                        fieldNode.put("type", "foreignKey");
                        paramsNode.put("table", referencedTable);
                    } else {
                        fieldNode.put("type", "random");
                        paramsNode.put("min", 1);
                        paramsNode.put("max", 1000);
                        paramsNode.put("integer", true);
                    }
                    break;
                    
                case "decimal":
                case "float":
                case "double":
                    fieldNode.put("type", "random");
                    setDecimalRange(paramsNode, columnName, columnComment, numericPrecision, numericScale);
                    break;
                    
                case "date":
                    fieldNode.put("type", "date");
                    setDateRange(paramsNode, "yyyy-MM-dd", columnComment);
                    break;
                    
                case "datetime":
                case "timestamp":
                    fieldNode.put("type", "date");
                    setDateRange(paramsNode, "yyyy-MM-dd HH:mm:ss", columnComment);
                    break;
                    
                case "time":
                    fieldNode.put("type", "date");
                    setDateRange(paramsNode, "HH:mm:ss", columnComment);
                    break;
                    
                case "enum":
                    fieldNode.put("type", "enum");
                    Set<String> enumValues = extractEnumValues(columnType, columnComment);
                    if (!enumValues.isEmpty()) {
                        ArrayNode valuesNode = objectMapper.createArrayNode();
                        enumValues.forEach(valuesNode::add);
                        paramsNode.set("values", valuesNode);
                    }
                    break;
                    
                default:
                    fieldNode.put("type", "string");
                    paramsNode.put("length", 10);
                    break;
            }
            
            fieldNode.set("params", paramsNode);
            rootNode.set(columnName, fieldNode);
        }
        
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
        } catch (Exception e) {
            log.error("生成默认模板失败: {}", e.getMessage());
            return "{}";
        }
    }
    
    /**
     * 从注释中推断数据类型
     */
    private String inferDataTypeFromComment(String comment) {
        if (comment == null || comment.isEmpty()) {
            return null;
        }
        
        Map<String, String[]> typeKeywords = new HashMap<>();
        typeKeywords.put("phone", new String[]{"电话", "手机", "联系方式", "phone", "mobile", "tel"});
        typeKeywords.put("address", new String[]{"地址", "位置", "address", "location"});
        typeKeywords.put("name", new String[]{"名称", "姓名", "用户名", "name", "username"});
        typeKeywords.put("email", new String[]{"邮箱", "email", "mail"});
        typeKeywords.put("url", new String[]{"网址", "链接", "url", "link", "website"});
        
        for (Map.Entry<String, String[]> entry : typeKeywords.entrySet()) {
            for (String keyword : entry.getValue()) {
                if (comment.contains(keyword)) {
                    return entry.getKey();
                }
            }
        }
        
        return null;
    }
    
    /**
     * 判断是否是枚举字段
     */
    private boolean isEnumField(String columnType, String comment) {
        if (columnType != null && columnType.startsWith("enum(")) {
            return true;
        }
        
        if (comment != null && !comment.isEmpty()) {
            return comment.contains("状态") || comment.contains("类型") || 
                   comment.contains("标志") || comment.contains("枚举") ||
                   comment.contains("status") || comment.contains("type") ||
                   comment.contains("flag") || comment.contains("enum");
        }
        
        return false;
    }
    
    /**
     * 提取枚举值
     */
    private Set<String> extractEnumValues(String columnType, String comment) {
        Set<String> values = new HashSet<>();
        
        // 从字段类型中提取
        if (columnType != null && columnType.startsWith("enum(")) {
            values.addAll(extractEnumValuesFromType(columnType));
        }
        
        // 从注释中提取
        if (comment != null && !comment.isEmpty()) {
            values.addAll(extractEnumValuesFromComment(comment));
        }
        
        // 如果没有找到枚举值，提供默认值
        if (values.isEmpty()) {
            values.add("1");
            values.add("0");
        }
        
        return values;
    }
    
    /**
     * 设置数值范围
     */
    private void setNumberRange(ObjectNode paramsNode, String columnName, String comment, String columnType) {
        int min = 1;
        int max = 1000;
        
        // 从注释中提取范围信息
        if (comment != null && !comment.isEmpty()) {
            // 尝试从注释中提取范围，例如：范围(1-100)或range[1,100]
            Pattern pattern = Pattern.compile("(?:范围|range)[\\(\\[](\\d+)[-~,](\\d+)[\\)\\]]");
            Matcher matcher = pattern.matcher(comment);
            if (matcher.find()) {
                try {
                    min = Integer.parseInt(matcher.group(1));
                    max = Integer.parseInt(matcher.group(2));
                } catch (NumberFormatException e) {
                    log.warn("解析注释中的范围失败: {}", e.getMessage());
                }
            }
        }
        
        paramsNode.put("min", min);
        paramsNode.put("max", max);
    }
    
    /**
     * 设置小数范围
     */
    private void setDecimalRange(ObjectNode paramsNode, String columnName, String comment, 
                               String numericPrecision, String numericScale) {
        double min = 0;
        double max = 1000;
        int decimals = 2;
        
        // 使用数据库定义的精度
        if (numericScale != null) {
            try {
                decimals = Integer.parseInt(numericScale);
            } catch (NumberFormatException e) {
                log.warn("解析小数位数失败: {}", e.getMessage());
            }
        }
        
        // 从注释中提取范围信息
        if (comment != null && !comment.isEmpty()) {
            Pattern pattern = Pattern.compile("(?:范围|range)[\\(\\[](\\d+(?:\\.\\d+)?)[,~-](\\d+(?:\\.\\d+)?)[\\)\\]]");
            Matcher matcher = pattern.matcher(comment);
            if (matcher.find()) {
                try {
                    min = Double.parseDouble(matcher.group(1));
                    max = Double.parseDouble(matcher.group(2));
                } catch (NumberFormatException e) {
                    log.warn("解析注释中的范围失败: {}", e.getMessage());
                }
            }
        }
        
        paramsNode.put("min", min);
        paramsNode.put("max", max);
        paramsNode.put("integer", false);
    }
    
    /**
     * 设置日期范围
     */
    private void setDateRange(ObjectNode paramsNode, String format, String comment) {
        String min = "2020-01-01";
        String max = "2024-12-31";
        
        if (format.contains("HH:mm:ss")) {
            if (format.equals("HH:mm:ss")) {
                min = "00:00:00";
                max = "23:59:59";
            } else {
                min += " 00:00:00";
                max += " 23:59:59";
            }
        }
        
        // 从注释中提取日期范围
        if (comment != null && !comment.isEmpty()) {
            Pattern pattern = Pattern.compile("(?:日期范围|date range)[\\(\\[](\\d{4}-\\d{2}-\\d{2})[,~-](\\d{4}-\\d{2}-\\d{2})[\\)\\]]");
            Matcher matcher = pattern.matcher(comment);
            if (matcher.find()) {
                min = matcher.group(1);
                max = matcher.group(2);
                if (format.contains("HH:mm:ss") && !format.equals("HH:mm:ss")) {
                    min += " 00:00:00";
                    max += " 23:59:59";
                }
            }
        }
        
        paramsNode.put("format", format);
        paramsNode.put("min", min);
        paramsNode.put("max", max);
    }

    /**
     * 检查字符串是否包含任何关键词
     */
    private boolean containsAnyKeyword(String fieldName, String comment, String... keywords) {
        for (String keyword : keywords) {
            if (fieldName.contains(keyword) || comment.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 从注释中提取可能的枚举值
     */
    private Set<String> extractEnumValuesFromComment(String comment) {
        Set<String> values = new HashSet<>();
        if (comment != null && !comment.isEmpty()) {
            // 尝试从注释中提取枚举值，假设它们用逗号、分号或其他分隔符分隔
            String[] possibleValues = comment.split("[,;|]");
            for (String value : possibleValues) {
                value = value.trim();
                if (!value.isEmpty()) {
                    values.add(value);
                }
            }
        }
        return values;
    }
    
    /**
     * 从字段类型中提取枚举值
     */
    private Set<String> extractEnumValuesFromType(String columnType) {
        Set<String> values = new HashSet<>();
        if (columnType != null && columnType.startsWith("enum(") && columnType.endsWith(")")) {
            String enumValues = columnType.substring(5, columnType.length() - 1);
            String[] rawValues = enumValues.split(",");
            for (String value : rawValues) {
                value = value.trim();
                if (value.startsWith("'") && value.endsWith("'")) {
                    value = value.substring(1, value.length() - 1);
                }
                if (!value.isEmpty()) {
                    values.add(value);
                }
            }
        }
        return values;
    }

    /**
     * 验证生成的数据是否符合表结构约束
     */
    private void validateGeneratedData(List<Map<String, Object>> data, Map<String, Map<String, String>> tableColumns) {
        if (data == null || data.isEmpty()) {
            throw new IllegalStateException("生成的数据为空");
        }
        
        // 检查每一行数据
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> row = data.get(i);
            
            // 检查必填字段
            for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                String columnName = entry.getKey();
                Map<String, String> columnInfo = entry.getValue();
                
                // 检查非空字段
                if ("NO".equals(columnInfo.get("IS_NULLABLE"))) {
                    // 跳过自增主键
                    if ("PRI".equals(columnInfo.get("COLUMN_KEY")) && 
                        "auto_increment".equalsIgnoreCase(columnInfo.get("EXTRA"))) {
                        continue;
                    }
                    
                    // 检查字段值是否为空
                    if (!row.containsKey(columnName) || row.get(columnName) == null) {
                        // 对于username字段，自动生成默认值
                        if ("username".equals(columnName)) {
                            String uniqueUsername = "user_" + System.nanoTime() % 100000 + "_" + ThreadLocalRandom.current().nextInt(1, 10000);
                            
                            // 确保不超过字段长度限制
                            String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
                            if (maxLength != null) {
                                int limit = Integer.parseInt(maxLength);
                                if (uniqueUsername.length() > limit) {
                                    uniqueUsername = "u" + System.nanoTime() % 10000;
                                    if (uniqueUsername.length() > limit) {
                                        uniqueUsername = "u" + ThreadLocalRandom.current().nextInt(1, limit);
                                    }
                                }
                            }
                            
                            log.info("在验证阶段为username字段生成默认值: {}", uniqueUsername);
                            row.put(columnName, uniqueUsername);
                        } else {
                            // 为其他非空字段生成默认值
                            Object defaultValue = generateDefaultValue(columnInfo);
                            if (defaultValue != null) {
                                log.info("在验证阶段为字段 {} 生成默认值: {}", columnName, defaultValue);
                                row.put(columnName, defaultValue);
                            } else {
                                throw new IllegalStateException("字段 '" + columnName + "' 不能为空，但生成的数据中该字段为null");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 处理生成的数据，确保符合表结构约束
     */
    private List<Map<String, Object>> processGeneratedData(List<Map<String, Object>> data, 
            Map<String, Map<String, String>> tableColumns, Long currentMaxId, String tableName, 
            Map<String, ForeignKeyInfo> foreignKeys, Map<String, Set<Object>> generatedPrimaryKeys) throws SQLException {
        List<Map<String, Object>> processedData = new ArrayList<>();
        
        // 找到主键字段
        String primaryKeyColumn = null;
        boolean isAutoIncrement = false;
        String primaryKeyDataType = null;
        
        for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
            if ("PRI".equals(entry.getValue().get("COLUMN_KEY"))) {
                primaryKeyColumn = entry.getKey();
                isAutoIncrement = "auto_increment".equalsIgnoreCase(entry.getValue().get("EXTRA"));
                primaryKeyDataType = entry.getValue().get("DATA_TYPE");
                break;
            }
        }
        
        // 预先处理外键，确保有有效的引用值
        Map<String, List<Object>> validForeignKeyValues = new HashMap<>();
        for (Map.Entry<String, ForeignKeyInfo> entry : foreignKeys.entrySet()) {
            String columnName = entry.getKey();
            ForeignKeyInfo fkInfo = entry.getValue();
            
            // 确保外键字段在表中存在
            if (!tableColumns.containsKey(columnName)) {
                log.warn("外键字段 {} 在表 {} 中不存在，将被忽略", columnName, tableName);
                continue;
            }
            
            // 首先检查是否有已生成的主键可以使用
            Set<Object> referencedKeys = generatedPrimaryKeys.get(fkInfo.referencedTable);
            if (referencedKeys != null && !referencedKeys.isEmpty()) {
                log.info("使用已生成的主键值作为外键 {}.{} 的引用值，共 {} 个值", 
                    tableName, columnName, referencedKeys.size());
                validForeignKeyValues.put(columnName, new ArrayList<>(referencedKeys));
                continue;
            }
            
            // 如果没有已生成的主键，尝试从数据库获取
            List<Object> fkValues = getValidForeignKeyValues(fkInfo.referencedTable, fkInfo.referencedColumn, 50);
            if (fkValues != null && !fkValues.isEmpty()) {
                log.info("从数据库获取到 {} 个有效的外键值，用于 {}.{}", fkValues.size(), tableName, columnName);
                validForeignKeyValues.put(columnName, fkValues);
            } else {
                log.warn("无法获取表 {} 字段 {} 的有效外键值", fkInfo.referencedTable, fkInfo.referencedColumn);
                
                // 尝试创建引用记录
                try {
                    // 获取当前数据源
                    DataSource dataSource = getCurrentDataSource();
                    if (dataSource != null) {
                        try (Connection conn = getConnection(dataSource)) {
                            String dbName = getCurrentDatabaseName(conn);
                            // 检查引用表是否存在
                            try (Statement stmt = conn.createStatement()) {
                                try {
                                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + fkInfo.referencedTable);
                                    if (rs.next()) {
                                        int count = rs.getInt(1);
                                        log.info("引用表 {} 中有 {} 条记录", fkInfo.referencedTable, count);
                                        
                                        // 如果引用表中没有数据，尝试插入一条记录
                                        if (count == 0) {
                                            log.info("引用表 {} 中没有数据，尝试插入一条记录", fkInfo.referencedTable);
                                            
                                            // 获取引用表的结构
                                            Map<String, Map<String, String>> refTableColumns = getTableDefinition(conn, fkInfo.referencedTable);
                                            
                                            // 构建插入SQL
                                            StringBuilder insertSql = new StringBuilder();
                                            insertSql.append("INSERT INTO ").append(fkInfo.referencedTable).append(" (");
                                            
                                            List<String> columns = new ArrayList<>();
                                            List<Object> values = new ArrayList<>();
                                            
                                            for (Map.Entry<String, Map<String, String>> colEntry : refTableColumns.entrySet()) {
                                                String colName = colEntry.getKey();
                                                Map<String, String> colInfo = colEntry.getValue();
                                                
                                                // 跳过自增主键
                                                if ("PRI".equals(colInfo.get("COLUMN_KEY")) && 
                                                    "auto_increment".equalsIgnoreCase(colInfo.get("EXTRA"))) {
                                                    continue;
                                                }
                                                
                                                // 为非空字段生成默认值
                                                if ("NO".equals(colInfo.get("IS_NULLABLE"))) {
                                                    columns.add(colName);
                                                    
                                                    // 特殊处理user_id字段
                                                    if (colName.equals("user_id")) {
                                                        values.add(1); // 使用默认值1
                                                    } else if (colName.toLowerCase().contains("email") || "UNI".equals(colInfo.get("COLUMN_KEY"))) {
                                                        // 为唯一字段生成唯一值
                                                        String uniqueValue = colName + "_" + System.nanoTime() + "_" + UUID.randomUUID().toString().substring(0, 8);
                                                        // 确保不超过字段长度限制
                                                        String maxLength = colInfo.get("CHARACTER_MAXIMUM_LENGTH");
                                                        if (maxLength != null) {
                                                            int limit = Integer.parseInt(maxLength);
                                                            if (uniqueValue.length() > limit) {
                                                                uniqueValue = uniqueValue.substring(0, limit);
                                                            }
                                                        }
                                                        values.add(uniqueValue);
                                                    } else {
                                                        // 其他字段
                                                        values.add(generateDefaultValue(colInfo));
                                                    }
                                                }
                                            }
                                            
                                            // 构建完整的SQL语句
                                            if (!columns.isEmpty()) {
                                                insertSql.append(String.join(", ", columns)).append(") VALUES (");
                                                
                                                for (int i = 0; i < values.size(); i++) {
                                                    if (i > 0) {
                                                        insertSql.append(", ");
                                                    }
                                                    insertSql.append("?");
                                                }
                                                insertSql.append(")");
                                                
                                                // 执行插入
                                                try (PreparedStatement pstmt = conn.prepareStatement(insertSql.toString(), Statement.RETURN_GENERATED_KEYS)) {
                                                    for (int i = 0; i < values.size(); i++) {
                                                        pstmt.setObject(i + 1, values.get(i));
                                                    }
                                                    
                                                    int affectedRows = pstmt.executeUpdate();
                                                    log.info("插入记录结果: {} 行受影响", affectedRows);
                                                    
                                                    // 获取生成的主键
                                                    if (affectedRows > 0) {
                                                        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                                                            if (generatedKeys.next()) {
                                                                Object generatedId = generatedKeys.getObject(1);
                                                                log.info("获取到生成的主键: {}={}", fkInfo.referencedColumn, generatedId);
                                                                
                                                                // 将生成的主键添加到有效值列表
                                                                List<Object> validValues = new ArrayList<>();
                                                                validValues.add(generatedId);
                                                                validForeignKeyValues.put(columnName, validValues);
                                                                
                        continue;
                    }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        
                                        // 如果无法插入记录，尝试获取现有记录
                                        try (Statement stmt2 = conn.createStatement();
                                             ResultSet rs2 = stmt2.executeQuery("SELECT " + fkInfo.referencedColumn + " FROM " + fkInfo.referencedTable + " LIMIT 50")) {
                                            List<Object> validValues = new ArrayList<>();
                                            while (rs2.next()) {
                                                validValues.add(rs2.getObject(1));
                                            }
                                            
                                            if (!validValues.isEmpty()) {
                                                log.info("从表 {} 获取到 {} 个有效的外键值", fkInfo.referencedTable, validValues.size());
                                                validForeignKeyValues.put(columnName, validValues);
                                            } else {
                                                log.warn("表 {} 中没有有效的外键值", fkInfo.referencedTable);
                                            }
                                        }
                                    }
                                } catch (SQLException e) {
                                    log.warn("查询表 {} 失败: {}", fkInfo.referencedTable, e.getMessage());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("处理外键 {} 时发生错误: {}", columnName, e.getMessage());
                }
            }
        }
        
        // 处理每一行数据
        for (Map<String, Object> row : data) {
            Map<String, Object> processedRow = new HashMap<>(row);
            
            // 处理主键
            if (primaryKeyColumn != null) {
                if (isAutoIncrement) {
                    // 如果是自增主键，移除该字段，让数据库自动生成
                    processedRow.remove(primaryKeyColumn);
                } else {
                    // 如果不是自增主键，生成唯一值
                    if (primaryKeyDataType != null && primaryKeyDataType.toLowerCase().contains("int")) {
                        // 为整数类型主键生成唯一值
                        processedRow.put(primaryKeyColumn, ++currentMaxId);
                    } else {
                        // 为其他类型主键生成唯一值
                        processedRow.put(primaryKeyColumn, UUID.randomUUID().toString());
                    }
                }
            }
            
            // 处理非空字段
            for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                String columnName = entry.getKey();
                Map<String, String> columnInfo = entry.getValue();
                
                // 跳过主键和已处理的字段
                if (primaryKeyColumn != null && columnName.equals(primaryKeyColumn)) {
                    continue;
                }
                
                // 处理非空字段
                if ("NO".equals(columnInfo.get("IS_NULLABLE")) && 
                    (!processedRow.containsKey(columnName) || processedRow.get(columnName) == null)) {
                    // 特殊处理username字段
                    if ("username".equals(columnName)) {
                        String uniqueUsername = "user_" + System.nanoTime() % 100000 + "_" + ThreadLocalRandom.current().nextInt(1, 10000);
                        
                        // 确保不超过字段长度限制
                        String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
                        if (maxLength != null) {
                            int limit = Integer.parseInt(maxLength);
                            if (uniqueUsername.length() > limit) {
                                uniqueUsername = "u" + System.nanoTime() % 10000;
                                if (uniqueUsername.length() > limit) {
                                    uniqueUsername = "u" + ThreadLocalRandom.current().nextInt(1, limit);
                                }
                            }
                        }
                        
                        log.info("在处理数据阶段为username字段生成默认值: {}", uniqueUsername);
                        processedRow.put(columnName, uniqueUsername);
                    } else {
                        // 为其他非空字段生成默认值
                        Object defaultValue = generateDefaultValue(columnInfo);
                        processedRow.put(columnName, defaultValue);
                        log.debug("为字段 {} 设置默认值: {}", columnName, defaultValue);
                    }
                }
            }
            
            // 处理外键和唯一约束
            Set<String> uniqueColumns = new HashSet<>();
            for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                if ("UNI".equals(entry.getValue().get("COLUMN_KEY"))) {
                    uniqueColumns.add(entry.getKey());
                }
            }
            
            // 修复唯一约束冲突
            processedRow = fixUniqueConstraintViolation(processedRow, tableName, tableColumns, uniqueColumns);
            
            // 处理外键
            for (Map.Entry<String, ForeignKeyInfo> entry : foreignKeys.entrySet()) {
                String columnName = entry.getKey();
                ForeignKeyInfo fkInfo = entry.getValue();
                
                // 如果有有效的外键值，随机选择一个
                List<Object> validValues = validForeignKeyValues.get(columnName);
                if (validValues != null && !validValues.isEmpty()) {
                    Object randomValue = validValues.get(new Random().nextInt(validValues.size()));
                    processedRow.put(columnName, randomValue);
                    log.debug("为字段 {} 设置外键值: {}", columnName, randomValue);
                }
            }
            
            processedData.add(processedRow);
        }
        
        return processedData;
    }

    /**
     * 创建引用记录
     */
    private Object createReferenceRecord(String tableName, String columnName) {
        try {
            // 获取当前数据源
            DataSource dataSource = getCurrentDataSource();
            if (dataSource == null) {
                log.error("无法获取当前数据源");
                return null;
            }
            
            try (Connection conn = getConnection(dataSource)) {
                // 获取表结构
                Map<String, Map<String, String>> tableColumns = getTableDefinition(conn, tableName);
                
                // 获取表的外键信息
                Map<String, ForeignKeyInfo> tableForeignKeys = getForeignKeyInfo(tableName);
                
                // 先处理该表的外键依赖
                for (Map.Entry<String, ForeignKeyInfo> entry : tableForeignKeys.entrySet()) {
                    String fkColumnName = entry.getKey();
                    ForeignKeyInfo fkInfo = entry.getValue();
                    
                    // 递归创建引用记录
                    Object fkValue = getValidForeignKeyValue(fkInfo.referencedTable, fkInfo.referencedColumn);
                    if (fkValue == null) {
                        fkValue = createReferenceRecord(fkInfo.referencedTable, fkInfo.referencedColumn);
                        if (fkValue != null) {
                            log.info("为表 {} 的外键 {} 创建了引用记录，值为: {}", 
                                tableName, fkColumnName, fkValue);
                        } else {
                            log.warn("无法为表 {} 的外键 {} 创建引用记录", tableName, fkColumnName);
                            return null;
                        }
                    }
                }
                
                // 构建插入SQL
                StringBuilder insertSql = new StringBuilder();
                insertSql.append("INSERT INTO ").append(tableName).append(" (");
                insertSql.append(String.join(", ", tableColumns.keySet()));
                insertSql.append(") VALUES (");
                insertSql.append(String.join(", ", Collections.nCopies(tableColumns.size(), "?")));
                insertSql.append(")");
                
                log.info("准备执行插入SQL: {}", insertSql);
                
                // 生成默认值
                Map<String, Object> defaultValues = new HashMap<>();
                for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                    String colName = entry.getKey();
                    Map<String, String> columnInfo = entry.getValue();
                    
                    // 为主键生成特殊值
                    if ("PRI".equals(columnInfo.get("COLUMN_KEY"))) {
                        if ("auto_increment".equalsIgnoreCase(columnInfo.get("EXTRA"))) {
                            // 自增主键，不需要设置值
                            continue;
                        } else {
                            // 非自增主键，生成一个随机值
                            defaultValues.put(colName, System.nanoTime() % Integer.MAX_VALUE);
                        }
                    } else if (tableForeignKeys.containsKey(colName)) {
                        // 为外键设置有效值
                        ForeignKeyInfo fkInfo = tableForeignKeys.get(colName);
                        Object fkValue = getValidForeignKeyValue(fkInfo.referencedTable, fkInfo.referencedColumn);
                        if (fkValue != null) {
                            defaultValues.put(colName, fkValue);
                        } else {
                            log.warn("无法为外键 {} 获取有效值", colName);
                            return null;
                        }
                    } else if (colName.toLowerCase().contains("email") || "UNI".equals(columnInfo.get("COLUMN_KEY"))) {
                        // 为唯一字段生成唯一值
                        String uniqueValue = colName + "_" + System.nanoTime() + "_" + UUID.randomUUID().toString().substring(0, 8);
                        // 确保不超过字段长度限制
                        String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
                        if (maxLength != null) {
                            int limit = Integer.parseInt(maxLength);
                            if (uniqueValue.length() > limit) {
                                uniqueValue = uniqueValue.substring(0, limit);
                            }
                        }
                        defaultValues.put(colName, uniqueValue);
                    } else {
                        // 其他字段
                        defaultValues.put(colName, generateDefaultValue(columnInfo));
                    }
                }
                
                // 执行插入
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql.toString(), Statement.RETURN_GENERATED_KEYS)) {
                    int paramIndex = 1;
                    for (String colName : tableColumns.keySet()) {
                        if (!defaultValues.containsKey(colName)) {
                            continue; // 跳过自增主键
                        }
                        Object value = defaultValues.get(colName);
                        pstmt.setObject(paramIndex++, value);
                    }
                    
                    int affectedRows = pstmt.executeUpdate();
                    log.info("插入记录结果: {} 行受影响", affectedRows);
                    
                    // 获取生成的主键
                    if (affectedRows > 0) {
                        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                Object generatedId = generatedKeys.getObject(1);
                                log.info("获取到生成的主键: {}={}", columnName, generatedId);
                                return generatedId;
                            }
                        }
                    }
                    
                    // 如果无法获取生成的主键，查询表中的第一条记录
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT " + columnName + " FROM " + tableName + " LIMIT 1")) {
                        if (rs.next()) {
                            Object value = rs.getObject(columnName);
                            log.info("查询到的值: {}={}", columnName, value);
                            return value;
                        }
                    }
                } catch (Exception e) {
                    log.error("插入记录到引用表失败: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("创建引用记录时发生错误: {}", e.getMessage());
        }
        
        return null;
    }

    /**
     * 获取表的外键信息
     */
    private Map<String, ForeignKeyInfo> getForeignKeyInfo(String tableName) {
        Map<String, ForeignKeyInfo> foreignKeys = new HashMap<>();
        
        try {
            // 获取当前数据源
            DataSource dataSource = getCurrentDataSource();
            if (dataSource == null) {
                log.error("无法获取当前数据源");
                return foreignKeys;
            }
            
            try (Connection conn = getConnection(dataSource)) {
                DatabaseMetaData metaData = conn.getMetaData();
                
                try (ResultSet rs = metaData.getImportedKeys(conn.getCatalog(), null, tableName)) {
                    while (rs.next()) {
                        String fkColumnName = rs.getString("FKCOLUMN_NAME");
                        String pkTableName = rs.getString("PKTABLE_NAME");
                        String pkColumnName = rs.getString("PKCOLUMN_NAME");
                        
                        ForeignKeyInfo fkInfo = new ForeignKeyInfo();
                        fkInfo.columnName = fkColumnName;
                        fkInfo.referencedTable = pkTableName;
                        fkInfo.referencedColumn = pkColumnName;
                        
                        foreignKeys.put(fkColumnName, fkInfo);
                        log.info("发现外键: {}.{} 引用 {}.{}", tableName, fkColumnName, pkTableName, pkColumnName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("获取表 {} 的外键信息时发生错误: {}", tableName, e.getMessage());
        }
        
        return foreignKeys;
    }

    /**
     * 获取有效的外键值
     */
    public static Object getValidForeignKeyValue(String referencedTable, String referencedColumn) {
        try {
            // 获取当前数据源
            DataSource dataSource = getCurrentDataSource();
            if (dataSource == null) {
                log.error("无法获取当前数据源");
                return null;
            }
            
            try (Connection conn = getConnection(dataSource)) {
                // 查询引用表中的有效值
                String sql = String.format("SELECT %s FROM %s LIMIT 5", referencedColumn, referencedTable);
                log.info("查询有效外键值: {}", sql);
                
                List<Object> validValues = new ArrayList<>();
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        Object value = rs.getObject(referencedColumn);
                        if (value != null) {
                            validValues.add(value);
                            log.info("找到有效的外键值: {}={}", referencedColumn, value);
                        }
                    }
                }
                
                if (!validValues.isEmpty()) {
                    // 随机选择一个有效值
                    int randomIndex = (int) (Math.random() * validValues.size());
                    Object selectedValue = validValues.get(randomIndex);
                    log.info("选择的外键值: {}={}", referencedColumn, selectedValue);
                    return selectedValue;
                } else {
                    log.warn("表 {} 中没有找到 {} 的有效值", referencedTable, referencedColumn);
                    return null;
                }
            }
        } catch (Exception e) {
            log.error("获取有效外键值时发生错误: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 获取多个有效的外键值
     */
    private List<Object> getValidForeignKeyValues(String referencedTable, String referencedColumn, int limit) {
        try {
            // 获取当前数据源
            DataSource dataSource = getCurrentDataSource();
            if (dataSource == null) {
                log.error("无法获取当前数据源");
                return Collections.emptyList();
            }
            
            try (Connection conn = getConnection(dataSource)) {
                // 查询引用表中的有效值
                String sql = String.format("SELECT %s FROM %s LIMIT %d", referencedColumn, referencedTable, limit);
                log.info("查询有效外键值: {}", sql);
                
                List<Object> validValues = new ArrayList<>();
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        Object value = rs.getObject(referencedColumn);
                        if (value != null) {
                            validValues.add(value);
                        }
                    }
                }
                
                if (!validValues.isEmpty()) {
                    log.info("在表 {} 中找到 {} 个有效的 {} 值", referencedTable, validValues.size(), referencedColumn);
                    return validValues;
                } else {
                    log.warn("表 {} 中没有找到 {} 的有效值", referencedTable, referencedColumn);
                    return Collections.emptyList();
                }
            }
        } catch (Exception e) {
            log.error("获取有效外键值时发生错误: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 获取当前数据源
     */
    private static DataSource getCurrentDataSource() {
        try {
            return staticDataSourceService.list().stream()
                    .filter(ds -> ds.getDeleted() == 0)
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            log.error("获取当前数据源时发生错误: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 外键信息类
     */
    @Data
    private static class ForeignKeyInfo {
        private String columnName;
        private String referencedTable;
        private String referencedColumn;
    }

    /**
     * 生成默认值
     */
    private Object generateDefaultValue(Map<String, String> columnInfo) {
        if (columnInfo == null) {
            log.warn("列信息为空,返回空字符串");
            return "";
        }

        String dataType = columnInfo.get("DATA_TYPE");
        if (dataType == null) {
            log.warn("数据类型为空,返回空字符串");
            return "";
        }

        String columnName = columnInfo.get("COLUMN_NAME");
        if (columnName == null) {
            log.warn("列名为空,返回空字符串");
            return "";
        }
        
        // 特殊处理user_id字段
        if ("user_id".equals(columnName)) {
            try {
                // 获取当前数据源
                DataSource dataSource = getCurrentDataSource();
                if (dataSource != null) {
                    try (Connection conn = getConnection(dataSource)) {
                        String dbName = getCurrentDatabaseName(conn);
                        try (Statement stmt = conn.createStatement()) {
                            try {
                                // 使用当前数据库名称查询users表
                                ResultSet rs = stmt.executeQuery("SELECT id FROM users ORDER BY RAND() LIMIT 1");
                                if (rs.next()) {
                                    return rs.getObject(1);
                                }
                            } catch (SQLException e) {
                                log.warn("查询users表失败: {}", e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("获取user_id时发生错误: {}", e.getMessage());
            }
            return 1; // 默认值
        }
        
        // 特殊处理username字段
        if ("username".equals(columnName)) {
            // 生成唯一的用户名
            String uniqueUsername = "user_" + System.nanoTime() % 100000 + "_" + ThreadLocalRandom.current().nextInt(1, 10000);
            
            // 确保不超过字段长度限制
            String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
            if (maxLength != null) {
                try {
                    int limit = Integer.parseInt(maxLength);
                    if (uniqueUsername.length() > limit) {
                        uniqueUsername = "u" + System.nanoTime() % 10000;
                        if (uniqueUsername.length() > limit) {
                            uniqueUsername = "u" + ThreadLocalRandom.current().nextInt(1, limit);
                        }
                    }
                } catch (NumberFormatException e) {
                    log.warn("解析CHARACTER_MAXIMUM_LENGTH失败: {}", e.getMessage());
                }
            }
            
            log.info("为username字段生成默认值: {}", uniqueUsername);
            return uniqueUsername;
        }
        
        // 处理其他数据类型
        switch (dataType.toLowerCase()) {
            case "int":
            case "bigint":
            case "tinyint":
            case "smallint":
            case "mediumint":
                return ThreadLocalRandom.current().nextInt(1, 1000);
            case "decimal":
            case "double":
            case "float":
                return ThreadLocalRandom.current().nextDouble(1, 1000);
            case "varchar":
            case "char":
            case "text":
                // 为唯一字段生成唯一值
                if ("UNI".equals(columnInfo.get("COLUMN_KEY")) || columnName.toLowerCase().contains("email")) {
                    String uniqueValue = columnName + "_" + System.nanoTime() + "_" + UUID.randomUUID().toString().substring(0, 8);
                    // 确保不超过字段长度限制
                    String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
                    if (maxLength != null) {
                        try {
                            int limit = Integer.parseInt(maxLength);
                            if (uniqueValue.length() > limit) {
                                uniqueValue = uniqueValue.substring(0, limit);
                            }
                        } catch (NumberFormatException e) {
                            log.warn("解析CHARACTER_MAXIMUM_LENGTH失败: {}", e.getMessage());
                        }
                    }
                    return uniqueValue;
                }
                return columnName + "_" + ThreadLocalRandom.current().nextInt(1, 1000);
            case "date":
                return LocalDate.now().minusDays(ThreadLocalRandom.current().nextInt(365));
            case "datetime":
            case "timestamp":
                return LocalDateTime.now().minusDays(ThreadLocalRandom.current().nextInt(365));
            case "time":
                return LocalTime.now();
            case "year":
                return Year.now().getValue() - ThreadLocalRandom.current().nextInt(10);
            case "enum":
                // 从枚举值中随机选择一个
                String enumValues = columnInfo.get("COLUMN_TYPE");
                if (enumValues != null && enumValues.startsWith("enum(") && enumValues.endsWith(")")) {
                    String[] values = enumValues.substring(5, enumValues.length() - 1).split(",");
                    if (values.length > 0) {
                        String value = values[ThreadLocalRandom.current().nextInt(values.length)];
                        // 移除引号
                        return value.substring(1, value.length() - 1);
                    }
                }
                return "";
            default:
                return "";
        }
    }

    /**
     * 从数据库获取最新的任务状态
     */
    private DataTask getLatestTaskStatus(Long taskId) {
        try {
            // 直接从数据库查询,绕过缓存
            return taskMapper.selectById(taskId);
        } catch (Exception e) {
            log.error("获取最新任务状态失败: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 解锁MySQL数据库中的所有表
     */
    private void unlockAllTables(DataSource dataSource) {
        if (!"MYSQL".equals(dataSource.getType())) {
            return;
        }
        
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection(dataSource);
            stmt = conn.createStatement();
            log.info("解锁所有表");
            stmt.execute("UNLOCK TABLES");
        } catch (Exception e) {
            log.warn("解锁所有表失败: {}", e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    log.warn("关闭Statement失败: {}", e.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    log.warn("关闭Connection失败: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 在应用启动时解锁所有表
     */
    @PostConstruct
    public void initUnlockAllTables() {
        log.info("应用启动，尝试解锁所有数据库中的表");
        try {
            List<DataSource> dataSources = dataSourceService.list();
            for (DataSource dataSource : dataSources) {
                if ("MYSQL".equals(dataSource.getType()) || "POSTGRESQL".equals(dataSource.getType())) {
                    log.info("解锁数据源 {} 中的所有表", dataSource.getName());
                    unlockAllTables(dataSource);
                }
            }
        } catch (Exception e) {
            log.warn("应用启动时解锁表失败: {}", e.getMessage());
        }
    }

    /**
     * 修复唯一约束冲突
     */
    private Map<String, Object> fixUniqueConstraintViolation(Map<String, Object> row, String tableName, 
            Map<String, Map<String, String>> tableColumns, Set<String> uniqueColumns) {
        Map<String, Object> fixedRow = new HashMap<>(row);
        
        for (String uniqueColumn : uniqueColumns) {
            // 特殊处理user_id字段
            if ("user_id".equals(uniqueColumn)) {
                try {
                    // 获取当前数据源
                    DataSource dataSource = getCurrentDataSource();
                    if (dataSource != null) {
                        try (Connection conn = getConnection(dataSource)) {
                            String dbName = getCurrentDatabaseName(conn);
                            try (Statement stmt = conn.createStatement()) {
                                try {
                                    // 使用当前数据库名称查询users表
                                    ResultSet rs = stmt.executeQuery("SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM " + tableName + ") ORDER BY RAND() LIMIT 1");
                                    if (rs.next()) {
                                        Object userId = rs.getObject(1);
                                        log.info("为表 {} 找到未使用的user_id: {}", tableName, userId);
                                        fixedRow.put(uniqueColumn, userId);
                    } else {
                                        log.warn("无法找到未使用的user_id，生成随机值");
                                        fixedRow.put(uniqueColumn, ThreadLocalRandom.current().nextInt(1, 1000));
                                    }
                                } catch (SQLException e) {
                                    log.warn("查询users表失败: {}", e.getMessage());
                                    fixedRow.put(uniqueColumn, ThreadLocalRandom.current().nextInt(1, 1000));
                        }
                    }
                }
            }
        } catch (Exception e) {
                    log.warn("获取user_id时发生错误: {}", e.getMessage());
                    fixedRow.put(uniqueColumn, ThreadLocalRandom.current().nextInt(1, 1000));
                }
                continue;
            }
            
            // 处理其他唯一字段
            Map<String, String> columnInfo = tableColumns.get(uniqueColumn);
            if (columnInfo != null) {
        String dataType = columnInfo.get("DATA_TYPE");
                
                if (dataType.toLowerCase().contains("char") || dataType.toLowerCase().contains("text")) {
                    // 为字符类型字段生成唯一值
                    String uniqueValue = uniqueColumn + "_" + System.nanoTime() + "_" + UUID.randomUUID().toString().substring(0, 8);
                    
                    // 确保不超过字段长度限制
                    String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
                    if (maxLength != null) {
                        int limit = Integer.parseInt(maxLength);
                        if (uniqueValue.length() > limit) {
                            uniqueValue = uniqueValue.substring(0, limit);
                        }
                    }
                    
                    fixedRow.put(uniqueColumn, uniqueValue);
                } else if (dataType.toLowerCase().contains("int")) {
                    // 为整数类型字段生成唯一值
                    fixedRow.put(uniqueColumn, System.nanoTime() % 1000000);
                }
            }
        }
        
        return fixedRow;
    }

    /**
     * 执行插入操作
     */
    private void executeInsert(String tableName, List<Map<String, Object>> data, 
            Map<String, Map<String, String>> tableColumns, Map<String, ForeignKeyInfo> foreignKeys) throws SQLException {
        // ... existing code ...
        
        // 处理每一行数据
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> currentRow = data.get(i);
            
            // 修复可能的唯一约束冲突和外键约束问题
            Set<String> uniqueColumns = new HashSet<>();
            for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                if ("UNI".equals(entry.getValue().get("COLUMN_KEY"))) {
                    uniqueColumns.add(entry.getKey());
                }
            }
            currentRow = fixUniqueConstraintViolation(currentRow, tableName, tableColumns, uniqueColumns);
            
            // ... existing code ...
        }
        
        // ... existing code ...
    }

    /**
     * 为单个字段生成模板
     */
    private ObjectNode generateFieldTemplate(String columnName, Map<String, String> columnInfo) {
        String dataType = columnInfo.get("DATA_TYPE");
        if (dataType == null) {
            return null;
        }

        ObjectNode fieldNode = objectMapper.createObjectNode();
        ObjectNode paramsNode = objectMapper.createObjectNode();

        // 获取字段的元数据信息
        String columnComment = columnInfo.get("COLUMN_COMMENT");
        String columnType = columnInfo.get("COLUMN_TYPE");
        String maxLength = columnInfo.get("CHARACTER_MAXIMUM_LENGTH");
        String numericPrecision = columnInfo.get("NUMERIC_PRECISION");
        String numericScale = columnInfo.get("NUMERIC_SCALE");
        String isNullable = columnInfo.get("IS_NULLABLE");
        String columnKey = columnInfo.get("COLUMN_KEY");
        String columnDefault = columnInfo.get("COLUMN_DEFAULT");

        // 检查是否是外键
        String referencedTable = columnInfo.get("REFERENCED_TABLE");
        if (referencedTable != null) {
            fieldNode.put("type", "foreignKey");
            paramsNode.put("table", referencedTable);
            fieldNode.set("params", paramsNode);
            return fieldNode;
        }

        // 检查是否是枚举类型
        if (columnType != null && columnType.startsWith("enum(")) {
            fieldNode.put("type", "enum");
            Set<String> enumValues = extractEnumValues(columnType, columnComment);
            if (!enumValues.isEmpty()) {
                ArrayNode valuesNode = objectMapper.createArrayNode();
                enumValues.forEach(valuesNode::add);
                paramsNode.set("values", valuesNode);
            }
            fieldNode.set("params", paramsNode);
            return fieldNode;
        }

        // 根据MySQL数据类型设置生成规则
        switch (dataType.toLowerCase()) {
            case "bit":
                fieldNode.put("type", "random");
                paramsNode.put("min", 0);
                paramsNode.put("max", 1);
                paramsNode.put("integer", true);
                break;

            case "tinyint":
                fieldNode.put("type", "random");
                paramsNode.put("min", -128);
                paramsNode.put("max", 127);
                paramsNode.put("integer", true);
                break;

            case "smallint":
                fieldNode.put("type", "random");
                paramsNode.put("min", -32768);
                paramsNode.put("max", 32767);
                paramsNode.put("integer", true);
                break;

            case "mediumint":
                fieldNode.put("type", "random");
                paramsNode.put("min", -8388608);
                paramsNode.put("max", 8388607);
                paramsNode.put("integer", true);
                break;

            case "int":
            case "integer":
                fieldNode.put("type", "random");
                paramsNode.put("min", 1);
                paramsNode.put("max", 1000);
                paramsNode.put("integer", true);
                break;

            case "bigint":
                fieldNode.put("type", "random");
                paramsNode.put("min", 1);
                paramsNode.put("max", 1000);
                paramsNode.put("integer", true);
                break;

            case "decimal":
            case "float":
            case "double":
                fieldNode.put("type", "random");
                setDecimalRange(paramsNode, columnName, columnComment, numericPrecision, numericScale);
                break;

            case "date":
                fieldNode.put("type", "date");
                setDateRange(paramsNode, "yyyy-MM-dd", columnComment);
                break;

            case "datetime":
            case "timestamp":
                fieldNode.put("type", "date");
                setDateRange(paramsNode, "yyyy-MM-dd HH:mm:ss", columnComment);
                break;

            case "time":
                fieldNode.put("type", "date");
                setDateRange(paramsNode, "HH:mm:ss", columnComment);
                break;

            case "year":
                fieldNode.put("type", "date");
                setDateRange(paramsNode, "yyyy", columnComment);
                break;

            case "char":
            case "varchar":
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                fieldNode.put("type", "string");
                // 如果有最大长度限制，使用它
                if (maxLength != null) {
                    paramsNode.put("length", Math.min(Integer.parseInt(maxLength), 20));
                } else {
                    paramsNode.put("length", 20); // 默认长度
                }
                break;

            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                fieldNode.put("type", "string");
                paramsNode.put("length", 10);
                break;

            case "json":
                fieldNode.put("type", "string");
                paramsNode.put("pattern", "{}");
                break;

            default:
                fieldNode.put("type", "string");
                paramsNode.put("length", 10);
                break;
        }

        fieldNode.set("params", paramsNode);
        return fieldNode;
    }

    /**
     * 自动从数据库元数据生成模板
     */
    private String generateTemplateFromMetadata(DataSource dataSource, String tableName) {
        try (Connection conn = getConnection(dataSource)) {
            Map<String, Map<String, String>> tableColumns = getTableDefinition(conn, tableName);
            Map<String, ForeignKeyInfo> foreignKeys = getForeignKeyInfo(tableName);
            ObjectNode template = objectMapper.createObjectNode();

            for (Map.Entry<String, Map<String, String>> entry : tableColumns.entrySet()) {
                String columnName = entry.getKey();
                Map<String, String> columnInfo = entry.getValue();

                // 如果是外键字段
                if (foreignKeys.containsKey(columnName)) {
                    ForeignKeyInfo fkInfo = foreignKeys.get(columnName);
                    ObjectNode fieldNode = objectMapper.createObjectNode();
                    fieldNode.put("type", "foreignKey");
                    fieldNode.put("referencedTable", fkInfo.getReferencedTable());
                    fieldNode.put("referencedColumn", fkInfo.getReferencedColumn());
                    template.set(columnName, fieldNode);
                    continue;
                }

                // 处理其他字段类型
                // ... existing code ...
            }

            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(template);
        } catch (Exception e) {
            log.error("生成模板时发生错误: {}", e.getMessage(), e);
            throw new RuntimeException("生成模板失败: " + e.getMessage());
        }
    }
} 