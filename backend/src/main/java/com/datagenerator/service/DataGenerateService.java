package com.datagenerator.service;

import com.datagenerator.entity.DataTask;
import com.datagenerator.entity.DataSource;
import com.datagenerator.entity.ExecutionRecord;
import com.datagenerator.generator.DataGenerator;
import com.datagenerator.generator.DataGeneratorFactory;
import com.datagenerator.generator.impl.RelationalDatabaseGenerator;
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
import java.util.concurrent.TimeUnit;
import com.datagenerator.mapper.TaskMapper;
import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Arrays;

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
    private final RelationalDatabaseGenerator relationalDatabaseGenerator;
    
    // 存储每个任务的Kafka生产者工厂
    private final Map<Long, ProducerFactory<String, String>> producerFactories = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        staticDataSourceService = dataSourceService;
    }

    /**
     * 执行数据生成任务
     */
    public void executeTask(DataTask task) {
        log.info("开始执行数据生成任务：{}", task.getId());
        
        // 检查任务状态
        if (!"RUNNING".equals(task.getStatus())) {
            log.warn("任务 {} 状态不是运行中状态，跳过执行", task.getId());
                return;
            }
            
            // 获取数据源
        DataSource dataSource = dataSourceService.getById(task.getDataSourceId());
        if (dataSource == null || dataSource.getDeleted() == 1) {
            log.error("任务 {} 的数据源不存在或已删除", task.getId());
                return;
            }
            
        // 创建执行记录
        ExecutionRecord record = new ExecutionRecord();
        record.setTaskId(task.getId());
        record.setStartTime(LocalDateTime.now());
        record.setStatus("RUNNING");
        executionRecordService.save(record);
        
        try {
            // 根据数据源类型处理
                if ("KAFKA".equals(dataSource.getType())) {
                processKafkaTask(task, dataSource);
                } else {
                // 处理关系型数据库任务
                String[] tables = task.getTargetName().split(",");
                relationalDatabaseGenerator.generateData(task, dataSource, tables);
            }
            
            // 更新执行记录状态
            record.setStatus("SUCCESS");
            record.setEndTime(LocalDateTime.now());
            executionRecordService.updateById(record);
            
            // 更新任务最后执行时间，但不改变任务状态
            task.setUpdateTime(LocalDateTime.now());
            taskMapper.updateById(task);
            
            log.info("任务 {} 执行完成", task.getId());
            
            } catch (Exception e) {
            log.error("任务 {} 执行失败：{}", task.getId(), e.getMessage(), e);
            
            // 更新执行记录状态
            record.setStatus("FAILED");
            record.setEndTime(LocalDateTime.now());
            record.setErrorMessage(e.getMessage());
            executionRecordService.updateById(record);
            
            // 更新任务最后执行时间，但不改变任务状态
            task.setUpdateTime(LocalDateTime.now());
            taskMapper.updateById(task);
        }
    }

    /**
     * 处理Kafka任务
     */
    private void processKafkaTask(DataTask task, DataSource dataSource) {
        log.info("开始处理Kafka任务：{}", task.getId());
        
        try {
            // 获取或创建Kafka生产者工厂
            ProducerFactory<String, String> producerFactory = producerFactories.computeIfAbsent(
                task.getId(),
                id -> {
                    Map<String, Object> configs = new HashMap<>();
                    configs.put("bootstrap.servers", dataSource.getUrl());
                    configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    return new DefaultKafkaProducerFactory<>(configs);
                }
            );

            // 创建数据生成器
            DataGenerator generator = dataGeneratorFactory.createGenerator(task.getDataFormat());
            
            // 生成并发送数据
            List<Map<String, Object>> data = generator.generate(task.getTemplate(), task.getBatchSize());
            for (Map<String, Object> record : data) {
                String value = JsonUtil.toJson(record);
                kafkaProducer.send(task.getTargetName(), value);
            }
            
            log.info("Kafka任务 {} 数据生成完成，共生成 {} 条记录", task.getId(), data.size());
            
                        } catch (Exception e) {
            log.error("Kafka任务处理失败：{}", e.getMessage(), e);
                            throw e;
                        }
    }

    /**
     * 强制关闭Kafka连接
     */
    public void forceCloseKafkaConnections(Long taskId) {
        ProducerFactory<String, String> factory = producerFactories.remove(taskId);
        if (factory != null) {
            try {
                factory.reset();
                log.info("成功关闭任务 {} 的Kafka连接", taskId);
        } catch (Exception e) {
                log.error("关闭任务 {} 的Kafka连接时发生错误：{}", taskId, e.getMessage());
            }
        }
    }
} 