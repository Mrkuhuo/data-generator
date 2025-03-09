package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("data_task")
public class DataTask {
    @TableId(type = IdType.AUTO)
    private Long id;
    
    private String name;
    private Long dataSourceId;
    private String targetType; // TABLE, TOPIC
    private String targetName; // 表名或主题名
    private String writeMode; // OVERWRITE, APPEND, UPDATE
    private String dataFormat; // JSON, AVRO, PROTOBUF
    private String template; // 数据生成模板
    private Integer batchSize;
    private Integer frequency; // 生成频率（秒）
    private Integer concurrentNum;
    private String status; // RUNNING, STOPPED, COMPLETED, FAILED
    private String cronExpression; // 定时任务表达式
    
    @TableField(exist = false)
    private Long maxId; // 用于记录当前最大ID
    
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
    
    @TableLogic
    private Integer deleted;
} 