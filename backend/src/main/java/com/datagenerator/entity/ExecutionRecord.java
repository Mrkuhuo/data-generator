package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("task_execution")
public class ExecutionRecord {
    @TableId(type = IdType.AUTO)
    private Long id;
    
    private Long taskId;
    
    private LocalDateTime startTime;
    
    private LocalDateTime endTime;
    
    private String status;
    
    private Long totalCount;
    
    private Long successCount;
    
    private Long errorCount;
    
    private String errorMessage;
    
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
    
    @TableLogic
    private Integer deleted;
} 