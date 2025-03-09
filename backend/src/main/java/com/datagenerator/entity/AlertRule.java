package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("alert_rule")
public class AlertRule {
    @TableId(type = IdType.AUTO)
    private Long id;
    
    private Long metricId;
    
    private String name;
    
    private String description;
    
    private String operator; // >, <, >=, <=, ==, !=
    
    private Double threshold;
    
    private Integer duration; // 持续时间（分钟）
    
    private Integer severity; // 1: 低, 2: 中, 3: 高
    
    private Integer status; // 0: 停用, 1: 启用
    
    private Integer deleted;
    
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
} 