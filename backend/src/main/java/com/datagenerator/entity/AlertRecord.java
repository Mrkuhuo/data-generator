package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("alert_record")
public class AlertRecord {
    @TableId(type = IdType.AUTO)
    private Long id;
    
    private Long alertRuleId;
    
    private Long metricId;
    
    private String name;
    
    private String description;
    
    private Double value;
    
    private Double threshold;
    
    private Integer severity;
    
    private Integer status; // 0: 未处理, 1: 已处理
    
    private String handler;
    
    private String handleNote;
    
    private LocalDateTime handleTime;
    
    private Integer deleted;
    
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
} 