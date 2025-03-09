package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("metric")
public class Metric {
    @TableId(type = IdType.AUTO)
    private Long id;
    
    private Long taskId;
    
    private String name;
    
    private String description;
    
    private String type; // counter, gauge, histogram
    
    private Double value;
    
    private String unit;
    
    private Integer status; // 0: 正常, 1: 警告, 2: 错误
    
    private Integer deleted;
    
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
} 