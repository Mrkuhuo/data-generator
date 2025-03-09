package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("system_info")
public class SystemInfo {
    
    @TableId(type = IdType.AUTO)
    private Long id;
    
    /**
     * CPU使用率
     */
    private Double cpuUsage;
    
    /**
     * 内存使用率
     */
    private Double memoryUsage;
    
    /**
     * 磁盘使用率
     */
    private Double diskUsage;
    
    /**
     * JVM堆内存使用率
     */
    private Double jvmHeapUsage;
    
    /**
     * JVM非堆内存使用率
     */
    private Double jvmNonHeapUsage;
    
    /**
     * 系统运行时间(毫秒)
     */
    private Long uptime;
    
    /**
     * 创建时间
     */
    private LocalDateTime createTime;
} 