package com.datagenerator.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("system_alert_rule")
public class SystemAlertRule {
    
    @TableId(type = IdType.AUTO)
    private Long id;
    
    /**
     * 规则名称
     */
    private String name;
    
    /**
     * 监控指标
     */
    private String metric;
    
    /**
     * 告警阈值
     */
    private Double threshold;
    
    /**
     * 告警级别(1:低 2:中 3:高)
     */
    private Integer level;
    
    /**
     * 告警方式(1:邮件 2:短信 3:webhook)
     */
    private String notifyType;
    
    /**
     * 告警接收人
     */
    private String receivers;
    
    /**
     * 是否启用
     */
    private Boolean enabled;
    
    /**
     * 创建时间
     */
    private LocalDateTime createTime;
    
    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
} 