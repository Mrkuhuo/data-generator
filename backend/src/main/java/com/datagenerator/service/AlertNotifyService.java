package com.datagenerator.service;

import com.datagenerator.entity.SystemAlertRule;

public interface AlertNotifyService {
    
    /**
     * 发送告警通知
     */
    void sendAlert(SystemAlertRule rule, String message);
    
    /**
     * 获取通知类型
     */
    String getNotifyType();
} 