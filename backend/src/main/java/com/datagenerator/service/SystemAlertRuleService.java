package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.SystemAlertRule;
import com.datagenerator.entity.SystemInfo;

import java.util.List;

public interface SystemAlertRuleService extends IService<SystemAlertRule> {
    
    /**
     * 获取启用的告警规则
     */
    List<SystemAlertRule> getEnabledRules();
    
    /**
     * 检查系统指标是否触发告警
     */
    void checkSystemMetrics(SystemInfo systemInfo);
} 