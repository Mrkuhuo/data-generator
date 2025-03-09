package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.SystemAlertRule;
import com.datagenerator.entity.SystemInfo;
import com.datagenerator.mapper.SystemAlertRuleMapper;
import com.datagenerator.service.SystemAlertRuleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class SystemAlertRuleServiceImpl extends ServiceImpl<SystemAlertRuleMapper, SystemAlertRule> implements SystemAlertRuleService {

    @Override
    public List<SystemAlertRule> getEnabledRules() {
        LambdaQueryWrapper<SystemAlertRule> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(SystemAlertRule::getEnabled, true);
        return list(wrapper);
    }

    @Override
    public void checkSystemMetrics(SystemInfo systemInfo) {
        List<SystemAlertRule> rules = getEnabledRules();
        for (SystemAlertRule rule : rules) {
            try {
                checkRule(rule, systemInfo);
            } catch (Exception e) {
                log.error("检查告警规则失败: {}", rule.getName(), e);
            }
        }
    }
    
    /**
     * 检查单个告警规则
     */
    private void checkRule(SystemAlertRule rule, SystemInfo systemInfo) {
        Double value = getMetricValue(rule.getMetric(), systemInfo);
        if (value != null && value >= rule.getThreshold()) {
            // TODO: 发送告警通知
            log.warn("触发告警规则: {}, 指标: {}, 当前值: {}, 阈值: {}", 
                    rule.getName(), rule.getMetric(), value, rule.getThreshold());
        }
    }
    
    /**
     * 获取指标值
     */
    private Double getMetricValue(String metric, SystemInfo systemInfo) {
        switch (metric) {
            case "cpu":
                return systemInfo.getCpuUsage();
            case "memory":
                return systemInfo.getMemoryUsage();
            case "disk":
                return systemInfo.getDiskUsage();
            case "jvm_heap":
                return systemInfo.getJvmHeapUsage();
            case "jvm_non_heap":
                return systemInfo.getJvmNonHeapUsage();
            default:
                return null;
        }
    }
} 