package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.AlertRecord;
import com.datagenerator.entity.AlertRule;
import com.datagenerator.entity.Metric;
import com.datagenerator.mapper.MetricMapper;
import com.datagenerator.service.AlertRecordService;
import com.datagenerator.service.AlertRuleService;
import com.datagenerator.service.MonitorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MonitorServiceImpl extends ServiceImpl<MetricMapper, Metric> implements MonitorService {

    private final AlertRuleService alertRuleService;
    private final AlertRecordService alertRecordService;

    @Override
    public void updateMetric(Long metricId, Double value) {
        Metric metric = getById(metricId);
        if (metric == null) {
            throw new RuntimeException("指标不存在");
        }
        
        metric.setValue(value);
        updateById(metric);
        
        // 检查告警规则
        checkAlertRules(metricId);
    }

    @Override
    public void checkAlertRules(Long metricId) {
        // 获取指标
        Metric metric = getById(metricId);
        if (metric == null) {
            return;
        }
        
        // 获取告警规则
        LambdaQueryWrapper<AlertRule> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(AlertRule::getMetricId, metricId)
                .eq(AlertRule::getStatus, 1)
                .eq(AlertRule::getDeleted, 0);
        List<AlertRule> rules = alertRuleService.list(wrapper);
        
        // 检查每个规则
        for (AlertRule rule : rules) {
            boolean triggered = false;
            switch (rule.getOperator()) {
                case ">":
                    triggered = metric.getValue() > rule.getThreshold();
                    break;
                case "<":
                    triggered = metric.getValue() < rule.getThreshold();
                    break;
                case ">=":
                    triggered = metric.getValue() >= rule.getThreshold();
                    break;
                case "<=":
                    triggered = metric.getValue() <= rule.getThreshold();
                    break;
                case "==":
                    triggered = metric.getValue().equals(rule.getThreshold());
                    break;
                case "!=":
                    triggered = !metric.getValue().equals(rule.getThreshold());
                    break;
            }
            
            if (triggered) {
                // 创建告警记录
                AlertRecord record = new AlertRecord();
                record.setAlertRuleId(rule.getId());
                record.setMetricId(metricId);
                record.setName(rule.getName());
                record.setDescription(rule.getDescription());
                record.setValue(metric.getValue());
                record.setThreshold(rule.getThreshold());
                record.setSeverity(rule.getSeverity());
                record.setStatus(0);
                alertRecordService.save(record);
                
                // 更新指标状态
                metric.setStatus(rule.getSeverity());
                updateById(metric);
            }
        }
    }

    @Override
    public void handleAlert(Long alertRecordId, String handler, String handleNote) {
        AlertRecord record = alertRecordService.getById(alertRecordId);
        if (record == null) {
            throw new RuntimeException("告警记录不存在");
        }
        
        record.setStatus(1);
        record.setHandler(handler);
        record.setHandleNote(handleNote);
        record.setHandleTime(LocalDateTime.now());
        alertRecordService.updateById(record);
        
        // 更新指标状态
        Metric metric = getById(record.getMetricId());
        if (metric != null) {
            metric.setStatus(0);
            updateById(metric);
        }
    }
} 