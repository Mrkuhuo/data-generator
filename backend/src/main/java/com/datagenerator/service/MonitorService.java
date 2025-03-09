package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.AlertRecord;
import com.datagenerator.entity.AlertRule;
import com.datagenerator.entity.Metric;

public interface MonitorService extends IService<Metric> {
    /**
     * 更新指标值
     */
    void updateMetric(Long metricId, Double value);

    /**
     * 检查告警规则
     */
    void checkAlertRules(Long metricId);

    /**
     * 处理告警
     */
    void handleAlert(Long alertRecordId, String handler, String handleNote);
} 