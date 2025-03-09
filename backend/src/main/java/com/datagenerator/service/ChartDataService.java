package com.datagenerator.service;

import com.datagenerator.dto.ChartDataDTO;

import java.time.LocalDateTime;
import java.util.List;

public interface ChartDataService {
    
    /**
     * 获取系统指标图表数据
     */
    ChartDataDTO getSystemMetricData(String metric, LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 获取所有支持的指标列表
     */
    List<String> getSupportedMetrics();
} 