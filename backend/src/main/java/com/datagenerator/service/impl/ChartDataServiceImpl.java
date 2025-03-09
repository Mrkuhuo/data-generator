package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datagenerator.dto.ChartDataDTO;
import com.datagenerator.entity.SystemInfo;
import com.datagenerator.mapper.SystemInfoMapper;
import com.datagenerator.service.ChartDataService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class ChartDataServiceImpl implements ChartDataService {

    @Resource
    private SystemInfoMapper systemInfoMapper;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final List<String> SUPPORTED_METRICS = Arrays.asList(
            "cpu", "memory", "disk", "jvm_heap", "jvm_non_heap"
    );

    @Override
    public ChartDataDTO getSystemMetricData(String metric, LocalDateTime startTime, LocalDateTime endTime) {
        // 查询时间范围内的系统信息
        LambdaQueryWrapper<SystemInfo> wrapper = new LambdaQueryWrapper<>();
        wrapper.between(SystemInfo::getCreateTime, startTime, endTime)
                .orderByAsc(SystemInfo::getCreateTime);
        List<SystemInfo> systemInfos = systemInfoMapper.selectList(wrapper);

        // 构建图表数据
        ChartDataDTO chartData = new ChartDataDTO();
        List<String> times = new ArrayList<>();
        List<Double> values = new ArrayList<>();

        for (SystemInfo info : systemInfos) {
            times.add(info.getCreateTime().format(TIME_FORMATTER));
            values.add(getMetricValue(metric, info));
        }

        chartData.setTimes(times);
        chartData.setValues(values);
        chartData.setMetric(metric);
        chartData.setDescription(getMetricDescription(metric));

        return chartData;
    }

    @Override
    public List<String> getSupportedMetrics() {
        return SUPPORTED_METRICS;
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

    /**
     * 获取指标描述
     */
    private String getMetricDescription(String metric) {
        switch (metric) {
            case "cpu":
                return "CPU使用率(%)";
            case "memory":
                return "内存使用率(%)";
            case "disk":
                return "磁盘使用率(%)";
            case "jvm_heap":
                return "JVM堆内存使用率(%)";
            case "jvm_non_heap":
                return "JVM非堆内存使用率(%)";
            default:
                return "";
        }
    }
} 