package com.datagenerator.controller;

import com.datagenerator.annotation.PerformanceMonitor;
import com.datagenerator.common.Result;
import com.datagenerator.dto.ChartDataDTO;
import com.datagenerator.service.ChartDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

@Tag(name = "图表数据", description = "系统监控图表数据接口")
@RestController
@RequestMapping("/chart")
public class ChartDataController {

    @Resource
    private ChartDataService chartDataService;

    @Operation(summary = "获取系统指标图表数据", description = "获取指定时间范围内的系统指标数据")
    @PerformanceMonitor(name = "获取图表数据", logParams = true, logResult = false)
    @GetMapping("/system-metric")
    public Result<ChartDataDTO> getSystemMetricData(
            @Parameter(description = "指标名称") @RequestParam String metric,
            @Parameter(description = "开始时间") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @Parameter(description = "结束时间") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        return Result.success(chartDataService.getSystemMetricData(metric, startTime, endTime));
    }

    @Operation(summary = "获取支持的指标列表", description = "获取所有可用的系统监控指标")
    @PerformanceMonitor(name = "获取指标列表", logParams = false, logResult = true)
    @GetMapping("/metrics")
    public Result<List<String>> getSupportedMetrics() {
        return Result.success(chartDataService.getSupportedMetrics());
    }
} 