package com.datagenerator.controller;

import com.datagenerator.annotation.PerformanceMonitor;
import com.datagenerator.common.Result;
import com.datagenerator.entity.SystemInfo;
import com.datagenerator.service.SystemInfoService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@Tag(name = "系统监控", description = "系统监控相关接口")
@RestController
@RequestMapping("/system")
public class SystemInfoController {

    @Resource
    private SystemInfoService systemInfoService;

    @Operation(summary = "获取系统信息", description = "获取系统CPU、内存、磁盘等使用情况")
    @PerformanceMonitor(name = "获取系统信息", logParams = false, logResult = true)
    @GetMapping("/info")
    public Result<SystemInfo> getSystemInfo() {
        SystemInfo systemInfo = systemInfoService.getSystemInfo();
        systemInfoService.saveSystemInfo(systemInfo);
        return Result.success(systemInfo);
    }
} 