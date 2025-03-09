package com.datagenerator.task;

import com.datagenerator.entity.SystemInfo;
import com.datagenerator.service.SystemAlertRuleService;
import com.datagenerator.service.SystemInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class SystemMonitorTask {

    @Resource
    private SystemInfoService systemInfoService;
    
    @Resource
    private SystemAlertRuleService alertRuleService;

    /**
     * 每分钟执行一次系统信息收集
     */
    @Scheduled(cron = "0 * * * * ?")
    public void collectSystemInfo() {
        try {
            log.info("开始收集系统信息...");
            SystemInfo systemInfo = systemInfoService.getSystemInfo();
            systemInfoService.saveSystemInfo(systemInfo);
            log.info("系统信息收集完成: CPU使用率={}%, 内存使用率={}%, JVM堆内存使用率={}%",
                    systemInfo.getCpuUsage(),
                    systemInfo.getMemoryUsage(),
                    systemInfo.getJvmHeapUsage());
                    
            // 检查告警规则
            alertRuleService.checkSystemMetrics(systemInfo);
        } catch (Exception e) {
            log.error("系统信息收集失败", e);
        }
    }
} 