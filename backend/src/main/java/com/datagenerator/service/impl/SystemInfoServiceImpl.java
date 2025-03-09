package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.SystemInfo;
import com.datagenerator.mapper.SystemInfoMapper;
import com.datagenerator.service.SystemInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.time.LocalDateTime;

@Slf4j
@Service
public class SystemInfoServiceImpl extends ServiceImpl<SystemInfoMapper, SystemInfo> implements SystemInfoService {

    @Override
    public SystemInfo getSystemInfo() {
        SystemInfo systemInfo = new SystemInfo();
        try {
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            
            // 获取CPU使用率
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
                systemInfo.setCpuUsage(sunOsBean.getSystemCpuLoad() * 100);
            }
            
            // 获取内存使用率
            long totalMemory = Runtime.getRuntime().totalMemory();
            long freeMemory = Runtime.getRuntime().freeMemory();
            systemInfo.setMemoryUsage((double) (totalMemory - freeMemory) / totalMemory * 100);
            
            // 获取JVM堆内存使用率
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            systemInfo.setJvmHeapUsage((double) heapUsage.getUsed() / heapUsage.getMax() * 100);
            
            // 获取JVM非堆内存使用率
            MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
            systemInfo.setJvmNonHeapUsage((double) nonHeapUsage.getUsed() / nonHeapUsage.getMax() * 100);
            
            // 获取系统运行时间
            systemInfo.setUptime(ManagementFactory.getRuntimeMXBean().getUptime());
            
            // 设置创建时间
            systemInfo.setCreateTime(LocalDateTime.now());
            
        } catch (Exception e) {
            log.error("获取系统信息失败", e);
        }
        return systemInfo;
    }

    @Override
    public void saveSystemInfo(SystemInfo systemInfo) {
        try {
            save(systemInfo);
        } catch (Exception e) {
            log.error("保存系统信息失败", e);
        }
    }
} 