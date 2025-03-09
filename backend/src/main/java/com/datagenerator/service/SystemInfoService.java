package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.SystemInfo;

public interface SystemInfoService extends IService<SystemInfo> {
    
    /**
     * 获取系统信息
     */
    SystemInfo getSystemInfo();
    
    /**
     * 保存系统信息
     */
    void saveSystemInfo(SystemInfo systemInfo);
} 