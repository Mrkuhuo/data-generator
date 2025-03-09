package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.DataTask;

public interface TaskService extends IService<DataTask> {
    /**
     * 启动任务
     */
    boolean startTask(Long taskId);

    /**
     * 停止任务
     */
    boolean stopTask(Long taskId);

    /**
     * 立即执行一次任务
     */
    boolean executeTask(Long taskId);
} 