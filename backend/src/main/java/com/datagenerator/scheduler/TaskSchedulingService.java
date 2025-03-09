package com.datagenerator.scheduler;

import com.datagenerator.entity.DataTask;

public interface TaskSchedulingService {
    /**
     * 调度任务
     * @param task 要调度的任务
     */
    void scheduleTask(DataTask task);

    /**
     * 取消任务调度
     * @param taskId 要取消的任务ID
     */
    void cancelTask(Long taskId);
} 