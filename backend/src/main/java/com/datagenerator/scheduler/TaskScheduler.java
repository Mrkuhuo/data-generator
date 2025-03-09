package com.datagenerator.scheduler;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datagenerator.entity.DataTask;
import com.datagenerator.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
public class TaskScheduler implements TaskSchedulingService {

    @Autowired
    @Lazy
    private TaskService taskService;

    @Autowired
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    private final Map<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("初始化任务调度器");
        List<DataTask> tasks = taskService.lambdaQuery()
                .eq(DataTask::getStatus, "RUNNING")
                .eq(DataTask::getDeleted, 0)
                .list();
        
        for (DataTask task : tasks) {
            scheduleTask(task);
        }
    }

    @Override
    public void scheduleTask(DataTask task) {
        log.info("开始调度任务: {}, 状态: {}, 频率: {}秒", task.getName(), task.getStatus(), task.getFrequency());
        
        if (!"RUNNING".equals(task.getStatus()) || task.getDeleted() == 1) {
            log.warn("任务状态不正确或已删除，无法调度: {}, 状态: {}, 是否删除: {}", 
                task.getName(), task.getStatus(), task.getDeleted());
            return;
        }

        // 如果任务已经调度，先取消
        cancelTask(task.getId());

        try {
            // 创建定时任务
            Runnable runnable = () -> {
                try {
                    log.debug("开始执行任务: {}", task.getName());
                    // 直接执行数据生成，不更新任务状态
                    taskService.executeTask(task.getId());
                    log.debug("任务执行完成: {}", task.getName());
                } catch (Exception e) {
                    log.error("任务执行异常: {}", task.getName(), e);
                }
            };
            
            // 使用任务配置的频率（秒）作为数据生成的间隔，设置初始延迟为0
            // 修改为scheduleWithFixedDelay，确保在上一个任务完成后才开始下一个任务
            ScheduledFuture<?> future = threadPoolTaskScheduler.scheduleWithFixedDelay(
                runnable,
                new Date(), // 从当前时间开始
                task.getFrequency() * 1000L // 间隔时间
            );
            
            scheduledTasks.put(task.getId(), future);
            log.info("任务调度成功: {}, 数据生成频率: {}秒", task.getName(), task.getFrequency());
        } catch (Exception e) {
            log.error("任务调度失败: {}", task.getName(), e);
        }
    }

    @Override
    public void cancelTask(Long taskId) {
        ScheduledFuture<?> future = scheduledTasks.remove(taskId);
        if (future != null) {
            future.cancel(true);
            log.info("取消任务调度: {}", taskId);
        } else {
            log.debug("任务未在调度中: {}", taskId);
        }
    }

    @Scheduled(fixedRate = 60000)
    public void checkTasks() {
        log.debug("开始检查任务状态");
        try {
            List<DataTask> tasks = taskService.lambdaQuery()
                    .eq(DataTask::getStatus, "RUNNING")
                    .eq(DataTask::getDeleted, 0)
                    .list();
            
            log.debug("当前运行中的任务数量: {}", tasks.size());
            
            for (DataTask task : tasks) {
                // 如果任务没有被调度，则重新调度
                if (!scheduledTasks.containsKey(task.getId())) {
                    log.info("发现未调度的运行中任务，重新调度: {}", task.getName());
                    scheduleTask(task);
                }
            }
        } catch (Exception e) {
            log.error("检查任务状态时发生异常", e);
        }
    }

    public void executeTask(Long taskId) {
        taskService.executeTask(taskId);
    }
} 