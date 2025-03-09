package com.datagenerator.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.datagenerator.entity.DataTask;
import com.datagenerator.entity.ExecutionRecord;
import com.datagenerator.mapper.TaskMapper;
import com.datagenerator.scheduler.TaskSchedulingService;
import com.datagenerator.service.DataGenerateService;
import com.datagenerator.service.ExecutionRecordService;
import com.datagenerator.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@Service
public class TaskServiceImpl extends ServiceImpl<TaskMapper, DataTask> implements TaskService {

    @Autowired
    private DataGenerateService dataGenerateService;
    
    @Autowired
    @Lazy
    private TaskSchedulingService taskSchedulingService;

    @Autowired
    private ExecutionRecordService executionRecordService;

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean startTask(Long taskId) {
        log.info("开始启动任务，taskId={}", taskId);
        DataTask task = getById(taskId);
        if (task == null) {
            log.error("任务不存在，taskId={}", taskId);
            return false;
        }
        task.setStatus("RUNNING");
        boolean result = updateById(task);
        if (result) {
            // 启动成功后，开始调度数据生成
            taskSchedulingService.scheduleTask(task);
        }
        log.info("任务启动{}，taskId={}", result ? "成功" : "失败", taskId);
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean stopTask(Long taskId) {
        log.info("开始停止任务，taskId={}", taskId);
        
        try {
            // 1. 直接使用SQL更新任务状态为STOPPED,绕过缓存
            getBaseMapper().update(null, 
                new com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper<DataTask>()
                    .eq(DataTask::getId, taskId)
                    .set(DataTask::getStatus, "STOPPED")
                    .set(DataTask::getUpdateTime, LocalDateTime.now()));
            
            log.info("已直接更新数据库中的任务状态为STOPPED，taskId={}", taskId);
            
            // 2. 取消任务调度
            taskSchedulingService.cancelTask(taskId);
            log.info("已取消任务调度，taskId={}", taskId);
            
            // 3. 关闭Kafka连接
            try {
                dataGenerateService.forceCloseKafkaConnections(taskId);
                log.info("已强制关闭Kafka连接，taskId={}", taskId);
            } catch (Exception e) {
                log.warn("强制关闭Kafka连接时发生异常，taskId={}", taskId, e);
            }
            
            // 4. 更新正在运行的执行记录状态为STOPPED
            try {
                // 查询该任务最近的RUNNING状态的执行记录
                com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<ExecutionRecord> wrapper = 
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<>();
                wrapper.eq(ExecutionRecord::getTaskId, taskId)
                      .eq(ExecutionRecord::getStatus, "RUNNING")
                      .orderByDesc(ExecutionRecord::getCreateTime)
                      .last("LIMIT 1");
                
                ExecutionRecord record = executionRecordService.getOne(wrapper);
                if (record != null) {
                    // 更新执行记录状态为STOPPED
                    executionRecordService.updateStatus(record.getId(), 2, "任务被手动停止", record.getTotalCount());
                    log.info("已更新执行记录状态为STOPPED，recordId={}", record.getId());
                }
            } catch (Exception e) {
                log.warn("更新执行记录状态时发生异常，taskId={}", taskId, e);
            }
            
            // 5. 清除缓存中的任务
            getBaseMapper().selectById(taskId);
            
            return true;
        } catch (Exception e) {
            log.error("停止任务失败，taskId={}", taskId, e);
            return false;
        }
    }

    @Override
    @Transactional
    public boolean executeTask(Long id) {
        try {
            DataTask task = getById(id);
            if (task == null) {
                throw new RuntimeException("任务不存在");
            }
            
            // 如果任务不是运行中状态或者是停止中状态，跳过执行
            if (!"RUNNING".equals(task.getStatus()) || "STOPPING".equals(task.getStatus())) {
                log.info("任务状态不是运行中或正在停止中，跳过执行: taskId={}, status={}", id, task.getStatus());
                return false;
            }
            
            // 执行任务
            dataGenerateService.executeTask(task);
            
            // 记录最后执行时间
            task.setUpdateTime(LocalDateTime.now());
            updateById(task);
            
            return true;
        } catch (Exception e) {
            log.error("数据生成失败，taskId={}", id, e);
            // 记录错误但不改变任务状态，让任务继续执行
            return false;
        }
    }
} 