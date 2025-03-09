package com.datagenerator.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datagenerator.common.PageRequest;
import com.datagenerator.common.Result;
import com.datagenerator.entity.DataTask;
import com.datagenerator.entity.ExecutionRecord;
import com.datagenerator.service.ExecutionRecordService;
import com.datagenerator.service.TaskService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/execution-records")
@RequiredArgsConstructor
public class ExecutionRecordController {

    private final ExecutionRecordService executionRecordService;
    private final TaskService taskService;

    @GetMapping("/page")
    public Result<Page<ExecutionRecord>> page(
            PageRequest pageRequest,
            String taskName,
            String status,
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        
        Page<ExecutionRecord> page = new Page<>(pageRequest.getPageNum(), pageRequest.getPageSize());
        LambdaQueryWrapper<ExecutionRecord> wrapper = new LambdaQueryWrapper<>();
        
        // 基本条件
        wrapper.eq(ExecutionRecord::getDeleted, 0);
        
        // 按状态筛选
        if (StringUtils.hasText(status)) {
            wrapper.eq(ExecutionRecord::getStatus, status);
        }
        
        // 按时间范围筛选
        if (startTime != null) {
            wrapper.ge(ExecutionRecord::getStartTime, startTime);
        }
        if (endTime != null) {
            wrapper.le(ExecutionRecord::getEndTime, endTime);
        }
        
        // 按任务名称筛选
        if (StringUtils.hasText(taskName)) {
            // 先查询符合名称的任务ID
            LambdaQueryWrapper<DataTask> taskWrapper = new LambdaQueryWrapper<>();
            taskWrapper.like(DataTask::getName, taskName)
                    .eq(DataTask::getDeleted, 0);
            List<DataTask> tasks = taskService.list(taskWrapper);
            
            if (!tasks.isEmpty()) {
                List<Long> taskIds = tasks.stream().map(DataTask::getId).collect(Collectors.toList());
                wrapper.in(ExecutionRecord::getTaskId, taskIds);
            } else {
                // 如果没有找到匹配的任务，返回空结果
                return Result.success(new Page<>());
            }
        }
        
        // 排序
        wrapper.orderByDesc(ExecutionRecord::getCreateTime);
        
        return Result.success(executionRecordService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public Result<ExecutionRecord> getById(@PathVariable Long id) {
        return Result.success(executionRecordService.getById(id));
    }

    @GetMapping("/task/{taskId}")
    public Result<Page<ExecutionRecord>> getByTaskId(@PathVariable Long taskId, PageRequest pageRequest) {
        Page<ExecutionRecord> page = new Page<>(pageRequest.getPageNum(), pageRequest.getPageSize());
        LambdaQueryWrapper<ExecutionRecord> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(ExecutionRecord::getTaskId, taskId)
                .eq(ExecutionRecord::getDeleted, 0)
                .orderByDesc(ExecutionRecord::getCreateTime);
        return Result.success(executionRecordService.page(page, wrapper));
    }
    
    @DeleteMapping("/{id}")
    public Result<Boolean> deleteById(@PathVariable Long id) {
        return Result.success(executionRecordService.removeById(id));
    }
} 