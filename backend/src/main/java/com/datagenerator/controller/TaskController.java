package com.datagenerator.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datagenerator.common.Result;
import com.datagenerator.entity.DataTask;
import com.datagenerator.service.TaskService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class TaskController {
    
    private final TaskService taskService;
    
    @GetMapping
    public Result<List<DataTask>> list() {
        try {
            log.info("开始获取所有任务列表");
            LambdaQueryWrapper<DataTask> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(DataTask::getDeleted, 0)
                    .orderByDesc(DataTask::getCreateTime);
            List<DataTask> tasks = taskService.list(wrapper);
            log.info("获取所有任务列表成功，size={}", tasks.size());
            return Result.success(tasks);
        } catch (Exception e) {
            log.error("获取所有任务列表失败", e);
            return Result.error(500, "获取所有任务列表失败: " + e.getMessage(), null);
        }
    }
    
    @GetMapping("/page")
    public Result<Page<DataTask>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "10") Integer size) {
        try {
            log.info("开始获取任务列表，current={}, size={}", current, size);
            Page<DataTask> page = new Page<>(current, size);
            LambdaQueryWrapper<DataTask> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(DataTask::getDeleted, 0)
                    .orderByDesc(DataTask::getCreateTime);
            log.info("执行分页查询，wrapper={}", wrapper.getSqlSegment());
            Page<DataTask> result = taskService.page(page, wrapper);
            log.info("获取任务列表成功，total={}, records={}", result.getTotal(), result.getRecords().size());
            return Result.success(result);
        } catch (Exception e) {
            log.error("获取任务列表失败", e);
            return Result.error(500, "获取任务列表失败: " + e.getMessage(), null);
        }
    }
    
    @GetMapping("/{id}")
    public Result<DataTask> getById(@PathVariable Long id) {
        return Result.success(taskService.getById(id));
    }
    
    @PostMapping
    public Result<Boolean> save(@RequestBody DataTask task) {
        return Result.success(taskService.save(task));
    }
    
    @PutMapping("/{id}")
    public Result<Boolean> update(@PathVariable Long id, @RequestBody DataTask task) {
        task.setId(id);
        return Result.success(taskService.updateById(task));
    }
    
    @DeleteMapping("/{id}")
    public Result<Boolean> delete(@PathVariable Long id) {
        return Result.success(taskService.removeById(id));
    }
    
    @PostMapping("/{id}/start")
    public Result<Boolean> startTask(@PathVariable Long id) {
        return Result.success(taskService.startTask(id));
    }
    
    @PostMapping("/{id}/stop")
    public Result<Boolean> stopTask(@PathVariable Long id) {
        return Result.success(taskService.stopTask(id));
    }
    
    @PostMapping("/{id}/execute")
    public Result<Boolean> executeTask(@PathVariable Long id) {
        return Result.success(taskService.executeTask(id));
    }
} 