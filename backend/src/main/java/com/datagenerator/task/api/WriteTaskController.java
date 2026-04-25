package com.datagenerator.task.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.task.application.WriteTaskSchedulingService;
import com.datagenerator.task.application.WriteTaskService;
import com.datagenerator.task.domain.WriteTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/write-tasks")
public class WriteTaskController {

    private final WriteTaskService service;
    private final WriteTaskSchedulingService schedulingService;
    private final ObjectMapper objectMapper;

    public WriteTaskController(
            WriteTaskService service,
            WriteTaskSchedulingService schedulingService,
            ObjectMapper objectMapper
    ) {
        this.service = service;
        this.schedulingService = schedulingService;
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public ApiResponse<List<WriteTaskResponse>> list() {
        return ApiResponse.success(service.findAll().stream()
                .map(this::toResponse)
                .toList());
    }

    @GetMapping("/{id}")
    public ApiResponse<WriteTaskResponse> detail(@PathVariable Long id) {
        return ApiResponse.success(toResponse(service.findById(id)));
    }

    @GetMapping("/{id}/executions")
    public ApiResponse<List<WriteTaskExecutionResponse>> executionsByTask(@PathVariable Long id) {
        return ApiResponse.success(service.findExecutionsByTaskId(id).stream()
                .map(WriteTaskExecutionResponse::from)
                .toList());
    }

    @PostMapping
    public ApiResponse<WriteTaskResponse> create(@Valid @RequestBody WriteTaskUpsertRequest request) {
        WriteTask task = service.create(request);
        schedulingService.scheduleOrUpdate(task);
        return ApiResponse.success(toResponse(task), "写入任务已创建");
    }

    @PutMapping("/{id}")
    public ApiResponse<WriteTaskResponse> update(@PathVariable Long id, @Valid @RequestBody WriteTaskUpsertRequest request) {
        WriteTask task = service.update(id, request);
        schedulingService.scheduleOrUpdate(task);
        return ApiResponse.success(toResponse(task), "写入任务已更新");
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        schedulingService.unschedule(id);
        service.delete(id);
        return ApiResponse.success(null, "写入任务已删除");
    }

    @PostMapping("/preview")
    public ApiResponse<WriteTaskPreviewResponse> preview(@Valid @RequestBody WriteTaskPreviewRequest request) {
        return ApiResponse.success(service.preview(request));
    }

    @GetMapping("/{id}/preview")
    public ApiResponse<WriteTaskPreviewResponse> previewExisting(
            @PathVariable Long id,
            @RequestParam(required = false) Integer count,
            @RequestParam(required = false) Long seed
    ) {
        return ApiResponse.success(service.previewExisting(id, count, seed));
    }

    @PostMapping("/{id}/run")
    public ApiResponse<WriteTaskExecutionResponse> run(@PathVariable Long id) {
        return ApiResponse.success(service.run(id), "写入任务已开始执行");
    }

    @PostMapping("/{id}/start")
    public ApiResponse<WriteTaskResponse> start(@PathVariable Long id) {
        return ApiResponse.success(toResponse(schedulingService.startContinuous(id)), "持续写入已启动");
    }

    @PostMapping("/{id}/pause")
    public ApiResponse<WriteTaskResponse> pause(@PathVariable Long id) {
        return ApiResponse.success(toResponse(schedulingService.pause(id)), "写入任务调度已暂停");
    }

    @PostMapping("/{id}/resume")
    public ApiResponse<WriteTaskResponse> resume(@PathVariable Long id) {
        return ApiResponse.success(toResponse(schedulingService.resume(id)), "写入任务调度已恢复");
    }

    @PostMapping("/{id}/stop")
    public ApiResponse<WriteTaskResponse> stop(@PathVariable Long id) {
        return ApiResponse.success(toResponse(schedulingService.stopContinuous(id)), "持续写入已停止");
    }

    @GetMapping("/executions")
    public ApiResponse<List<WriteTaskExecutionResponse>> executions() {
        return ApiResponse.success(service.findExecutions().stream()
                .map(WriteTaskExecutionResponse::from)
                .toList());
    }

    @GetMapping("/executions/{id}")
    public ApiResponse<WriteTaskExecutionResponse> executionDetail(@PathVariable Long id) {
        return ApiResponse.success(WriteTaskExecutionResponse.from(service.findExecutionById(id)));
    }

    @GetMapping("/executions/{id}/logs")
    public ApiResponse<List<WriteTaskExecutionLogResponse>> executionLogs(@PathVariable Long id) {
        return ApiResponse.success(service.findExecutionLogs(id).stream()
                .map(WriteTaskExecutionLogResponse::from)
                .toList());
    }

    private WriteTaskResponse toResponse(WriteTask task) {
        schedulingService.applyScheduleSnapshot(task);
        return WriteTaskResponse.from(task, objectMapper);
    }
}
