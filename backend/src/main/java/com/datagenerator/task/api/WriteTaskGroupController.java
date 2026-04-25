package com.datagenerator.task.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.task.application.WriteTaskGroupScheduleSnapshot;
import com.datagenerator.task.application.WriteTaskGroupSchedulingService;
import com.datagenerator.task.application.WriteTaskGroupService;
import com.datagenerator.task.domain.WriteTaskGroup;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
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
@RequestMapping("/api/write-task-groups")
public class WriteTaskGroupController {

    private final WriteTaskGroupService service;
    private final WriteTaskGroupSchedulingService schedulingService;

    public WriteTaskGroupController(
            WriteTaskGroupService service,
            WriteTaskGroupSchedulingService schedulingService
    ) {
        this.service = service;
        this.schedulingService = schedulingService;
    }

    @GetMapping
    public ApiResponse<List<WriteTaskGroupResponse>> list() {
        return ApiResponse.success(service.findAll().stream()
                .map(this::enrich)
                .toList());
    }

    @GetMapping("/{id}")
    public ApiResponse<WriteTaskGroupResponse> detail(@PathVariable Long id) {
        return ApiResponse.success(enrich(service.findResponseById(id)));
    }

    @PostMapping
    public ApiResponse<WriteTaskGroupResponse> create(@Valid @RequestBody WriteTaskGroupUpsertRequest request) {
        WriteTaskGroupResponse response = service.create(request);
        WriteTaskGroup group = service.findById(response.id());
        schedulingService.scheduleOrUpdate(group);
        return ApiResponse.success(
                enrich(service.findResponseById(group.getId())),
                "\u5173\u7cfb\u4efb\u52a1\u7ec4\u5df2\u521b\u5efa"
        );
    }

    @PutMapping("/{id}")
    public ApiResponse<WriteTaskGroupResponse> update(@PathVariable Long id, @Valid @RequestBody WriteTaskGroupUpsertRequest request) {
        WriteTaskGroupResponse response = service.update(id, request);
        WriteTaskGroup group = service.findById(response.id());
        schedulingService.scheduleOrUpdate(group);
        return ApiResponse.success(
                enrich(service.findResponseById(group.getId())),
                "\u5173\u7cfb\u4efb\u52a1\u7ec4\u5df2\u66f4\u65b0"
        );
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        schedulingService.unschedule(id);
        service.delete(id);
        return ApiResponse.success(null, "\u5173\u7cfb\u4efb\u52a1\u7ec4\u5df2\u5220\u9664");
    }

    @PostMapping("/preview")
    public ApiResponse<WriteTaskGroupPreviewResponse> preview(@Valid @RequestBody WriteTaskGroupPreviewRequest request) {
        return ApiResponse.success(service.preview(request));
    }

    @GetMapping("/{id}/preview")
    public ApiResponse<WriteTaskGroupPreviewResponse> previewExisting(
            @PathVariable Long id,
            @RequestParam(required = false) Integer previewCount,
            @RequestParam(required = false) Long seed
    ) {
        return ApiResponse.success(service.previewExisting(id, previewCount, seed));
    }

    @PostMapping("/{id}/run")
    public ApiResponse<WriteTaskGroupExecutionResponse> run(@PathVariable Long id) {
        return ApiResponse.success(service.run(id), "\u5173\u7cfb\u4efb\u52a1\u7ec4\u5df2\u5f00\u59cb\u6267\u884c");
    }

    @PostMapping("/{id}/start")
    public ApiResponse<WriteTaskGroupResponse> start(@PathVariable Long id) {
        WriteTaskGroup group = schedulingService.startContinuous(id);
        return ApiResponse.success(
                enrich(service.findResponseById(group.getId())),
                "\u6301\u7eed\u5199\u5165\u5df2\u542f\u52a8"
        );
    }

    @PostMapping("/{id}/pause")
    public ApiResponse<WriteTaskGroupResponse> pause(@PathVariable Long id) {
        WriteTaskGroup group = schedulingService.pause(id);
        return ApiResponse.success(
                enrich(service.findResponseById(group.getId())),
                "\u5173\u7cfb\u4efb\u52a1\u7ec4\u8c03\u5ea6\u5df2\u6682\u505c"
        );
    }

    @PostMapping("/{id}/resume")
    public ApiResponse<WriteTaskGroupResponse> resume(@PathVariable Long id) {
        WriteTaskGroup group = schedulingService.resume(id);
        return ApiResponse.success(
                enrich(service.findResponseById(group.getId())),
                "\u5173\u7cfb\u4efb\u52a1\u7ec4\u8c03\u5ea6\u5df2\u6062\u590d"
        );
    }

    @PostMapping("/{id}/stop")
    public ApiResponse<WriteTaskGroupResponse> stop(@PathVariable Long id) {
        WriteTaskGroup group = schedulingService.stopContinuous(id);
        return ApiResponse.success(
                enrich(service.findResponseById(group.getId())),
                "\u6301\u7eed\u5199\u5165\u5df2\u505c\u6b62"
        );
    }

    @GetMapping("/{id}/executions")
    public ApiResponse<List<WriteTaskGroupExecutionResponse>> executions(@PathVariable Long id) {
        return ApiResponse.success(service.findExecutions(id));
    }

    @GetMapping("/executions/{id}")
    public ApiResponse<WriteTaskGroupExecutionResponse> executionDetail(@PathVariable Long id) {
        return ApiResponse.success(service.findExecutionById(id));
    }

    private WriteTaskGroupResponse enrich(WriteTaskGroupResponse response) {
        WriteTaskGroupScheduleSnapshot snapshot = schedulingService.readSnapshot(
                response.id(),
                WriteTaskScheduleType.valueOf(response.scheduleType()),
                WriteTaskStatus.valueOf(response.status()),
                response.triggerAt(),
                response.lastTriggeredAt()
        );
        return new WriteTaskGroupResponse(
                response.id(),
                response.createdAt(),
                response.updatedAt(),
                response.name(),
                response.connectionId(),
                response.description(),
                response.seed(),
                response.status(),
                response.scheduleType(),
                response.cronExpression(),
                response.triggerAt(),
                response.intervalSeconds(),
                response.maxRuns(),
                response.maxRowsTotal(),
                response.lastTriggeredAt(),
                snapshot.schedulerState(),
                snapshot.nextFireAt(),
                snapshot.previousFireAt(),
                response.tasks(),
                response.relations()
        );
    }
}
