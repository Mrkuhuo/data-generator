package com.datagenerator.job.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.job.application.ExecutionService;
import com.datagenerator.job.application.JobService;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobExecution;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
@ConditionalOnProperty(name = "mdg.legacy.enabled", havingValue = "true")
public class JobController {

    private final JobService jobService;
    private final ExecutionService executionService;

    public JobController(JobService jobService, ExecutionService executionService) {
        this.jobService = jobService;
        this.executionService = executionService;
    }

    @GetMapping
    public ApiResponse<List<JobDefinition>> list() {
        return ApiResponse.success(jobService.findAll());
    }

    @GetMapping("/{id}")
    public ApiResponse<JobDefinition> detail(@PathVariable Long id) {
        return ApiResponse.success(jobService.findById(id));
    }

    @PostMapping
    public ApiResponse<JobDefinition> create(@Valid @RequestBody JobUpsertRequest request) {
        return ApiResponse.success(jobService.create(request), "任务已创建");
    }

    @PostMapping("/quickstart")
    public ApiResponse<JobDefinition> createExample() {
        return ApiResponse.success(jobService.createExample(), "示例任务已创建");
    }

    @PutMapping("/{id}")
    public ApiResponse<JobDefinition> update(@PathVariable Long id, @Valid @RequestBody JobUpsertRequest request) {
        return ApiResponse.success(jobService.update(id, request), "任务已更新");
    }

    @PostMapping("/{id}/run")
    public ApiResponse<JobExecution> run(@PathVariable Long id) {
        return ApiResponse.success(executionService.triggerApiRun(id), "任务执行已开始");
    }

    @PostMapping("/{id}/pause")
    public ApiResponse<JobDefinition> pause(@PathVariable Long id) {
        return ApiResponse.success(jobService.pause(id), "任务已暂停");
    }

    @PostMapping("/{id}/resume")
    public ApiResponse<JobDefinition> resume(@PathVariable Long id) {
        return ApiResponse.success(jobService.resume(id), "任务已恢复");
    }

    @PostMapping("/{id}/disable")
    public ApiResponse<JobDefinition> disable(@PathVariable Long id) {
        return ApiResponse.success(jobService.disable(id), "任务已禁用");
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        jobService.delete(id);
        return ApiResponse.success(null, "任务已删除");
    }
}
