package com.datagenerator.job.api;

import com.datagenerator.common.web.ApiResponse;
import com.datagenerator.job.application.ExecutionService;
import com.datagenerator.job.domain.JobExecution;
import com.datagenerator.job.domain.JobExecutionLog;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/executions")
@ConditionalOnProperty(name = "mdg.legacy.enabled", havingValue = "true")
public class JobExecutionController {

    private final ExecutionService executionService;

    public JobExecutionController(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @GetMapping
    public ApiResponse<List<JobExecution>> list() {
        return ApiResponse.success(executionService.findAll());
    }

    @GetMapping("/{id}")
    public ApiResponse<JobExecution> detail(@PathVariable Long id) {
        return ApiResponse.success(executionService.findById(id));
    }

    @GetMapping("/{id}/logs")
    public ApiResponse<List<JobExecutionLog>> logs(@PathVariable Long id) {
        return ApiResponse.success(executionService.findLogs(id));
    }
}

