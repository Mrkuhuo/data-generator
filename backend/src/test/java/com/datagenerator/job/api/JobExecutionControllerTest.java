package com.datagenerator.job.api;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.job.application.ExecutionService;
import com.datagenerator.job.domain.ExecutionStatus;
import com.datagenerator.job.domain.JobExecution;
import com.datagenerator.job.domain.JobExecutionLog;
import com.datagenerator.job.domain.LogLevel;
import com.datagenerator.job.domain.TriggerType;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(JobExecutionController.class)
class JobExecutionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ExecutionService executionService;

    @Test
    void list_shouldReturnExecutions() throws Exception {
        JobExecution execution = new JobExecution();
        execution.setId(44L);
        execution.setJobDefinitionId(7L);
        execution.setTriggerType(TriggerType.SCHEDULED);
        execution.setStatus(ExecutionStatus.SUCCESS);
        execution.setStartedAt(Instant.parse("2026-04-12T10:00:00Z"));
        execution.setFinishedAt(Instant.parse("2026-04-12T10:01:00Z"));
        execution.setGeneratedCount(100L);
        execution.setSuccessCount(100L);
        given(executionService.findAll()).willReturn(List.of(execution));

        mockMvc.perform(get("/api/executions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].id").value(44))
                .andExpect(jsonPath("$.data[0].triggerType").value("SCHEDULED"))
                .andExpect(jsonPath("$.data[0].status").value("SUCCESS"));
    }

    @Test
    void logs_shouldReturnExecutionLogs() throws Exception {
        JobExecutionLog log = new JobExecutionLog();
        log.setId(91L);
        log.setJobExecutionId(44L);
        log.setLogLevel(LogLevel.INFO);
        log.setMessage("连接器投递完成");
        log.setDetailJson("{\"deliveredCount\":100}");
        log.setLoggedAt(Instant.parse("2026-04-12T10:01:00Z"));
        given(executionService.findLogs(44L)).willReturn(List.of(log));

        mockMvc.perform(get("/api/executions/44/logs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].id").value(91))
                .andExpect(jsonPath("$.data[0].message").value("连接器投递完成"))
                .andExpect(jsonPath("$.data[0].logLevel").value("INFO"));
    }
}
