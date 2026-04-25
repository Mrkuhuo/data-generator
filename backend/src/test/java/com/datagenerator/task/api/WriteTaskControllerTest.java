package com.datagenerator.task.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.task.application.WriteTaskSchedulingService;
import com.datagenerator.task.application.WriteTaskService;
import com.datagenerator.task.domain.ColumnGeneratorType;
import com.datagenerator.task.domain.TableMode;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteMode;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskColumn;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(WriteTaskController.class)
class WriteTaskControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private WriteTaskService service;

    @MockBean
    private WriteTaskSchedulingService schedulingService;

    @Test
    void create_shouldReturnCreatedTask() throws Exception {
        WriteTask task = sampleTask();
        task.setId(15L);
        given(service.create(any(WriteTaskUpsertRequest.class))).willReturn(task);

        mockMvc.perform(post("/api/write-tasks")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleRequest())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("写入任务已创建"))
                .andExpect(jsonPath("$.data.id").value(15))
                .andExpect(jsonPath("$.data.columns[0].columnName").value("id"));

        verify(schedulingService).scheduleOrUpdate(task);
    }

    @Test
    void executions_shouldReturnExecutionList() throws Exception {
        given(service.findExecutions()).willReturn(List.of(execution()));

        mockMvc.perform(get("/api/write-tasks/executions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].writeTaskId").value(15))
                .andExpect(jsonPath("$.data[0].status").value("SUCCESS"));
    }

    @Test
    void executionsByTask_shouldReturnTaskScopedExecutionList() throws Exception {
        given(service.findExecutionsByTaskId(15L)).willReturn(List.of(execution()));

        mockMvc.perform(get("/api/write-tasks/15/executions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].id").value(7))
                .andExpect(jsonPath("$.data[0].writeTaskId").value(15));
    }

    @Test
    void start_shouldReturnUpdatedContinuousTask() throws Exception {
        WriteTask task = sampleTask();
        task.setId(15L);
        task.setScheduleType(WriteTaskScheduleType.INTERVAL);
        task.setStatus(WriteTaskStatus.RUNNING);
        task.setIntervalSeconds(5);
        task.setSchedulerState("RUNNING");
        task.setNextFireAt(Instant.parse("2026-04-13T00:05:00Z"));
        given(schedulingService.startContinuous(15L)).willReturn(task);

        mockMvc.perform(post("/api/write-tasks/15/start"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("持续写入已启动"))
                .andExpect(jsonPath("$.data.id").value(15))
                .andExpect(jsonPath("$.data.scheduleType").value("INTERVAL"))
                .andExpect(jsonPath("$.data.status").value("RUNNING"))
                .andExpect(jsonPath("$.data.intervalSeconds").value(5))
                .andExpect(jsonPath("$.data.schedulerState").value("RUNNING"));
    }

    @Test
    void pause_shouldReturnPausedTask() throws Exception {
        WriteTask task = sampleTask();
        task.setId(15L);
        task.setScheduleType(WriteTaskScheduleType.CRON);
        task.setStatus(WriteTaskStatus.PAUSED);
        task.setCronExpression("0 0/5 * * * ?");
        task.setSchedulerState("PAUSED");
        given(schedulingService.pause(15L)).willReturn(task);

        mockMvc.perform(post("/api/write-tasks/15/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("写入任务调度已暂停"))
                .andExpect(jsonPath("$.data.id").value(15))
                .andExpect(jsonPath("$.data.scheduleType").value("CRON"))
                .andExpect(jsonPath("$.data.status").value("PAUSED"))
                .andExpect(jsonPath("$.data.cronExpression").value("0 0/5 * * * ?"))
                .andExpect(jsonPath("$.data.schedulerState").value("PAUSED"));
    }

    private WriteTaskUpsertRequest sampleRequest() {
        return new WriteTaskUpsertRequest(
                "demo-task",
                2L,
                "demo_table",
                TableMode.USE_EXISTING,
                WriteMode.APPEND,
                100,
                500,
                1L,
                WriteTaskStatus.READY,
                WriteTaskScheduleType.MANUAL,
                null,
                null,
                null,
                null,
                null,
                "desc",
                null,
                null,
                List.of(new WriteTaskColumnUpsertRequest(
                        "id",
                        "BIGINT",
                        null,
                        null,
                        null,
                        false,
                        true,
                        ColumnGeneratorType.SEQUENCE,
                        java.util.Map.of("start", 1, "step", 1),
                        0
                ))
        );
    }

    private WriteTask sampleTask() {
        WriteTask task = new WriteTask();
        task.setName("demo-task");
        task.setConnectionId(2L);
        task.setTableName("demo_table");
        task.setTableMode(TableMode.USE_EXISTING);
        task.setWriteMode(WriteMode.APPEND);
        task.setRowCount(100);
        task.setBatchSize(500);
        task.setSeed(1L);
        task.setStatus(WriteTaskStatus.READY);
        task.setScheduleType(WriteTaskScheduleType.MANUAL);
        task.setCreatedAt(Instant.parse("2026-04-13T00:00:00Z"));
        task.setUpdatedAt(Instant.parse("2026-04-13T00:00:00Z"));

        WriteTaskColumn column = new WriteTaskColumn();
        column.setId(1L);
        column.setColumnName("id");
        column.setDbType("BIGINT");
        column.setNullableFlag(false);
        column.setPrimaryKeyFlag(true);
        column.setGeneratorType(ColumnGeneratorType.SEQUENCE);
        column.setGeneratorConfigJson("{\"start\":1,\"step\":1}");
        column.setSortOrder(0);
        task.replaceColumns(List.of(column));
        return task;
    }

    private com.datagenerator.task.domain.WriteTaskExecution execution() {
        com.datagenerator.task.domain.WriteTaskExecution execution = new com.datagenerator.task.domain.WriteTaskExecution();
        execution.setId(7L);
        execution.setWriteTaskId(15L);
        execution.setTriggerType(WriteExecutionTriggerType.API);
        execution.setStatus(WriteExecutionStatus.SUCCESS);
        execution.setStartedAt(Instant.parse("2026-04-13T01:00:00Z"));
        execution.setFinishedAt(Instant.parse("2026-04-13T01:01:00Z"));
        execution.setGeneratedCount(10L);
        execution.setSuccessCount(10L);
        execution.setErrorCount(0L);
        return execution;
    }
}
