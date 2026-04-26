package com.datagenerator.job.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datagenerator.job.application.ExecutionService;
import com.datagenerator.job.application.JobService;
import com.datagenerator.job.domain.ExecutionStatus;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobExecution;
import com.datagenerator.job.domain.JobScheduleType;
import com.datagenerator.job.domain.JobStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import com.datagenerator.job.domain.TriggerType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(JobController.class)
@AutoConfigureMockMvc(addFilters = false)
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private JobService jobService;

    @MockBean
    private ExecutionService executionService;

    @Test
    void create_shouldReturnCreatedJob() throws Exception {
        JobDefinition job = jobDefinition(12L, "Realtime export");
        given(jobService.create(any(JobUpsertRequest.class))).willReturn(job);

        mockMvc.perform(post("/api/jobs")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new JobUpsertRequest(
                                "Realtime export",
                                4L,
                                8L,
                                JobWriteStrategy.UPSERT,
                                JobScheduleType.CRON,
                                "0 */10 * * * ?",
                                JobStatus.READY,
                                "{\"count\":100}"
                        ))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("任务已创建"))
                .andExpect(jsonPath("$.data.id").value(12))
                .andExpect(jsonPath("$.data.name").value("Realtime export"))
                .andExpect(jsonPath("$.data.scheduleType").value("CRON"));

        ArgumentCaptor<JobUpsertRequest> captor = ArgumentCaptor.forClass(JobUpsertRequest.class);
        verify(jobService).create(captor.capture());
        assertThat(captor.getValue().datasetDefinitionId()).isEqualTo(4L);
        assertThat(captor.getValue().targetConnectorId()).isEqualTo(8L);
        assertThat(captor.getValue().scheduleType()).isEqualTo(JobScheduleType.CRON);
    }

    @Test
    void create_shouldRejectInvalidPayload() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {
                                  "name": " ",
                                  "datasetDefinitionId": 4,
                                  "targetConnectorId": 8,
                                  "writeStrategy": "APPEND",
                                  "scheduleType": "MANUAL",
                                  "status": "READY",
                                  "runtimeConfigJson": "{}"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message", containsString("name")));
    }

    @Test
    void run_shouldStartExecution() throws Exception {
        JobExecution execution = new JobExecution();
        execution.setId(33L);
        execution.setJobDefinitionId(9L);
        execution.setTriggerType(TriggerType.API);
        execution.setStatus(ExecutionStatus.RUNNING);
        execution.setStartedAt(Instant.parse("2026-04-12T08:00:00Z"));
        given(executionService.triggerApiRun(9L)).willReturn(execution);

        mockMvc.perform(post("/api/jobs/9/run"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("任务执行已开始"))
                .andExpect(jsonPath("$.data.id").value(33))
                .andExpect(jsonPath("$.data.triggerType").value("API"))
                .andExpect(jsonPath("$.data.status").value("RUNNING"));
    }

    private JobDefinition jobDefinition(Long id, String name) {
        JobDefinition job = new JobDefinition();
        job.setId(id);
        job.setName(name);
        job.setDatasetDefinitionId(4L);
        job.setTargetConnectorId(8L);
        job.setWriteStrategy(JobWriteStrategy.UPSERT);
        job.setScheduleType(JobScheduleType.CRON);
        job.setCronExpression("0 */10 * * * ?");
        job.setStatus(JobStatus.READY);
        job.setRuntimeConfigJson("{\"count\":100}");
        return job;
    }
}
