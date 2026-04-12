package com.datagenerator.job.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.connector.application.ConnectorService;
import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorRole;
import com.datagenerator.connector.domain.ConnectorStatus;
import com.datagenerator.connector.domain.ConnectorType;
import com.datagenerator.connector.spi.ConnectorDeliveryResult;
import com.datagenerator.connector.spi.ConnectorRegistry;
import com.datagenerator.connector.spi.DeliveryStatus;
import com.datagenerator.dataset.domain.DatasetDefinition;
import com.datagenerator.dataset.domain.DatasetStatus;
import com.datagenerator.dataset.preview.DatasetPreviewService;
import com.datagenerator.dataset.preview.GeneratedDatasetBatch;
import com.datagenerator.job.domain.ExecutionStatus;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobExecution;
import com.datagenerator.job.domain.JobExecutionLog;
import com.datagenerator.job.domain.JobScheduleType;
import com.datagenerator.job.domain.JobStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import com.datagenerator.job.domain.TriggerType;
import com.datagenerator.job.repository.JobExecutionLogRepository;
import com.datagenerator.job.repository.JobExecutionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExecutionServiceTest {

    @Mock
    private JobExecutionRepository executionRepository;

    @Mock
    private JobExecutionLogRepository executionLogRepository;

    @Mock
    private JobService jobService;

    @Mock
    private ConnectorService connectorService;

    @Mock
    private DatasetPreviewService datasetPreviewService;

    @Mock
    private ConnectorRegistry connectorRegistry;

    @Captor
    private ArgumentCaptor<JobExecutionLog> logCaptor;

    private ExecutionService executionService;

    @BeforeEach
    void setUp() {
        executionService = new ExecutionService(
                executionRepository,
                executionLogRepository,
                jobService,
                connectorService,
                datasetPreviewService,
                connectorRegistry,
                new ObjectMapper()
        );

        when(executionRepository.save(any(JobExecution.class))).thenAnswer(invocation -> {
            JobExecution execution = invocation.getArgument(0);
            if (execution.getId() == null) {
                execution.setId(101L);
            }
            return execution;
        });
        when(executionLogRepository.save(any(JobExecutionLog.class))).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void triggerApiRun_shouldPersistSuccessfulExecution() {
        JobDefinition job = job("""
                {
                  "count": 2,
                  "seed": 20260412
                }
                """);
        when(jobService.findById(5L)).thenReturn(job);
        when(connectorService.findById(7L)).thenReturn(connector());
        when(datasetPreviewService.generate(eq(3L), eq(2), eq(20260412L))).thenReturn(batch(2));
        when(connectorRegistry.deliver(any())).thenReturn(new ConnectorDeliveryResult(
                DeliveryStatus.SUCCESS,
                2,
                0,
                "ok",
                "{\"delivered\":2}"
        ));

        JobExecution execution = executionService.triggerApiRun(5L);

        assertThat(execution.getTriggerType()).isEqualTo(TriggerType.API);
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.SUCCESS);
        assertThat(execution.getGeneratedCount()).isEqualTo(2);
        assertThat(execution.getSuccessCount()).isEqualTo(2);
        assertThat(execution.getErrorCount()).isZero();
        assertThat(execution.getDeliveryDetailsJson()).isEqualTo("{\"delivered\":2}");
        assertThat(job.getLastTriggeredAt()).isNotNull();

        verify(connectorRegistry, times(1)).deliver(any());
    }

    @Test
    void triggerManualRun_shouldRetryFailedDeliveriesUntilSuccess() {
        JobDefinition job = job("""
                {
                  "count": 2,
                  "seed": 20260412,
                  "retry": {
                    "maxAttempts": 2,
                    "backoffMs": 0
                  }
                }
                """);
        when(jobService.findById(5L)).thenReturn(job);
        when(connectorService.findById(7L)).thenReturn(connector());
        when(datasetPreviewService.generate(eq(3L), eq(2), eq(20260412L))).thenReturn(batch(2));
        when(connectorRegistry.deliver(any()))
                .thenReturn(new ConnectorDeliveryResult(DeliveryStatus.FAILED, 0, 2, "first fail", "{\"attempt\":1}"))
                .thenReturn(new ConnectorDeliveryResult(DeliveryStatus.SUCCESS, 2, 0, "ok", "{\"attempt\":2}"));

        JobExecution execution = executionService.triggerManualRun(5L);

        assertThat(execution.getTriggerType()).isEqualTo(TriggerType.MANUAL);
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.SUCCESS);
        assertThat(execution.getSuccessCount()).isEqualTo(2);
        assertThat(execution.getDeliveryDetailsJson()).isEqualTo("{\"attempt\":2}");

        verify(connectorRegistry, times(2)).deliver(any());
        verify(executionLogRepository, times(4)).save(logCaptor.capture());
        assertThat(logCaptor.getAllValues())
                .anyMatch(log -> log.getMessage().equals("正在重试连接器投递"));
    }

    @Test
    void triggerScheduledRun_shouldMapPartialSuccessStatus() {
        JobDefinition job = job("""
                {
                  "count": 2,
                  "seed": 20260412
                }
                """);
        when(jobService.findById(5L)).thenReturn(job);
        when(connectorService.findById(7L)).thenReturn(connector());
        when(datasetPreviewService.generate(eq(3L), eq(2), eq(20260412L))).thenReturn(batch(2));
        when(connectorRegistry.deliver(any())).thenReturn(new ConnectorDeliveryResult(
                DeliveryStatus.PARTIAL_SUCCESS,
                1,
                1,
                "one failed",
                "{\"failed\":1}"
        ));

        JobExecution execution = executionService.triggerScheduledRun(5L);

        assertThat(execution.getTriggerType()).isEqualTo(TriggerType.SCHEDULED);
        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.PARTIAL_SUCCESS);
        assertThat(execution.getSuccessCount()).isEqualTo(1);
        assertThat(execution.getErrorCount()).isEqualTo(1);
        verify(connectorRegistry, times(1)).deliver(any());
    }

    @Test
    void triggerRun_shouldFailFastForInvalidRuntimeConfig() {
        JobDefinition job = job("{broken");
        when(jobService.findById(5L)).thenReturn(job);

        JobExecution execution = executionService.triggerApiRun(5L);

        assertThat(execution.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(execution.getErrorCount()).isEqualTo(1);
        assertThat(execution.getErrorSummary()).contains("运行时配置 runtimeConfigJson 非法");
        verify(datasetPreviewService, never()).generate(any(Long.class), any(), any());
        verify(connectorRegistry, never()).deliver(any());
    }

    private JobDefinition job(String runtimeConfigJson) {
        JobDefinition job = new JobDefinition();
        job.setId(5L);
        job.setName("test-job");
        job.setDatasetDefinitionId(3L);
        job.setTargetConnectorId(7L);
        job.setWriteStrategy(JobWriteStrategy.APPEND);
        job.setScheduleType(JobScheduleType.MANUAL);
        job.setStatus(JobStatus.READY);
        job.setRuntimeConfigJson(runtimeConfigJson);
        return job;
    }

    private ConnectorInstance connector() {
        ConnectorInstance connector = new ConnectorInstance();
        connector.setId(7L);
        connector.setName("file");
        connector.setConnectorType(ConnectorType.FILE);
        connector.setConnectorRole(ConnectorRole.TARGET);
        connector.setStatus(ConnectorStatus.READY);
        connector.setConfigJson("{\"path\":\"./output/test.jsonl\"}");
        return connector;
    }

    private GeneratedDatasetBatch batch(int rowCount) {
        DatasetDefinition dataset = new DatasetDefinition();
        dataset.setId(3L);
        dataset.setName("dataset");
        dataset.setStatus(DatasetStatus.READY);
        List<Map<String, Object>> rows = java.util.stream.IntStream.range(0, rowCount)
                .mapToObj(index -> Map.<String, Object>of("id", index + 1, "email", "user-" + (index + 1) + "@demo.local"))
                .toList();
        return new GeneratedDatasetBatch(dataset, rowCount, 20260412L, rows);
    }
}
