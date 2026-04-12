package com.datagenerator.job.application;

import com.datagenerator.connector.application.ConnectorService;
import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.spi.ConnectorConfigSupport;
import com.datagenerator.connector.spi.ConnectorDeliveryRequest;
import com.datagenerator.connector.spi.ConnectorDeliveryResult;
import com.datagenerator.connector.spi.ConnectorRegistry;
import com.datagenerator.connector.spi.DeliveryStatus;
import com.datagenerator.dataset.preview.DatasetPreviewService;
import com.datagenerator.dataset.preview.GeneratedDatasetBatch;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datagenerator.job.domain.ExecutionStatus;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobExecution;
import com.datagenerator.job.domain.JobExecutionLog;
import com.datagenerator.job.domain.LogLevel;
import com.datagenerator.job.domain.TriggerType;
import com.datagenerator.job.repository.JobExecutionLogRepository;
import com.datagenerator.job.repository.JobExecutionRepository;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ExecutionService {

    private final JobExecutionRepository executionRepository;
    private final JobExecutionLogRepository executionLogRepository;
    private final JobService jobService;
    private final ConnectorService connectorService;
    private final DatasetPreviewService datasetPreviewService;
    private final ConnectorRegistry connectorRegistry;
    private final ObjectMapper objectMapper;

    public ExecutionService(
            JobExecutionRepository executionRepository,
            JobExecutionLogRepository executionLogRepository,
            JobService jobService,
            ConnectorService connectorService,
            DatasetPreviewService datasetPreviewService,
            ConnectorRegistry connectorRegistry,
            ObjectMapper objectMapper
    ) {
        this.executionRepository = executionRepository;
        this.executionLogRepository = executionLogRepository;
        this.jobService = jobService;
        this.connectorService = connectorService;
        this.datasetPreviewService = datasetPreviewService;
        this.connectorRegistry = connectorRegistry;
        this.objectMapper = objectMapper;
    }

    public List<JobExecution> findAll() {
        return executionRepository.findAll(Sort.by(Sort.Direction.DESC, "startedAt"));
    }

    public JobExecution findById(Long id) {
        return executionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + id));
    }

    public List<JobExecutionLog> findLogs(Long executionId) {
        return executionLogRepository.findOrderedByExecutionId(executionId);
    }

    @Transactional
    public JobExecution triggerManualRun(Long jobId) {
        return triggerRun(jobId, TriggerType.MANUAL);
    }

    @Transactional
    public JobExecution triggerScheduledRun(Long jobId) {
        return triggerRun(jobId, TriggerType.SCHEDULED);
    }

    @Transactional
    public JobExecution triggerApiRun(Long jobId) {
        return triggerRun(jobId, TriggerType.API);
    }

    @Transactional
    public JobExecution triggerRun(Long jobId, TriggerType triggerType) {
        JobDefinition job = jobService.findById(jobId);
        job.setLastTriggeredAt(Instant.now());

        JobExecution execution = new JobExecution();
        execution.setJobDefinitionId(job.getId());
        execution.setTriggerType(triggerType);
        execution.setStatus(ExecutionStatus.RUNNING);
        execution.setStartedAt(Instant.now());
        JobExecution savedExecution = executionRepository.save(execution);

        log(savedExecution.getId(), LogLevel.INFO, triggerType + " execution started", Map.of("jobId", jobId));

        try {
            Map<String, Object> runtimeConfig = readRuntimeConfig(job.getRuntimeConfigJson());
            Integer requestedCount = runtimeConfig.containsKey("count")
                    ? asInteger(runtimeConfig.get("count"), 10)
                    : runtimeConfig.containsKey("batchSize")
                    ? asInteger(runtimeConfig.get("batchSize"), 10)
                    : null;
            Long requestedSeed = runtimeConfig.containsKey("seed")
                    ? asLong(runtimeConfig.get("seed"), null)
                    : null;

            GeneratedDatasetBatch batch = datasetPreviewService.generate(job.getDatasetDefinitionId(), requestedCount, requestedSeed);
            savedExecution.setGeneratedCount((long) batch.count());
            log(savedExecution.getId(), LogLevel.INFO, "Dataset batch generated", Map.of(
                    "datasetId", batch.dataset().getId(),
                    "rows", batch.count(),
                    "seed", batch.seed()
            ));

            ConnectorInstance connector = connectorService.findById(job.getTargetConnectorId());
            int maxAttempts = runtimeConfig.containsKey("retry")
                    ? asInteger(ConnectorConfigSupport.findValue(runtimeConfig, "retry.maxAttempts"), 1)
                    : asInteger(runtimeConfig.get("maxAttempts"), 1);
            long backoffMs = runtimeConfig.containsKey("retry")
                    ? asLong(ConnectorConfigSupport.findValue(runtimeConfig, "retry.backoffMs"), 0L)
                    : asLong(runtimeConfig.get("retryBackoffMs"), 0L);

            ConnectorDeliveryResult result = null;
            int attempts = Math.max(maxAttempts, 1);
            for (int attempt = 1; attempt <= attempts; attempt++) {
                if (attempt > 1) {
                    log(savedExecution.getId(), LogLevel.WARN, "Retrying connector delivery", Map.of(
                            "attempt", attempt,
                            "maxAttempts", attempts,
                            "backoffMs", backoffMs
                    ));
                    sleepQuietly(backoffMs);
                }

                result = connectorRegistry.deliver(
                        new ConnectorDeliveryRequest(connector, job, savedExecution, batch.rows())
                );

                if (result.status() != DeliveryStatus.FAILED || attempt >= attempts) {
                    break;
                }
            }

            savedExecution.setSuccessCount(result.deliveredCount());
            savedExecution.setErrorCount(result.errorCount());
            savedExecution.setErrorSummary(result.summary());
            savedExecution.setDeliveryDetailsJson(result.detailsJson());
            savedExecution.setFinishedAt(Instant.now());
            savedExecution.setStatus(mapStatus(result.status()));

            log(savedExecution.getId(), result.errorCount() > 0 ? LogLevel.WARN : LogLevel.INFO,
                    "Connector delivery finished", Map.of(
                            "connectorId", connector.getId(),
                            "deliveryStatus", result.status(),
                            "deliveredCount", result.deliveredCount(),
                            "errorCount", result.errorCount(),
                            "details", result.detailsJson()
                    ));
        } catch (Exception exception) {
            savedExecution.setFinishedAt(Instant.now());
            savedExecution.setStatus(ExecutionStatus.FAILED);
            savedExecution.setErrorSummary(exception.getMessage());
            savedExecution.setErrorCount(Math.max(savedExecution.getErrorCount(), 1));
            log(savedExecution.getId(), LogLevel.ERROR, "Execution failed", Map.of("error", exception.getMessage()));
        }

        return executionRepository.save(savedExecution);
    }

    public long count() {
        return executionRepository.count();
    }

    private void log(Long executionId, LogLevel level, String message, Map<String, Object> details) {
        JobExecutionLog log = new JobExecutionLog();
        log.setJobExecutionId(executionId);
        log.setLogLevel(level);
        log.setMessage(message);
        log.setDetailJson(writeJson(details));
        executionLogRepository.save(log);
    }

    private Map<String, Object> readRuntimeConfig(String runtimeConfigJson) {
        try {
            if (runtimeConfigJson == null || runtimeConfigJson.isBlank()) {
                return Map.of();
            }
            return objectMapper.readValue(runtimeConfigJson, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid runtimeConfigJson: " + exception.getMessage());
        }
    }

    private String writeJson(Map<String, Object> details) {
        try {
            return objectMapper.writeValueAsString(new LinkedHashMap<>(details));
        } catch (Exception exception) {
            return "{\"error\":\"Failed to serialize execution log details\"}";
        }
    }

    private ExecutionStatus mapStatus(DeliveryStatus deliveryStatus) {
        return switch (deliveryStatus) {
            case SUCCESS -> ExecutionStatus.SUCCESS;
            case PARTIAL_SUCCESS -> ExecutionStatus.PARTIAL_SUCCESS;
            case FAILED, UNSUPPORTED -> ExecutionStatus.FAILED;
        };
    }

    private Integer asInteger(Object value, Integer fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private Long asLong(Object value, Long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private void sleepQuietly(long backoffMs) {
        if (backoffMs <= 0) {
            return;
        }
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
