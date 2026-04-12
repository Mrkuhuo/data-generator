package com.datagenerator.job.application;

import com.datagenerator.connector.repository.ConnectorInstanceRepository;
import com.datagenerator.dataset.repository.DatasetDefinitionRepository;
import com.datagenerator.job.api.JobUpsertRequest;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobScheduleType;
import com.datagenerator.job.domain.JobStatus;
import com.datagenerator.job.domain.JobWriteStrategy;
import com.datagenerator.job.repository.JobDefinitionRepository;
import com.datagenerator.job.scheduling.JobScheduleSnapshot;
import com.datagenerator.job.scheduling.JobSchedulingService;
import java.util.List;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService {

    private final JobDefinitionRepository jobRepository;
    private final DatasetDefinitionRepository datasetRepository;
    private final ConnectorInstanceRepository connectorRepository;
    private final JobSchedulingService jobSchedulingService;

    public JobService(
            JobDefinitionRepository jobRepository,
            DatasetDefinitionRepository datasetRepository,
            ConnectorInstanceRepository connectorRepository,
            JobSchedulingService jobSchedulingService
    ) {
        this.jobRepository = jobRepository;
        this.datasetRepository = datasetRepository;
        this.connectorRepository = connectorRepository;
        this.jobSchedulingService = jobSchedulingService;
    }

    public List<JobDefinition> findAll() {
        List<JobDefinition> jobs = jobRepository.findAll(Sort.by(Sort.Direction.DESC, "updatedAt"));
        jobs.forEach(this::applyScheduleSnapshot);
        return jobs;
    }

    public JobDefinition findById(Long id) {
        JobDefinition job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        applyScheduleSnapshot(job);
        return job;
    }

    @Transactional
    public JobDefinition create(JobUpsertRequest request) {
        JobDefinition job = new JobDefinition();
        apply(job, request);
        JobDefinition saved = jobRepository.save(job);
        jobSchedulingService.scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    @Transactional
    public JobDefinition update(Long id, JobUpsertRequest request) {
        JobDefinition job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        apply(job, request);
        JobDefinition saved = jobRepository.save(job);
        jobSchedulingService.scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    @Transactional
    public void delete(Long id) {
        jobSchedulingService.unschedule(id);
        jobRepository.deleteById(id);
    }

    public long count() {
        return jobRepository.count();
    }

    @Transactional
    public JobDefinition createExample() {
        Long datasetId = datasetRepository.findAll(Sort.by(Sort.Direction.ASC, "id")).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Create a dataset first before creating a quickstart job"))
                .getId();
        Long connectorId = connectorRepository.findAll(Sort.by(Sort.Direction.ASC, "id")).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Create a connector first before creating a quickstart job"))
                .getId();

        JobDefinition job = new JobDefinition();
        job.setName("Example Delivery Job");
        job.setDatasetDefinitionId(datasetId);
        job.setTargetConnectorId(connectorId);
        job.setWriteStrategy(JobWriteStrategy.APPEND);
        job.setScheduleType(JobScheduleType.MANUAL);
        job.setStatus(JobStatus.READY);
        job.setRuntimeConfigJson("""
                {
                  "count": 12,
                  "seed": 20260412,
                  "batchSize": 500,
                  "target": {
                    "table": "synthetic_user_activity",
                    "topic": "synthetic.user.activity",
                    "keyField": "userId"
                  },
                  "concurrency": 1,
                  "note": "target.table is used by MYSQL/POSTGRESQL, target.topic and target.keyField are used by KAFKA"
                }
                """);
        JobDefinition saved = jobRepository.save(job);
        jobSchedulingService.scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    @Transactional
    public JobDefinition pause(Long id) {
        JobDefinition job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        job.setStatus(JobStatus.PAUSED);
        JobDefinition saved = jobRepository.save(job);
        jobSchedulingService.pause(id);
        applyScheduleSnapshot(saved);
        return saved;
    }

    @Transactional
    public JobDefinition resume(Long id) {
        JobDefinition job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        job.setStatus(JobStatus.READY);
        JobDefinition saved = jobRepository.save(job);
        jobSchedulingService.scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    @Transactional
    public JobDefinition disable(Long id) {
        JobDefinition job = jobRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        job.setStatus(JobStatus.DISABLED);
        JobDefinition saved = jobRepository.save(job);
        jobSchedulingService.unschedule(id);
        applyScheduleSnapshot(saved);
        return saved;
    }

    private void apply(JobDefinition job, JobUpsertRequest request) {
        job.setName(request.name());
        job.setDatasetDefinitionId(request.datasetDefinitionId());
        job.setTargetConnectorId(request.targetConnectorId());
        job.setWriteStrategy(request.writeStrategy());
        job.setScheduleType(request.scheduleType());
        job.setCronExpression(request.cronExpression());
        job.setStatus(request.status());
        job.setRuntimeConfigJson(request.runtimeConfigJson());
    }

    private void applyScheduleSnapshot(JobDefinition job) {
        JobScheduleSnapshot snapshot = jobSchedulingService.readSnapshot(job);
        job.setSchedulerState(snapshot.schedulerState());
        job.setNextFireAt(snapshot.nextFireAt());
        job.setPreviousFireAt(snapshot.previousFireAt());
    }
}
