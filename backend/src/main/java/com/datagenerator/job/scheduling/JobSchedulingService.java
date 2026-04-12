package com.datagenerator.job.scheduling;

import com.datagenerator.connector.spi.ConnectorConfigSupport;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobScheduleType;
import com.datagenerator.job.domain.JobStatus;
import com.datagenerator.job.repository.JobDefinitionRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Service
public class JobSchedulingService {

    private static final String JOB_GROUP = "mdg-jobs";
    private static final String TRIGGER_GROUP = "mdg-triggers";

    private final Scheduler scheduler;
    private final JobDefinitionRepository jobRepository;
    private final ObjectMapper objectMapper;

    public JobSchedulingService(
            Scheduler scheduler,
            JobDefinitionRepository jobRepository,
            ObjectMapper objectMapper
    ) {
        this.scheduler = scheduler;
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void syncOnStartup() {
        syncAll();
    }

    public void syncAll() {
        List<JobDefinition> jobs = jobRepository.findAll();
        for (JobDefinition job : jobs) {
            try {
                scheduleOrUpdate(job);
            } catch (Exception ignored) {
            }
        }
    }

    public void scheduleOrUpdate(JobDefinition job) {
        try {
            if (job.getScheduleType() == JobScheduleType.MANUAL || job.getStatus() == JobStatus.DISABLED || job.getStatus() == JobStatus.DRAFT) {
                unschedule(job.getId());
                return;
            }

            if (job.getScheduleType() == JobScheduleType.ONCE && hasOnceAlreadyCompleted(job)) {
                unschedule(job.getId());
                return;
            }

            JobDetail jobDetail = buildJobDetail(job);
            Trigger trigger = buildTrigger(job);

            if (trigger == null) {
                unschedule(job.getId());
                return;
            }

            if (scheduler.checkExists(jobDetail.getKey())) {
                scheduler.addJob(jobDetail, true, true);
                scheduler.rescheduleJob(trigger.getKey(), trigger);
            } else {
                scheduler.scheduleJob(jobDetail, trigger);
            }

            if (job.getStatus() == JobStatus.PAUSED) {
                scheduler.pauseJob(jobDetail.getKey());
            } else {
                scheduler.resumeJob(jobDetail.getKey());
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("Failed to schedule job " + job.getId() + ": " + exception.getMessage(), exception);
        }
    }

    public void pause(Long jobId) {
        try {
            JobKey jobKey = jobKey(jobId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.pauseJob(jobKey);
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("Failed to pause job " + jobId + ": " + exception.getMessage(), exception);
        }
    }

    public void resume(Long jobId) {
        try {
            JobKey jobKey = jobKey(jobId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.resumeJob(jobKey);
            } else {
                jobRepository.findById(jobId).ifPresent(this::scheduleOrUpdate);
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("Failed to resume job " + jobId + ": " + exception.getMessage(), exception);
        }
    }

    public void unschedule(Long jobId) {
        try {
            TriggerKey triggerKey = triggerKey(jobId);
            if (scheduler.checkExists(triggerKey)) {
                scheduler.unscheduleJob(triggerKey);
            }
            JobKey jobKey = jobKey(jobId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("Failed to unschedule job " + jobId + ": " + exception.getMessage(), exception);
        }
    }

    public JobScheduleSnapshot readSnapshot(JobDefinition job) {
        try {
            if (job.getScheduleType() == JobScheduleType.MANUAL) {
                return new JobScheduleSnapshot("MANUAL", null, null);
            }
            if (job.getStatus() == JobStatus.DISABLED) {
                return new JobScheduleSnapshot("DISABLED", null, null);
            }
            if (job.getScheduleType() == JobScheduleType.ONCE && hasOnceAlreadyCompleted(job)) {
                return new JobScheduleSnapshot("COMPLETED", null, job.getLastTriggeredAt());
            }

            JobKey jobKey = jobKey(job.getId());
            if (!scheduler.checkExists(jobKey)) {
                return new JobScheduleSnapshot(job.getStatus().name(), null, null);
            }

            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            if (triggers.isEmpty()) {
                return new JobScheduleSnapshot("UNSCHEDULED", null, null);
            }

            Trigger trigger = triggers.getFirst();
            Trigger.TriggerState state = scheduler.getTriggerState(trigger.getKey());
            Instant nextFireAt = trigger.getNextFireTime() == null ? null : trigger.getNextFireTime().toInstant();
            Instant previousFireAt = trigger.getPreviousFireTime() == null ? null : trigger.getPreviousFireTime().toInstant();
            return new JobScheduleSnapshot(state.name(), nextFireAt, previousFireAt);
        } catch (SchedulerException exception) {
            return new JobScheduleSnapshot("ERROR", null, null);
        }
    }

    private JobDetail buildJobDetail(JobDefinition job) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put(ScheduledExecutionJob.jobIdKey(), job.getId());

        return JobBuilder.newJob(ScheduledExecutionJob.class)
                .withIdentity(jobKey(job.getId()))
                .usingJobData(dataMap)
                .storeDurably(false)
                .build();
    }

    private Trigger buildTrigger(JobDefinition job) {
        return switch (job.getScheduleType()) {
            case MANUAL -> null;
            case CRON -> buildCronTrigger(job);
            case ONCE -> buildOnceTrigger(job);
        };
    }

    private Trigger buildCronTrigger(JobDefinition job) {
        if (job.getCronExpression() == null || job.getCronExpression().isBlank()) {
            throw new IllegalArgumentException("CRON jobs require cronExpression");
        }
        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(job.getId()))
                .forJob(jobKey(job.getId()))
                .withSchedule(CronScheduleBuilder.cronSchedule(job.getCronExpression()))
                .build();
    }

    private Trigger buildOnceTrigger(JobDefinition job) {
        Instant triggerAt = resolveOnceTriggerAt(job);
        Instant startAt = triggerAt.isBefore(Instant.now()) ? Instant.now().plusSeconds(1) : triggerAt;

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(job.getId()))
                .forJob(jobKey(job.getId()))
                .startAt(Date.from(startAt))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0))
                .build();
    }

    private Instant resolveOnceTriggerAt(JobDefinition job) {
        Map<String, Object> runtimeConfig = readRuntimeConfig(job.getRuntimeConfigJson());
        String value = ConnectorConfigSupport.optionalString(
                runtimeConfig,
                "schedule.triggerAt",
                "triggerAt",
                "runAt",
                "onceAt"
        );
        if (value == null) {
            throw new IllegalArgumentException("ONCE jobs require runtimeConfig schedule.triggerAt");
        }
        return Instant.parse(value);
    }

    private boolean hasOnceAlreadyCompleted(JobDefinition job) {
        if (job.getScheduleType() != JobScheduleType.ONCE || job.getLastTriggeredAt() == null) {
            return false;
        }
        try {
            Instant triggerAt = resolveOnceTriggerAt(job);
            return !job.getLastTriggeredAt().isBefore(triggerAt);
        } catch (Exception exception) {
            return true;
        }
    }

    private Map<String, Object> readRuntimeConfig(String runtimeConfigJson) {
        try {
            if (runtimeConfigJson == null || runtimeConfigJson.isBlank()) {
                return Map.of();
            }
            return objectMapper.readValue(runtimeConfigJson, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid runtimeConfigJson: " + exception.getMessage(), exception);
        }
    }

    private JobKey jobKey(Long jobId) {
        return new JobKey("job-" + jobId, JOB_GROUP);
    }

    private TriggerKey triggerKey(Long jobId) {
        return new TriggerKey("trigger-" + jobId, TRIGGER_GROUP);
    }
}
