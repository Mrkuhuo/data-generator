package com.datagenerator.task.application;

import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteTaskGroup;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskGroupExecutionRepository;
import com.datagenerator.task.repository.WriteTaskGroupRepository;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Service
public class WriteTaskGroupSchedulingService {

    private static final String JOB_GROUP = "mdg-write-task-groups";
    private static final String TRIGGER_GROUP = "mdg-write-task-group-triggers";
    private static final List<WriteExecutionStatus> COMPLETED_EXECUTION_STATUSES = List.of(
            WriteExecutionStatus.SUCCESS,
            WriteExecutionStatus.PARTIAL_SUCCESS,
            WriteExecutionStatus.FAILED
    );

    private final Scheduler scheduler;
    private final WriteTaskGroupRepository repository;
    private final WriteTaskGroupExecutionRepository executionRepository;
    private final WriteTaskGroupService service;

    public WriteTaskGroupSchedulingService(
            Scheduler scheduler,
            WriteTaskGroupRepository repository,
            WriteTaskGroupExecutionRepository executionRepository,
            WriteTaskGroupService service
    ) {
        this.scheduler = scheduler;
        this.repository = repository;
        this.executionRepository = executionRepository;
        this.service = service;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void syncOnStartup() {
        syncAll();
    }

    public void syncAll() {
        repository.findAll().forEach(group -> {
            try {
                scheduleOrUpdate(group);
            } catch (Exception ignored) {
            }
        });
    }

    public void applyScheduleSnapshot(WriteTaskGroup group) {
        WriteTaskGroupScheduleSnapshot snapshot = readSnapshot(group);
        group.setSchedulerState(snapshot.schedulerState());
        group.setNextFireAt(snapshot.nextFireAt());
        group.setPreviousFireAt(snapshot.previousFireAt());
    }

    public WriteTaskGroupScheduleSnapshot readSnapshot(WriteTaskGroup group) {
        return readSnapshot(
                group.getId(),
                group.getScheduleType(),
                group.getStatus(),
                group.getTriggerAt(),
                group.getLastTriggeredAt()
        );
    }

    public WriteTaskGroupScheduleSnapshot readSnapshot(
            Long groupId,
            WriteTaskScheduleType scheduleType,
            WriteTaskStatus status,
            Instant triggerAt,
            Instant lastTriggeredAt
    ) {
        try {
            if (scheduleType == WriteTaskScheduleType.MANUAL) {
                return new WriteTaskGroupScheduleSnapshot("MANUAL", null, null);
            }
            if (status == WriteTaskStatus.DISABLED) {
                return new WriteTaskGroupScheduleSnapshot("DISABLED", null, null);
            }
            if (scheduleType == WriteTaskScheduleType.INTERVAL && status == WriteTaskStatus.READY) {
                return new WriteTaskGroupScheduleSnapshot("STOPPED", null, null);
            }
            if (scheduleType == WriteTaskScheduleType.ONCE
                    && triggerAt != null
                    && lastTriggeredAt != null
                    && !lastTriggeredAt.isBefore(triggerAt)) {
                return new WriteTaskGroupScheduleSnapshot("COMPLETED", null, lastTriggeredAt);
            }

            JobKey jobKey = jobKey(groupId);
            if (!scheduler.checkExists(jobKey)) {
                return new WriteTaskGroupScheduleSnapshot(status.name(), null, null);
            }

            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            if (triggers.isEmpty()) {
                return new WriteTaskGroupScheduleSnapshot("UNSCHEDULED", null, null);
            }

            Trigger trigger = triggers.getFirst();
            Instant nextFireAt = trigger.getNextFireTime() == null ? null : trigger.getNextFireTime().toInstant();
            Instant previousFireAt = trigger.getPreviousFireTime() == null ? null : trigger.getPreviousFireTime().toInstant();

            if (scheduleType == WriteTaskScheduleType.INTERVAL) {
                return new WriteTaskGroupScheduleSnapshot(status.name(), nextFireAt, previousFireAt);
            }

            Trigger.TriggerState state = scheduler.getTriggerState(trigger.getKey());
            return new WriteTaskGroupScheduleSnapshot(state.name(), nextFireAt, previousFireAt);
        } catch (SchedulerException exception) {
            return new WriteTaskGroupScheduleSnapshot("ERROR", null, null);
        }
    }

    public void scheduleOrUpdate(WriteTaskGroup group) {
        try {
            if (!shouldSchedule(group)) {
                unschedule(group.getId());
                return;
            }

            if (hasOnceAlreadyCompleted(group)) {
                unschedule(group.getId());
                return;
            }

            JobDetail jobDetail = buildJobDetail(group);
            Trigger trigger = buildTrigger(group);
            if (trigger == null) {
                unschedule(group.getId());
                return;
            }

            if (scheduler.checkExists(jobDetail.getKey())) {
                scheduler.addJob(jobDetail, true, true);
                if (scheduler.checkExists(trigger.getKey())) {
                    scheduler.rescheduleJob(trigger.getKey(), trigger);
                } else {
                    scheduler.scheduleJob(trigger);
                }
            } else {
                scheduler.scheduleJob(jobDetail, trigger);
            }

            if (group.getStatus() == WriteTaskStatus.PAUSED) {
                scheduler.pauseJob(jobDetail.getKey());
            } else {
                scheduler.resumeJob(jobDetail.getKey());
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("\u540c\u6b65\u5173\u7cfb\u4efb\u52a1\u7ec4\u8c03\u5ea6\u5931\u8d25: " + exception.getMessage(), exception);
        }
    }

    public void unschedule(Long groupId) {
        try {
            TriggerKey triggerKey = triggerKey(groupId);
            if (scheduler.checkExists(triggerKey)) {
                scheduler.unscheduleJob(triggerKey);
            }

            JobKey jobKey = jobKey(groupId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("\u53d6\u6d88\u5173\u7cfb\u4efb\u52a1\u7ec4\u8c03\u5ea6\u5931\u8d25: " + exception.getMessage(), exception);
        }
    }

    public WriteTaskGroup startContinuous(Long groupId) {
        WriteTaskGroup group = service.findById(groupId);
        if (group.getScheduleType() != WriteTaskScheduleType.INTERVAL) {
            throw new IllegalArgumentException("\u53ea\u6709\u6301\u7eed\u5199\u5165\u7684\u5173\u7cfb\u4efb\u52a1\u7ec4\u624d\u80fd\u542f\u52a8");
        }
        group.setStatus(WriteTaskStatus.RUNNING);
        WriteTaskGroup saved = repository.save(group);
        scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public WriteTaskGroup pause(Long groupId) {
        WriteTaskGroup group = service.findById(groupId);
        if (group.getScheduleType() == WriteTaskScheduleType.MANUAL) {
            throw new IllegalArgumentException("\u624b\u52a8\u5173\u7cfb\u4efb\u52a1\u7ec4\u4e0d\u652f\u6301\u6682\u505c");
        }
        if (group.getScheduleType() == WriteTaskScheduleType.INTERVAL && group.getStatus() == WriteTaskStatus.READY) {
            throw new IllegalArgumentException("\u6301\u7eed\u5199\u5165\u5c1a\u672a\u542f\u52a8");
        }
        group.setStatus(WriteTaskStatus.PAUSED);
        WriteTaskGroup saved = repository.save(group);
        scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public WriteTaskGroup resume(Long groupId) {
        WriteTaskGroup group = service.findById(groupId);
        if (group.getScheduleType() == WriteTaskScheduleType.MANUAL) {
            throw new IllegalArgumentException("\u624b\u52a8\u5173\u7cfb\u4efb\u52a1\u7ec4\u4e0d\u652f\u6301\u6062\u590d");
        }
        group.setStatus(group.getScheduleType() == WriteTaskScheduleType.INTERVAL ? WriteTaskStatus.RUNNING : WriteTaskStatus.READY);
        WriteTaskGroup saved = repository.save(group);
        scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public WriteTaskGroup stopContinuous(Long groupId) {
        WriteTaskGroup group = service.findById(groupId);
        if (group.getScheduleType() != WriteTaskScheduleType.INTERVAL) {
            throw new IllegalArgumentException("\u53ea\u6709\u6301\u7eed\u5199\u5165\u7684\u5173\u7cfb\u4efb\u52a1\u7ec4\u624d\u80fd\u505c\u6b62");
        }
        group.setStatus(WriteTaskStatus.READY);
        WriteTaskGroup saved = repository.save(group);
        unschedule(groupId);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public void handleScheduledFire(Long groupId, WriteExecutionTriggerType triggerType) {
        WriteTaskGroup group = repository.findById(groupId)
                .orElseThrow(() -> new IllegalArgumentException("\u672a\u627e\u5230\u5173\u7cfb\u4efb\u52a1\u7ec4: " + groupId));

        if (group.getScheduleType() == WriteTaskScheduleType.INTERVAL) {
            if (group.getStatus() != WriteTaskStatus.RUNNING) {
                return;
            }
            if (reachedContinuousLimit(group)) {
                stopContinuous(groupId);
                return;
            }
        } else if (group.getStatus() != WriteTaskStatus.READY) {
            return;
        }

        if (executionRepository.existsByWriteTaskGroupIdAndStatus(groupId, WriteExecutionStatus.RUNNING)) {
            return;
        }

        service.runScheduled(groupId, triggerType);

        if (group.getScheduleType() == WriteTaskScheduleType.INTERVAL) {
            WriteTaskGroup refreshed = repository.findById(groupId)
                    .orElseThrow(() -> new IllegalArgumentException("\u672a\u627e\u5230\u5173\u7cfb\u4efb\u52a1\u7ec4: " + groupId));
            if (reachedContinuousLimit(refreshed)) {
                stopContinuous(groupId);
            }
        }
    }

    private boolean shouldSchedule(WriteTaskGroup group) {
        if (group.getStatus() == WriteTaskStatus.DRAFT || group.getStatus() == WriteTaskStatus.DISABLED) {
            return false;
        }
        return switch (group.getScheduleType()) {
            case MANUAL -> false;
            case ONCE, CRON -> group.getStatus() == WriteTaskStatus.READY || group.getStatus() == WriteTaskStatus.PAUSED;
            case INTERVAL -> group.getStatus() == WriteTaskStatus.RUNNING || group.getStatus() == WriteTaskStatus.PAUSED;
        };
    }

    private boolean reachedContinuousLimit(WriteTaskGroup group) {
        long completedRuns = executionRepository.countByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
                group.getId(),
                WriteExecutionTriggerType.CONTINUOUS,
                COMPLETED_EXECUTION_STATUSES
        );
        if (group.getMaxRuns() != null && completedRuns >= group.getMaxRuns()) {
            return true;
        }

        long insertedRows = executionRepository.sumInsertedRowCountByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
                group.getId(),
                WriteExecutionTriggerType.CONTINUOUS,
                COMPLETED_EXECUTION_STATUSES
        );
        return group.getMaxRowsTotal() != null && insertedRows >= group.getMaxRowsTotal();
    }

    private JobDetail buildJobDetail(WriteTaskGroup group) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put(ScheduledWriteTaskGroupJob.groupIdKey(), group.getId());
        dataMap.put(ScheduledWriteTaskGroupJob.triggerTypeKey(), resolveTriggerType(group).name());

        return JobBuilder.newJob(ScheduledWriteTaskGroupJob.class)
                .withIdentity(jobKey(group.getId()))
                .usingJobData(dataMap)
                .storeDurably(false)
                .build();
    }

    private Trigger buildTrigger(WriteTaskGroup group) {
        return switch (group.getScheduleType()) {
            case MANUAL -> null;
            case ONCE -> buildOnceTrigger(group);
            case CRON -> buildCronTrigger(group);
            case INTERVAL -> buildIntervalTrigger(group);
        };
    }

    private Trigger buildOnceTrigger(WriteTaskGroup group) {
        if (group.getTriggerAt() == null) {
            throw new IllegalArgumentException("\u5355\u6b21\u4efb\u52a1\u7ec4\u5fc5\u987b\u8bbe\u7f6e triggerAt");
        }

        Instant startAt = group.getTriggerAt().isBefore(Instant.now())
                ? Instant.now().plusSeconds(1)
                : group.getTriggerAt();

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(group.getId()))
                .forJob(jobKey(group.getId()))
                .startAt(Date.from(startAt))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0))
                .build();
    }

    private Trigger buildCronTrigger(WriteTaskGroup group) {
        if (group.getCronExpression() == null || group.getCronExpression().isBlank()) {
            throw new IllegalArgumentException("\u5468\u671f\u4efb\u52a1\u7ec4\u5fc5\u987b\u586b\u5199 cronExpression");
        }

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(group.getId()))
                .forJob(jobKey(group.getId()))
                .withSchedule(org.quartz.CronScheduleBuilder.cronSchedule(group.getCronExpression()))
                .build();
    }

    private Trigger buildIntervalTrigger(WriteTaskGroup group) {
        if (group.getIntervalSeconds() == null || group.getIntervalSeconds() < 1) {
            throw new IllegalArgumentException("\u6301\u7eed\u5199\u5165\u4efb\u52a1\u7ec4\u5fc5\u987b\u8bbe\u7f6e intervalSeconds");
        }

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(group.getId()))
                .forJob(jobKey(group.getId()))
                .startNow()
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInSeconds(group.getIntervalSeconds())
                                .repeatForever()
                                .withMisfireHandlingInstructionNextWithExistingCount()
                )
                .build();
    }

    private boolean hasOnceAlreadyCompleted(WriteTaskGroup group) {
        return group.getScheduleType() == WriteTaskScheduleType.ONCE
                && group.getTriggerAt() != null
                && group.getLastTriggeredAt() != null
                && !group.getLastTriggeredAt().isBefore(group.getTriggerAt());
    }

    private WriteExecutionTriggerType resolveTriggerType(WriteTaskGroup group) {
        return group.getScheduleType() == WriteTaskScheduleType.INTERVAL
                ? WriteExecutionTriggerType.CONTINUOUS
                : WriteExecutionTriggerType.SCHEDULED;
    }

    private JobKey jobKey(Long groupId) {
        return new JobKey("write-task-group-" + groupId, JOB_GROUP);
    }

    private TriggerKey triggerKey(Long groupId) {
        return new TriggerKey("write-task-group-trigger-" + groupId, TRIGGER_GROUP);
    }
}
