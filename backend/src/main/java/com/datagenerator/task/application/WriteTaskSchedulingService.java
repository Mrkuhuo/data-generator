package com.datagenerator.task.application;

import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
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
public class WriteTaskSchedulingService {

    private static final String JOB_GROUP = "mdg-write-tasks";
    private static final String TRIGGER_GROUP = "mdg-write-task-triggers";
    private static final List<WriteExecutionStatus> COMPLETED_EXECUTION_STATUSES = List.of(
            WriteExecutionStatus.SUCCESS,
            WriteExecutionStatus.PARTIAL_SUCCESS,
            WriteExecutionStatus.FAILED
    );

    private final Scheduler scheduler;
    private final WriteTaskRepository repository;
    private final WriteTaskExecutionRepository executionRepository;
    private final WriteTaskService service;

    public WriteTaskSchedulingService(
            Scheduler scheduler,
            WriteTaskRepository repository,
            WriteTaskExecutionRepository executionRepository,
            WriteTaskService service
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
        repository.findAll().forEach(task -> {
            try {
                scheduleOrUpdate(task);
            } catch (Exception ignored) {
            }
        });
    }

    public void applyScheduleSnapshot(WriteTask task) {
        WriteTaskScheduleSnapshot snapshot = readSnapshot(task);
        task.setSchedulerState(snapshot.schedulerState());
        task.setNextFireAt(snapshot.nextFireAt());
        task.setPreviousFireAt(snapshot.previousFireAt());
    }

    public void scheduleOrUpdate(WriteTask task) {
        try {
            if (!shouldSchedule(task)) {
                unschedule(task.getId());
                return;
            }

            if (task.getScheduleType() == WriteTaskScheduleType.ONCE && hasOnceAlreadyCompleted(task)) {
                unschedule(task.getId());
                return;
            }

            JobDetail jobDetail = buildJobDetail(task);
            Trigger trigger = buildTrigger(task);
            if (trigger == null) {
                unschedule(task.getId());
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

            if (task.getStatus() == WriteTaskStatus.PAUSED) {
                scheduler.pauseJob(jobDetail.getKey());
            } else {
                scheduler.resumeJob(jobDetail.getKey());
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("同步写入任务调度失败: " + exception.getMessage(), exception);
        }
    }

    public void unschedule(Long taskId) {
        try {
            TriggerKey triggerKey = triggerKey(taskId);
            if (scheduler.checkExists(triggerKey)) {
                scheduler.unscheduleJob(triggerKey);
            }

            JobKey jobKey = jobKey(taskId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
        } catch (SchedulerException exception) {
            throw new IllegalStateException("取消写入任务调度失败: " + exception.getMessage(), exception);
        }
    }

    public WriteTask startContinuous(Long taskId) {
        WriteTask task = service.findById(taskId);
        if (task.getScheduleType() != WriteTaskScheduleType.INTERVAL) {
            throw new IllegalArgumentException("只有持续写入任务才能启动");
        }
        task.setStatus(WriteTaskStatus.RUNNING);
        WriteTask saved = repository.save(task);
        scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public WriteTask pause(Long taskId) {
        WriteTask task = service.findById(taskId);
        if (task.getScheduleType() == WriteTaskScheduleType.MANUAL) {
            throw new IllegalArgumentException("手动任务不支持暂停调度");
        }
        if (task.getScheduleType() == WriteTaskScheduleType.INTERVAL && task.getStatus() == WriteTaskStatus.READY) {
            throw new IllegalArgumentException("持续写入尚未启动");
        }
        task.setStatus(WriteTaskStatus.PAUSED);
        WriteTask saved = repository.save(task);
        scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public WriteTask resume(Long taskId) {
        WriteTask task = service.findById(taskId);
        if (task.getScheduleType() == WriteTaskScheduleType.MANUAL) {
            throw new IllegalArgumentException("手动任务不支持恢复调度");
        }
        task.setStatus(task.getScheduleType() == WriteTaskScheduleType.INTERVAL ? WriteTaskStatus.RUNNING : WriteTaskStatus.READY);
        WriteTask saved = repository.save(task);
        scheduleOrUpdate(saved);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public WriteTask stopContinuous(Long taskId) {
        WriteTask task = service.findById(taskId);
        if (task.getScheduleType() != WriteTaskScheduleType.INTERVAL) {
            throw new IllegalArgumentException("只有持续写入任务才能停止");
        }
        task.setStatus(WriteTaskStatus.READY);
        WriteTask saved = repository.save(task);
        unschedule(taskId);
        applyScheduleSnapshot(saved);
        return saved;
    }

    public void handleScheduledFire(Long taskId, WriteExecutionTriggerType triggerType) {
        WriteTask task = repository.findById(taskId)
                .orElseThrow(() -> new IllegalArgumentException("未找到写入任务: " + taskId));

        if (task.getScheduleType() == WriteTaskScheduleType.INTERVAL) {
            if (task.getStatus() != WriteTaskStatus.RUNNING) {
                return;
            }
            if (reachedContinuousLimit(task)) {
                stopContinuous(taskId);
                return;
            }
        } else if (task.getStatus() != WriteTaskStatus.READY) {
            return;
        }

        service.runScheduled(taskId, triggerType);

        if (task.getScheduleType() == WriteTaskScheduleType.INTERVAL) {
            WriteTask refreshed = repository.findById(taskId)
                    .orElseThrow(() -> new IllegalArgumentException("未找到写入任务: " + taskId));
            if (reachedContinuousLimit(refreshed)) {
                stopContinuous(taskId);
            }
        }
    }

    public WriteTaskScheduleSnapshot readSnapshot(WriteTask task) {
        try {
            if (task.getScheduleType() == WriteTaskScheduleType.MANUAL) {
                return new WriteTaskScheduleSnapshot("MANUAL", null, null);
            }
            if (task.getStatus() == WriteTaskStatus.DISABLED) {
                return new WriteTaskScheduleSnapshot("DISABLED", null, null);
            }
            if (task.getScheduleType() == WriteTaskScheduleType.INTERVAL && task.getStatus() == WriteTaskStatus.READY) {
                return new WriteTaskScheduleSnapshot("STOPPED", null, null);
            }
            if (task.getScheduleType() == WriteTaskScheduleType.ONCE && hasOnceAlreadyCompleted(task)) {
                return new WriteTaskScheduleSnapshot("COMPLETED", null, task.getLastTriggeredAt());
            }

            JobKey jobKey = jobKey(task.getId());
            if (!scheduler.checkExists(jobKey)) {
                return new WriteTaskScheduleSnapshot(task.getStatus().name(), null, null);
            }

            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            if (triggers.isEmpty()) {
                return new WriteTaskScheduleSnapshot("UNSCHEDULED", null, null);
            }

            Trigger trigger = triggers.getFirst();
            Instant nextFireAt = trigger.getNextFireTime() == null ? null : trigger.getNextFireTime().toInstant();
            Instant previousFireAt = trigger.getPreviousFireTime() == null ? null : trigger.getPreviousFireTime().toInstant();

            if (task.getScheduleType() == WriteTaskScheduleType.INTERVAL) {
                return new WriteTaskScheduleSnapshot(task.getStatus().name(), nextFireAt, previousFireAt);
            }

            Trigger.TriggerState state = scheduler.getTriggerState(trigger.getKey());
            return new WriteTaskScheduleSnapshot(state.name(), nextFireAt, previousFireAt);
        } catch (SchedulerException exception) {
            return new WriteTaskScheduleSnapshot("ERROR", null, null);
        }
    }

    private boolean shouldSchedule(WriteTask task) {
        if (task.getStatus() == WriteTaskStatus.DRAFT || task.getStatus() == WriteTaskStatus.DISABLED) {
            return false;
        }
        return switch (task.getScheduleType()) {
            case MANUAL -> false;
            case ONCE, CRON -> task.getStatus() == WriteTaskStatus.READY || task.getStatus() == WriteTaskStatus.PAUSED;
            case INTERVAL -> task.getStatus() == WriteTaskStatus.RUNNING || task.getStatus() == WriteTaskStatus.PAUSED;
        };
    }

    private boolean reachedContinuousLimit(WriteTask task) {
        long completedRuns = executionRepository.countByWriteTaskIdAndTriggerTypeAndStatusIn(
                task.getId(),
                WriteExecutionTriggerType.CONTINUOUS,
                COMPLETED_EXECUTION_STATUSES
        );
        if (task.getMaxRuns() != null && completedRuns >= task.getMaxRuns()) {
            return true;
        }

        long writtenRows = executionRepository.sumSuccessCountByWriteTaskIdAndTriggerTypeAndStatusIn(
                task.getId(),
                WriteExecutionTriggerType.CONTINUOUS,
                COMPLETED_EXECUTION_STATUSES
        );
        return task.getMaxRowsTotal() != null && writtenRows >= task.getMaxRowsTotal();
    }

    private JobDetail buildJobDetail(WriteTask task) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put(ScheduledWriteTaskJob.taskIdKey(), task.getId());
        dataMap.put(ScheduledWriteTaskJob.triggerTypeKey(), resolveTriggerType(task).name());

        return JobBuilder.newJob(ScheduledWriteTaskJob.class)
                .withIdentity(jobKey(task.getId()))
                .usingJobData(dataMap)
                .storeDurably(false)
                .build();
    }

    private Trigger buildTrigger(WriteTask task) {
        return switch (task.getScheduleType()) {
            case MANUAL -> null;
            case ONCE -> buildOnceTrigger(task);
            case CRON -> buildCronTrigger(task);
            case INTERVAL -> buildIntervalTrigger(task);
        };
    }

    private Trigger buildOnceTrigger(WriteTask task) {
        if (task.getTriggerAt() == null) {
            throw new IllegalArgumentException("单次任务必须设置 triggerAt");
        }

        Instant startAt = task.getTriggerAt().isBefore(Instant.now())
                ? Instant.now().plusSeconds(1)
                : task.getTriggerAt();

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(task.getId()))
                .forJob(jobKey(task.getId()))
                .startAt(Date.from(startAt))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0))
                .build();
    }

    private Trigger buildCronTrigger(WriteTask task) {
        if (task.getCronExpression() == null || task.getCronExpression().isBlank()) {
            throw new IllegalArgumentException("周期任务必须填写 cronExpression");
        }

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(task.getId()))
                .forJob(jobKey(task.getId()))
                .withSchedule(org.quartz.CronScheduleBuilder.cronSchedule(task.getCronExpression()))
                .build();
    }

    private Trigger buildIntervalTrigger(WriteTask task) {
        if (task.getIntervalSeconds() == null || task.getIntervalSeconds() < 1) {
            throw new IllegalArgumentException("持续写入任务必须设置 intervalSeconds");
        }

        return TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(task.getId()))
                .forJob(jobKey(task.getId()))
                .startNow()
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInSeconds(task.getIntervalSeconds())
                                .repeatForever()
                                .withMisfireHandlingInstructionNextWithExistingCount()
                )
                .build();
    }

    private boolean hasOnceAlreadyCompleted(WriteTask task) {
        return task.getScheduleType() == WriteTaskScheduleType.ONCE
                && task.getTriggerAt() != null
                && task.getLastTriggeredAt() != null
                && !task.getLastTriggeredAt().isBefore(task.getTriggerAt());
    }

    private WriteExecutionTriggerType resolveTriggerType(WriteTask task) {
        return task.getScheduleType() == WriteTaskScheduleType.INTERVAL
                ? WriteExecutionTriggerType.CONTINUOUS
                : WriteExecutionTriggerType.SCHEDULED;
    }

    private JobKey jobKey(Long taskId) {
        return new JobKey("write-task-" + taskId, JOB_GROUP);
    }

    private TriggerKey triggerKey(Long taskId) {
        return new TriggerKey("write-task-trigger-" + taskId, TRIGGER_GROUP);
    }
}
