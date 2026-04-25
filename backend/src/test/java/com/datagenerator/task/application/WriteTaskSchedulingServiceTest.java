package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteTask;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskExecutionRepository;
import com.datagenerator.task.repository.WriteTaskRepository;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;

@ExtendWith(MockitoExtension.class)
class WriteTaskSchedulingServiceTest {

    @Mock
    private Scheduler scheduler;

    @Mock
    private WriteTaskRepository repository;

    @Mock
    private WriteTaskExecutionRepository executionRepository;

    @Mock
    private WriteTaskService service;

    private WriteTaskSchedulingService schedulingService;

    @BeforeEach
    void setUp() {
        schedulingService = new WriteTaskSchedulingService(
                scheduler,
                repository,
                executionRepository,
                service
        );
    }

    @Test
    void startContinuous_shouldSwitchTaskToRunningAndScheduleJob() throws Exception {
        WriteTask task = sampleTask();
        task.setScheduleType(WriteTaskScheduleType.INTERVAL);
        task.setStatus(WriteTaskStatus.READY);

        when(service.findById(8L)).thenReturn(task);
        when(repository.save(any(WriteTask.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        WriteTask saved = schedulingService.startContinuous(8L);

        assertThat(saved.getStatus()).isEqualTo(WriteTaskStatus.RUNNING);
        verify(scheduler).scheduleJob(any(org.quartz.JobDetail.class), any(org.quartz.Trigger.class));
    }

    @Test
    void handleScheduledFire_shouldStopContinuousTaskWhenRunLimitReachedBeforeExecution() {
        WriteTask task = sampleTask();
        task.setScheduleType(WriteTaskScheduleType.INTERVAL);
        task.setStatus(WriteTaskStatus.RUNNING);
        task.setMaxRuns(1);

        when(repository.findById(8L)).thenReturn(Optional.of(task));
        when(service.findById(8L)).thenReturn(task);
        when(repository.save(any(WriteTask.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(executionRepository.countByWriteTaskIdAndTriggerTypeAndStatusIn(
                8L,
                WriteExecutionTriggerType.CONTINUOUS,
                java.util.List.of(WriteExecutionStatus.SUCCESS, WriteExecutionStatus.PARTIAL_SUCCESS, WriteExecutionStatus.FAILED)
        )).thenReturn(1L);

        schedulingService.handleScheduledFire(8L, WriteExecutionTriggerType.CONTINUOUS);

        assertThat(task.getStatus()).isEqualTo(WriteTaskStatus.READY);
        verify(service, never()).runScheduled(any(Long.class), any(WriteExecutionTriggerType.class));
    }

    @Test
    void readSnapshot_shouldReturnStoppedForReadyIntervalTask() {
        WriteTask task = sampleTask();
        task.setScheduleType(WriteTaskScheduleType.INTERVAL);
        task.setStatus(WriteTaskStatus.READY);

        WriteTaskScheduleSnapshot snapshot = schedulingService.readSnapshot(task);

        assertThat(snapshot.schedulerState()).isEqualTo("STOPPED");
        assertThat(snapshot.nextFireAt()).isNull();
    }

    @Test
    void scheduleOrUpdate_shouldPausePausedCronTask() throws Exception {
        WriteTask task = sampleTask();
        task.setScheduleType(WriteTaskScheduleType.CRON);
        task.setCronExpression("0 0/5 * * * ?");
        task.setStatus(WriteTaskStatus.PAUSED);

        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        schedulingService.scheduleOrUpdate(task);

        verify(scheduler).scheduleJob(any(org.quartz.JobDetail.class), any(org.quartz.Trigger.class));
        verify(scheduler).pauseJob(any(JobKey.class));
    }

    private WriteTask sampleTask() {
        WriteTask task = new WriteTask();
        task.setId(8L);
        task.setName("continuous-demo");
        task.setConnectionId(2L);
        task.setTableName("synthetic_orders");
        task.setRowCount(100);
        task.setBatchSize(100);
        task.setStatus(WriteTaskStatus.READY);
        task.setScheduleType(WriteTaskScheduleType.MANUAL);
        task.setIntervalSeconds(5);
        task.setTriggerAt(Instant.parse("2026-04-14T01:00:00Z"));
        return task;
    }
}
