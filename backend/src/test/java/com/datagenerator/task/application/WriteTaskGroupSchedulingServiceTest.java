package com.datagenerator.task.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import com.datagenerator.task.domain.WriteTaskGroup;
import com.datagenerator.task.domain.WriteTaskScheduleType;
import com.datagenerator.task.domain.WriteTaskStatus;
import com.datagenerator.task.repository.WriteTaskGroupExecutionRepository;
import com.datagenerator.task.repository.WriteTaskGroupRepository;
import java.time.Instant;
import java.util.List;
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
class WriteTaskGroupSchedulingServiceTest {

    @Mock
    private Scheduler scheduler;

    @Mock
    private WriteTaskGroupRepository repository;

    @Mock
    private WriteTaskGroupExecutionRepository executionRepository;

    @Mock
    private WriteTaskGroupService service;

    private WriteTaskGroupSchedulingService schedulingService;

    @BeforeEach
    void setUp() {
        schedulingService = new WriteTaskGroupSchedulingService(
                scheduler,
                repository,
                executionRepository,
                service
        );
    }

    @Test
    void startContinuous_shouldSwitchGroupToRunningAndScheduleJob() throws Exception {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.INTERVAL);
        group.setStatus(WriteTaskStatus.READY);

        when(service.findById(8L)).thenReturn(group);
        when(repository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        WriteTaskGroup saved = schedulingService.startContinuous(8L);

        assertThat(saved.getStatus()).isEqualTo(WriteTaskStatus.RUNNING);
        verify(scheduler).scheduleJob(any(org.quartz.JobDetail.class), any(org.quartz.Trigger.class));
    }

    @Test
    void pause_shouldSwitchIntervalGroupToPaused() throws Exception {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.INTERVAL);
        group.setStatus(WriteTaskStatus.RUNNING);

        when(service.findById(8L)).thenReturn(group);
        when(repository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        WriteTaskGroup saved = schedulingService.pause(8L);

        assertThat(saved.getStatus()).isEqualTo(WriteTaskStatus.PAUSED);
        verify(scheduler).scheduleJob(any(org.quartz.JobDetail.class), any(org.quartz.Trigger.class));
        verify(scheduler).pauseJob(any(JobKey.class));
    }

    @Test
    void resume_shouldSwitchPausedIntervalGroupBackToRunning() throws Exception {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.INTERVAL);
        group.setStatus(WriteTaskStatus.PAUSED);

        when(service.findById(8L)).thenReturn(group);
        when(repository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        WriteTaskGroup saved = schedulingService.resume(8L);

        assertThat(saved.getStatus()).isEqualTo(WriteTaskStatus.RUNNING);
        verify(scheduler).scheduleJob(any(org.quartz.JobDetail.class), any(org.quartz.Trigger.class));
        verify(scheduler).resumeJob(any(JobKey.class));
    }

    @Test
    void stopContinuous_shouldSwitchGroupToReadyAndUnschedule() throws Exception {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.INTERVAL);
        group.setStatus(WriteTaskStatus.RUNNING);

        when(service.findById(8L)).thenReturn(group);
        when(repository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> invocation.getArgument(0));

        WriteTaskGroup saved = schedulingService.stopContinuous(8L);

        assertThat(saved.getStatus()).isEqualTo(WriteTaskStatus.READY);
        verify(scheduler).checkExists(any(TriggerKey.class));
        verify(scheduler).checkExists(any(JobKey.class));
    }

    @Test
    void readSnapshot_shouldReturnCompletedForFinishedOnceGroup() {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.ONCE);
        group.setStatus(WriteTaskStatus.READY);
        group.setTriggerAt(Instant.parse("2026-04-14T01:00:00Z"));
        group.setLastTriggeredAt(Instant.parse("2026-04-14T01:00:05Z"));

        WriteTaskGroupScheduleSnapshot snapshot = schedulingService.readSnapshot(group);

        assertThat(snapshot.schedulerState()).isEqualTo("COMPLETED");
        assertThat(snapshot.nextFireAt()).isNull();
        assertThat(snapshot.previousFireAt()).isEqualTo(group.getLastTriggeredAt());
    }

    @Test
    void handleScheduledFire_shouldStopContinuousGroupWhenRunLimitReachedBeforeExecution() {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.INTERVAL);
        group.setStatus(WriteTaskStatus.RUNNING);
        group.setMaxRuns(1);

        when(repository.findById(8L)).thenReturn(Optional.of(group));
        when(service.findById(8L)).thenReturn(group);
        when(repository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(executionRepository.countByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
                8L,
                WriteExecutionTriggerType.CONTINUOUS,
                List.of(WriteExecutionStatus.SUCCESS, WriteExecutionStatus.PARTIAL_SUCCESS, WriteExecutionStatus.FAILED)
        )).thenReturn(1L);

        schedulingService.handleScheduledFire(8L, WriteExecutionTriggerType.CONTINUOUS);

        assertThat(group.getStatus()).isEqualTo(WriteTaskStatus.READY);
        verify(service, never()).runScheduled(any(Long.class), any(WriteExecutionTriggerType.class));
    }

    @Test
    void handleScheduledFire_shouldStopContinuousGroupWhenRowLimitReachedAfterExecution() {
        WriteTaskGroup group = sampleGroup();
        group.setScheduleType(WriteTaskScheduleType.INTERVAL);
        group.setStatus(WriteTaskStatus.RUNNING);
        group.setMaxRowsTotal(10L);

        when(repository.findById(8L)).thenReturn(Optional.of(group));
        when(service.findById(8L)).thenReturn(group);
        when(repository.save(any(WriteTaskGroup.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(executionRepository.existsByWriteTaskGroupIdAndStatus(8L, WriteExecutionStatus.RUNNING)).thenReturn(false);
        when(executionRepository.countByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
                8L,
                WriteExecutionTriggerType.CONTINUOUS,
                List.of(WriteExecutionStatus.SUCCESS, WriteExecutionStatus.PARTIAL_SUCCESS, WriteExecutionStatus.FAILED)
        )).thenReturn(0L, 0L);
        when(executionRepository.sumInsertedRowCountByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
                8L,
                WriteExecutionTriggerType.CONTINUOUS,
                List.of(WriteExecutionStatus.SUCCESS, WriteExecutionStatus.PARTIAL_SUCCESS, WriteExecutionStatus.FAILED)
        )).thenReturn(0L, 10L);

        schedulingService.handleScheduledFire(8L, WriteExecutionTriggerType.CONTINUOUS);

        verify(service).runScheduled(8L, WriteExecutionTriggerType.CONTINUOUS);
        assertThat(group.getStatus()).isEqualTo(WriteTaskStatus.READY);
    }

    private WriteTaskGroup sampleGroup() {
        WriteTaskGroup group = new WriteTaskGroup();
        group.setId(8L);
        group.setName("group-demo");
        group.setConnectionId(2L);
        group.setStatus(WriteTaskStatus.READY);
        group.setScheduleType(WriteTaskScheduleType.MANUAL);
        group.setIntervalSeconds(5);
        group.setTriggerAt(Instant.parse("2026-04-14T01:00:00Z"));
        return group;
    }
}
