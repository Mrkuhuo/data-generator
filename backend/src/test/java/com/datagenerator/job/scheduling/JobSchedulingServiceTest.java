package com.datagenerator.job.scheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobScheduleType;
import com.datagenerator.job.domain.JobStatus;
import com.datagenerator.job.repository.JobDefinitionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

@ExtendWith(MockitoExtension.class)
class JobSchedulingServiceTest {

    @Mock
    private Scheduler scheduler;

    @Mock
    private JobDefinitionRepository jobRepository;

    @Captor
    private ArgumentCaptor<JobDetail> jobDetailCaptor;

    @Captor
    private ArgumentCaptor<Trigger> triggerCaptor;

    private JobSchedulingService service;

    @BeforeEach
    void setUp() {
        service = new JobSchedulingService(scheduler, jobRepository, new ObjectMapper());
    }

    @Test
    void scheduleOrUpdate_shouldScheduleCronJobsAndResumeThem() throws Exception {
        JobDefinition job = job(11L, JobScheduleType.CRON, JobStatus.READY, "0 */5 * * * ?", "{}");
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        service.scheduleOrUpdate(job);

        verify(scheduler).scheduleJob(jobDetailCaptor.capture(), triggerCaptor.capture());
        verify(scheduler).resumeJob(any(JobKey.class));

        JobDetail detail = jobDetailCaptor.getValue();
        Trigger trigger = triggerCaptor.getValue();

        assertThat(detail.getKey().getName()).isEqualTo("job-11");
        assertThat(detail.getJobDataMap().getLongValue(ScheduledExecutionJob.jobIdKey())).isEqualTo(11L);
        assertThat(trigger).isInstanceOf(CronTrigger.class);
        assertThat(((CronTrigger) trigger).getCronExpression()).isEqualTo("0 */5 * * * ?");
    }

    @Test
    void scheduleOrUpdate_shouldPausePausedJobsAfterScheduling() throws Exception {
        JobDefinition job = job(12L, JobScheduleType.CRON, JobStatus.PAUSED, "0 */10 * * * ?", "{}");
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);

        service.scheduleOrUpdate(job);

        verify(scheduler).scheduleJob(any(JobDetail.class), any(Trigger.class));
        verify(scheduler).pauseJob(any(JobKey.class));
        verify(scheduler, never()).resumeJob(any(JobKey.class));
    }

    @Test
    void scheduleOrUpdate_shouldUnscheduleManualJobs() throws Exception {
        JobDefinition job = job(13L, JobScheduleType.MANUAL, JobStatus.READY, null, "{}");
        when(scheduler.checkExists(any(TriggerKey.class))).thenReturn(true);
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(true);

        service.scheduleOrUpdate(job);

        verify(scheduler).unscheduleJob(any(TriggerKey.class));
        verify(scheduler).deleteJob(any(JobKey.class));
        verify(scheduler, never()).scheduleJob(any(JobDetail.class), any(Trigger.class));
    }

    @Test
    void readSnapshot_shouldExposeCurrentTriggerState() throws Exception {
        JobDefinition job = job(14L, JobScheduleType.CRON, JobStatus.READY, "0 */5 * * * ?", "{}");
        Trigger trigger = mock(Trigger.class);
        TriggerKey key = new TriggerKey("trigger-14", "mdg-triggers");
        Date nextFireAt = Date.from(Instant.parse("2026-04-12T15:00:00Z"));

        when(trigger.getKey()).thenReturn(key);
        when(trigger.getNextFireTime()).thenReturn(nextFireAt);
        when(trigger.getPreviousFireTime()).thenReturn(null);

        when(scheduler.checkExists(any(JobKey.class))).thenReturn(true);
        doReturn(List.of(trigger)).when(scheduler).getTriggersOfJob(any(JobKey.class));
        when(scheduler.getTriggerState(any(TriggerKey.class))).thenReturn(Trigger.TriggerState.NORMAL);

        JobScheduleSnapshot snapshot = service.readSnapshot(job);

        assertThat(snapshot.schedulerState()).isEqualTo("NORMAL");
        assertThat(snapshot.nextFireAt()).isEqualTo(Instant.parse("2026-04-12T15:00:00Z"));
        assertThat(snapshot.previousFireAt()).isNull();
    }

    @Test
    void readSnapshot_shouldMarkCompletedOnceJobs() {
        JobDefinition job = job(
                15L,
                JobScheduleType.ONCE,
                JobStatus.READY,
                null,
                """
                {
                  "schedule": {
                    "triggerAt": "2026-04-12T14:00:00Z"
                  }
                }
                """
        );
        job.setLastTriggeredAt(Instant.parse("2026-04-12T14:01:00Z"));

        JobScheduleSnapshot snapshot = service.readSnapshot(job);

        assertThat(snapshot.schedulerState()).isEqualTo("COMPLETED");
        assertThat(snapshot.previousFireAt()).isEqualTo(Instant.parse("2026-04-12T14:01:00Z"));
    }

    @Test
    void resume_shouldRecreateMissingSchedulesFromRepository() throws Exception {
        JobDefinition job = job(16L, JobScheduleType.CRON, JobStatus.READY, "0 */5 * * * ?", "{}");
        when(scheduler.checkExists(any(JobKey.class))).thenReturn(false);
        when(jobRepository.findById(16L)).thenReturn(Optional.of(job));

        service.resume(16L);

        verify(scheduler).scheduleJob(any(JobDetail.class), any(Trigger.class));
    }

    private JobDefinition job(
            Long id,
            JobScheduleType scheduleType,
            JobStatus status,
            String cronExpression,
            String runtimeConfigJson
    ) {
        JobDefinition job = new JobDefinition();
        job.setId(id);
        job.setName("job-" + id);
        job.setScheduleType(scheduleType);
        job.setStatus(status);
        job.setCronExpression(cronExpression);
        job.setRuntimeConfigJson(runtimeConfigJson);
        return job;
    }
}
