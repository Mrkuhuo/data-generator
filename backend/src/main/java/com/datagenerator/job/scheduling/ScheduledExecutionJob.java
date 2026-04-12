package com.datagenerator.job.scheduling;

import com.datagenerator.job.application.ExecutionService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

@DisallowConcurrentExecution
public class ScheduledExecutionJob implements Job {

    private static final String JOB_ID_KEY = "jobId";

    @Autowired
    private ExecutionService executionService;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap dataMap = context.getMergedJobDataMap();
        Long jobId = dataMap.getLongValue(JOB_ID_KEY);
        try {
            executionService.triggerScheduledRun(jobId);
        } catch (Exception exception) {
            throw new JobExecutionException(exception);
        }
    }

    public static String jobIdKey() {
        return JOB_ID_KEY;
    }
}
