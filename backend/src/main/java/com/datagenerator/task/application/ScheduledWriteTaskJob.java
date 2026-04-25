package com.datagenerator.task.application;

import com.datagenerator.task.domain.WriteExecutionTriggerType;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

@DisallowConcurrentExecution
public class ScheduledWriteTaskJob implements Job {

    private static final String TASK_ID_KEY = "writeTaskId";
    private static final String TRIGGER_TYPE_KEY = "triggerType";

    @Autowired
    private WriteTaskSchedulingService schedulingService;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap dataMap = context.getMergedJobDataMap();
        Long taskId = dataMap.getLongValue(TASK_ID_KEY);
        WriteExecutionTriggerType triggerType = WriteExecutionTriggerType.valueOf(
                dataMap.getString(TRIGGER_TYPE_KEY)
        );
        try {
            schedulingService.handleScheduledFire(taskId, triggerType);
        } catch (Exception exception) {
            throw new JobExecutionException(exception);
        }
    }

    public static String taskIdKey() {
        return TASK_ID_KEY;
    }

    public static String triggerTypeKey() {
        return TRIGGER_TYPE_KEY;
    }
}
