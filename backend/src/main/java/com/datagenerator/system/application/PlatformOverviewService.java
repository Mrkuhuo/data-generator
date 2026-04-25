package com.datagenerator.system.application;

import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.connector.application.ConnectorService;
import com.datagenerator.dataset.application.DatasetService;
import com.datagenerator.job.application.ExecutionService;
import com.datagenerator.job.application.JobService;
import com.datagenerator.task.application.WriteTaskService;
import org.springframework.stereotype.Service;

@Service
public class PlatformOverviewService {

    private final ConnectorService connectorService;
    private final DatasetService datasetService;
    private final JobService jobService;
    private final ExecutionService executionService;
    private final TargetConnectionService targetConnectionService;
    private final WriteTaskService writeTaskService;

    public PlatformOverviewService(
            ConnectorService connectorService,
            DatasetService datasetService,
            JobService jobService,
            ExecutionService executionService,
            TargetConnectionService targetConnectionService,
            WriteTaskService writeTaskService
    ) {
        this.connectorService = connectorService;
        this.datasetService = datasetService;
        this.jobService = jobService;
        this.executionService = executionService;
        this.targetConnectionService = targetConnectionService;
        this.writeTaskService = writeTaskService;
    }

    public PlatformOverviewResponse getOverview() {
        return new PlatformOverviewResponse(
                connectorService.count(),
                datasetService.count(),
                jobService.count(),
                executionService.count(),
                targetConnectionService.count(),
                writeTaskService.count(),
                writeTaskService.findExecutions().size()
        );
    }
}
