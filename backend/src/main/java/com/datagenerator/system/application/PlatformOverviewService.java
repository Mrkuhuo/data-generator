package com.datagenerator.system.application;

import com.datagenerator.connector.application.ConnectorService;
import com.datagenerator.dataset.application.DatasetService;
import com.datagenerator.job.application.ExecutionService;
import com.datagenerator.job.application.JobService;
import org.springframework.stereotype.Service;

@Service
public class PlatformOverviewService {

    private final ConnectorService connectorService;
    private final DatasetService datasetService;
    private final JobService jobService;
    private final ExecutionService executionService;

    public PlatformOverviewService(
            ConnectorService connectorService,
            DatasetService datasetService,
            JobService jobService,
            ExecutionService executionService
    ) {
        this.connectorService = connectorService;
        this.datasetService = datasetService;
        this.jobService = jobService;
        this.executionService = executionService;
    }

    public PlatformOverviewResponse getOverview() {
        return new PlatformOverviewResponse(
                connectorService.count(),
                datasetService.count(),
                jobService.count(),
                executionService.count()
        );
    }
}

