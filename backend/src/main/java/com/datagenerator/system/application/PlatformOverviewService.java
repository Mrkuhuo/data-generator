package com.datagenerator.system.application;

import com.datagenerator.connection.application.TargetConnectionService;
import com.datagenerator.task.application.WriteTaskService;
import org.springframework.stereotype.Service;

@Service
public class PlatformOverviewService {

    private final TargetConnectionService targetConnectionService;
    private final WriteTaskService writeTaskService;

    public PlatformOverviewService(
            TargetConnectionService targetConnectionService,
            WriteTaskService writeTaskService
    ) {
        this.targetConnectionService = targetConnectionService;
        this.writeTaskService = writeTaskService;
    }

    public PlatformOverviewResponse getOverview() {
        return new PlatformOverviewResponse(
                0,
                0,
                0,
                0,
                targetConnectionService.count(),
                writeTaskService.count(),
                writeTaskService.findExecutions().size()
        );
    }
}
