package com.datagenerator.connector.spi;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobExecution;
import java.util.List;
import java.util.Map;

public record ConnectorDeliveryRequest(
        ConnectorInstance connector,
        JobDefinition job,
        JobExecution execution,
        List<Map<String, Object>> rows
) {
}

