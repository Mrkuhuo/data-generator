package com.datagenerator.task.application;

import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.task.domain.WriteTask;
import java.util.List;
import java.util.Map;

public interface WriteTaskDeliveryWriter {

    boolean supports(DatabaseType databaseType);

    WriteTaskDeliveryResult write(
            WriteTask task,
            TargetConnection connection,
            List<Map<String, Object>> rows,
            Long executionId
    ) throws Exception;
}
