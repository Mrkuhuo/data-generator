package com.datagenerator.task.api;

import com.datagenerator.task.domain.WriteLogLevel;
import com.datagenerator.task.domain.WriteTaskExecutionLog;
import java.time.Instant;

public record WriteTaskExecutionLogResponse(
        Long id,
        Long writeTaskExecutionId,
        WriteLogLevel logLevel,
        String message,
        String detailJson,
        Instant loggedAt
) {

    public static WriteTaskExecutionLogResponse from(WriteTaskExecutionLog log) {
        return new WriteTaskExecutionLogResponse(
                log.getId(),
                log.getWriteTaskExecutionId(),
                log.getLogLevel(),
                log.getMessage(),
                log.getDetailJson(),
                log.getLoggedAt()
        );
    }
}
