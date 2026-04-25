package com.datagenerator.task.application;

import java.time.Instant;

public record WriteTaskScheduleSnapshot(
        String schedulerState,
        Instant nextFireAt,
        Instant previousFireAt
) {
}
