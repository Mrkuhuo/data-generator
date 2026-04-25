package com.datagenerator.task.application;

import java.time.Instant;

public record WriteTaskGroupScheduleSnapshot(
        String schedulerState,
        Instant nextFireAt,
        Instant previousFireAt
) {
}
