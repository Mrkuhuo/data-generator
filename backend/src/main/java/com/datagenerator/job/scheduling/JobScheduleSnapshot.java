package com.datagenerator.job.scheduling;

import java.time.Instant;

public record JobScheduleSnapshot(
        String schedulerState,
        Instant nextFireAt,
        Instant previousFireAt
) {
}
