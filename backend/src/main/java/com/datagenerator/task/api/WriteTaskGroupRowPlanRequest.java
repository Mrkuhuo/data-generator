package com.datagenerator.task.api;

import com.datagenerator.task.domain.WriteTaskGroupRowPlanMode;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record WriteTaskGroupRowPlanRequest(
        @NotNull WriteTaskGroupRowPlanMode mode,
        @Min(1) Integer rowCount,
        String driverTaskKey,
        @Min(1) Integer minChildrenPerParent,
        @Min(1) Integer maxChildrenPerParent
) {
}
