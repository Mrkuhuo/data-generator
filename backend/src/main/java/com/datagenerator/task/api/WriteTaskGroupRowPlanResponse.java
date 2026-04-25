package com.datagenerator.task.api;

import com.datagenerator.task.domain.WriteTaskGroupRowPlanMode;

public record WriteTaskGroupRowPlanResponse(
        WriteTaskGroupRowPlanMode mode,
        Integer rowCount,
        String driverTaskKey,
        Integer minChildrenPerParent,
        Integer maxChildrenPerParent
) {
}
