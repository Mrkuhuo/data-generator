package com.datagenerator.task.application;

import com.datagenerator.task.domain.WriteTask;
import java.util.List;
import java.util.Map;

record WriteTaskGroupGenerationResult(
        long seed,
        List<WriteTaskTableGenerationResult> tables
) {
}

record WriteTaskTableGenerationResult(
        WriteTask task,
        List<Map<String, Object>> rows,
        int foreignKeyMissCount,
        int nullViolationCount,
        int blankStringCount,
        long primaryKeyDuplicateCount
) {
}
