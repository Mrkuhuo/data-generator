package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskExecution;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface WriteTaskExecutionRepository extends JpaRepository<WriteTaskExecution, Long> {

    List<WriteTaskExecution> findByWriteTaskIdOrderByStartedAtDesc(Long writeTaskId);

    List<WriteTaskExecution> findByWriteTaskIdIn(List<Long> writeTaskIds);

    boolean existsByWriteTaskIdAndStatus(Long writeTaskId, WriteExecutionStatus status);

    long countByWriteTaskIdAndTriggerTypeAndStatusIn(
            Long writeTaskId,
            WriteExecutionTriggerType triggerType,
            Collection<WriteExecutionStatus> statuses
    );

    @Query("""
            select coalesce(sum(execution.successCount), 0)
            from WriteTaskExecution execution
            where execution.writeTaskId = :writeTaskId
              and execution.triggerType = :triggerType
              and execution.status in :statuses
            """)
    Long sumSuccessCountByWriteTaskIdAndTriggerTypeAndStatusIn(
            @Param("writeTaskId") Long writeTaskId,
            @Param("triggerType") WriteExecutionTriggerType triggerType,
            @Param("statuses") Collection<WriteExecutionStatus> statuses
    );
}
