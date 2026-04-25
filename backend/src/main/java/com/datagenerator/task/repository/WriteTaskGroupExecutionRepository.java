package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskGroupExecution;
import com.datagenerator.task.domain.WriteExecutionStatus;
import com.datagenerator.task.domain.WriteExecutionTriggerType;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface WriteTaskGroupExecutionRepository extends JpaRepository<WriteTaskGroupExecution, Long> {

    List<WriteTaskGroupExecution> findByWriteTaskGroupIdOrderByStartedAtDesc(Long writeTaskGroupId);

    boolean existsByWriteTaskGroupIdAndStatus(Long writeTaskGroupId, WriteExecutionStatus status);

    long countByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
            Long writeTaskGroupId,
            WriteExecutionTriggerType triggerType,
            Collection<WriteExecutionStatus> statuses
    );

    @Query("""
            select coalesce(sum(execution.insertedRowCount), 0)
            from WriteTaskGroupExecution execution
            where execution.writeTaskGroupId = :writeTaskGroupId
              and execution.triggerType = :triggerType
              and execution.status in :statuses
            """)
    Long sumInsertedRowCountByWriteTaskGroupIdAndTriggerTypeAndStatusIn(
            @Param("writeTaskGroupId") Long writeTaskGroupId,
            @Param("triggerType") WriteExecutionTriggerType triggerType,
            @Param("statuses") Collection<WriteExecutionStatus> statuses
    );
}
