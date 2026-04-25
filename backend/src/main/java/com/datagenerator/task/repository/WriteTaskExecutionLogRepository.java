package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskExecutionLog;
import java.util.List;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WriteTaskExecutionLogRepository extends JpaRepository<WriteTaskExecutionLog, Long> {

    default List<WriteTaskExecutionLog> findOrderedByExecutionId(Long executionId) {
        return findByWriteTaskExecutionId(executionId, Sort.by(Sort.Direction.ASC, "loggedAt"));
    }

    List<WriteTaskExecutionLog> findByWriteTaskExecutionId(Long executionId, Sort sort);

    List<WriteTaskExecutionLog> findByWriteTaskExecutionIdIn(List<Long> executionIds);
}
