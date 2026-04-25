package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskGroupTableExecution;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WriteTaskGroupTableExecutionRepository extends JpaRepository<WriteTaskGroupTableExecution, Long> {

    List<WriteTaskGroupTableExecution> findByWriteTaskGroupExecutionIdOrderByIdAsc(Long writeTaskGroupExecutionId);
}
