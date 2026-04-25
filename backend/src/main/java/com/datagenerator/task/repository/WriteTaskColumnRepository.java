package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskColumn;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WriteTaskColumnRepository extends JpaRepository<WriteTaskColumn, Long> {

    List<WriteTaskColumn> findByTaskIdOrderBySortOrderAscIdAsc(Long taskId);

    List<WriteTaskColumn> findByTaskIdIn(List<Long> taskIds);
}
