package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTask;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WriteTaskRepository extends JpaRepository<WriteTask, Long> {

    List<WriteTask> findByConnectionId(Long connectionId);

    List<WriteTask> findByGroupIdOrderByIdAsc(Long groupId);
}
