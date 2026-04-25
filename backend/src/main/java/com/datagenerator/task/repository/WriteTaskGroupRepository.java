package com.datagenerator.task.repository;

import com.datagenerator.task.domain.WriteTaskGroup;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WriteTaskGroupRepository extends JpaRepository<WriteTaskGroup, Long> {
}
