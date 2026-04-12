package com.datagenerator.job.repository;

import com.datagenerator.job.domain.JobExecutionLog;
import java.util.List;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobExecutionLogRepository extends JpaRepository<JobExecutionLog, Long> {

    default List<JobExecutionLog> findOrderedByExecutionId(Long executionId) {
        return findByJobExecutionId(executionId, Sort.by(Sort.Direction.ASC, "loggedAt"));
    }

    List<JobExecutionLog> findByJobExecutionId(Long executionId, Sort sort);
}

