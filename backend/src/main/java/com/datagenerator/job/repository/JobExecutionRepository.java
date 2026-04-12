package com.datagenerator.job.repository;

import com.datagenerator.job.domain.JobExecution;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobExecutionRepository extends JpaRepository<JobExecution, Long> {
}

