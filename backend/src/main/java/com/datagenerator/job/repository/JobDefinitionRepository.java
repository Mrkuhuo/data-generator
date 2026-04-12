package com.datagenerator.job.repository;

import com.datagenerator.job.domain.JobDefinition;
import com.datagenerator.job.domain.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobDefinitionRepository extends JpaRepository<JobDefinition, Long> {

    long countByStatus(JobStatus status);
}

