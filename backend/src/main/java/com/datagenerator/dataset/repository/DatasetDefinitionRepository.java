package com.datagenerator.dataset.repository;

import com.datagenerator.dataset.domain.DatasetDefinition;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DatasetDefinitionRepository extends JpaRepository<DatasetDefinition, Long> {
}

