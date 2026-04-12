package com.datagenerator.connector.repository;

import com.datagenerator.connector.domain.ConnectorInstance;
import com.datagenerator.connector.domain.ConnectorStatus;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConnectorInstanceRepository extends JpaRepository<ConnectorInstance, Long> {

    long countByStatus(ConnectorStatus status);
}

