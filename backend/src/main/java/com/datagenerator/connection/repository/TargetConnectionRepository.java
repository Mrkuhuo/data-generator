package com.datagenerator.connection.repository;

import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.connection.domain.TargetConnectionStatus;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TargetConnectionRepository extends JpaRepository<TargetConnection, Long> {

    long countByStatus(TargetConnectionStatus status);
}
