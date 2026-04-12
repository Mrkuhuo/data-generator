package com.datagenerator.job.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "job_execution_log")
public class JobExecutionLog extends BaseEntity {

    @Column(name = "job_execution_id", nullable = false)
    private Long jobExecutionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "log_level", nullable = false, length = 10)
    private LogLevel logLevel = LogLevel.INFO;

    @Column(nullable = false, length = 1000)
    private String message;

    @Lob
    @Column(name = "detail_json")
    private String detailJson;

    @Column(name = "logged_at", nullable = false)
    private Instant loggedAt;

    @PrePersist
    public void onLogCreate() {
        if (loggedAt == null) {
            loggedAt = Instant.now();
        }
    }
}

