package com.datagenerator.job.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "job_execution")
public class JobExecution extends BaseEntity {

    @Column(name = "job_definition_id", nullable = false)
    private Long jobDefinitionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "trigger_type", nullable = false, length = 20)
    private TriggerType triggerType = TriggerType.MANUAL;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ExecutionStatus status = ExecutionStatus.PENDING;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "generated_count", nullable = false)
    private Long generatedCount = 0L;

    @Column(name = "success_count", nullable = false)
    private Long successCount = 0L;

    @Column(name = "error_count", nullable = false)
    private Long errorCount = 0L;

    @Column(name = "error_summary", length = 1000)
    private String errorSummary;

    @Lob
    @Column(name = "delivery_details_json", columnDefinition = "LONGTEXT")
    private String deliveryDetailsJson;
}
