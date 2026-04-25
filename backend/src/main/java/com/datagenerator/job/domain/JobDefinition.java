package com.datagenerator.job.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "job_definition")
public class JobDefinition extends BaseEntity {

    @Column(nullable = false, length = 120)
    private String name;

    @Column(name = "dataset_definition_id", nullable = false)
    private Long datasetDefinitionId;

    @Column(name = "target_connector_id", nullable = false)
    private Long targetConnectorId;

    @Enumerated(EnumType.STRING)
    @Column(name = "write_strategy", nullable = false, length = 20)
    private JobWriteStrategy writeStrategy = JobWriteStrategy.APPEND;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false, length = 20)
    private JobScheduleType scheduleType = JobScheduleType.MANUAL;

    @Column(name = "cron_expression", length = 120)
    private String cronExpression;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private JobStatus status = JobStatus.DRAFT;

    @Lob
    @Column(name = "runtime_config_json", nullable = false, columnDefinition = "LONGTEXT")
    private String runtimeConfigJson = "{}";

    @Column(name = "last_triggered_at")
    private Instant lastTriggeredAt;

    @Transient
    private String schedulerState;

    @Transient
    private Instant nextFireAt;

    @Transient
    private Instant previousFireAt;
}
