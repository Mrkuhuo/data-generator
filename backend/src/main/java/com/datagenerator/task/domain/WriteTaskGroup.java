package com.datagenerator.task.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "write_task_group")
public class WriteTaskGroup extends BaseEntity {

    @Column(nullable = false, length = 120)
    private String name;

    @Column(name = "connection_id", nullable = false)
    private Long connectionId;

    @Column(length = 1000)
    private String description;

    @Column
    private Long seed;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private WriteTaskStatus status = WriteTaskStatus.DRAFT;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false, length = 20)
    private WriteTaskScheduleType scheduleType = WriteTaskScheduleType.MANUAL;

    @Column(name = "cron_expression", length = 120)
    private String cronExpression;

    @Column(name = "trigger_at")
    private Instant triggerAt;

    @Column(name = "interval_seconds")
    private Integer intervalSeconds;

    @Column(name = "max_runs")
    private Integer maxRuns;

    @Column(name = "max_rows_total")
    private Long maxRowsTotal;

    @Column(name = "last_triggered_at")
    private Instant lastTriggeredAt;

    @Transient
    private String schedulerState;

    @Transient
    private Instant nextFireAt;

    @Transient
    private Instant previousFireAt;
}
