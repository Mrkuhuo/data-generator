package com.datagenerator.task.domain;

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
@Table(name = "write_task_group_execution")
public class WriteTaskGroupExecution extends BaseEntity {

    @Column(name = "write_task_group_id", nullable = false)
    private Long writeTaskGroupId;

    @Enumerated(EnumType.STRING)
    @Column(name = "trigger_type", nullable = false, length = 20)
    private WriteExecutionTriggerType triggerType = WriteExecutionTriggerType.MANUAL;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private WriteExecutionStatus status = WriteExecutionStatus.RUNNING;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "planned_table_count", nullable = false)
    private Integer plannedTableCount = 0;

    @Column(name = "completed_table_count", nullable = false)
    private Integer completedTableCount = 0;

    @Column(name = "success_table_count", nullable = false)
    private Integer successTableCount = 0;

    @Column(name = "failure_table_count", nullable = false)
    private Integer failureTableCount = 0;

    @Column(name = "inserted_row_count", nullable = false)
    private Long insertedRowCount = 0L;

    @Column(name = "error_summary", length = 1000)
    private String errorSummary;

    @Lob
    @Column(name = "summary_json", columnDefinition = "LONGTEXT")
    private String summaryJson;
}
