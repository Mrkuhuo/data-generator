package com.datagenerator.task.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "write_task_group_table_execution")
public class WriteTaskGroupTableExecution extends BaseEntity {

    @Column(name = "write_task_group_execution_id", nullable = false)
    private Long writeTaskGroupExecutionId;

    @Column(name = "write_task_id", nullable = false)
    private Long writeTaskId;

    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private WriteExecutionStatus status = WriteExecutionStatus.RUNNING;

    @Column(name = "before_write_row_count")
    private Long beforeWriteRowCount;

    @Column(name = "after_write_row_count")
    private Long afterWriteRowCount;

    @Column(name = "inserted_count", nullable = false)
    private Long insertedCount = 0L;

    @Column(name = "null_violation_count", nullable = false)
    private Long nullViolationCount = 0L;

    @Column(name = "blank_string_count", nullable = false)
    private Long blankStringCount = 0L;

    @Column(name = "fk_miss_count", nullable = false)
    private Long fkMissCount = 0L;

    @Column(name = "pk_duplicate_count", nullable = false)
    private Long pkDuplicateCount = 0L;

    @Lob
    @Column(name = "summary_json", columnDefinition = "LONGTEXT")
    private String summaryJson;

    @Column(name = "error_summary", length = 1000)
    private String errorSummary;
}
