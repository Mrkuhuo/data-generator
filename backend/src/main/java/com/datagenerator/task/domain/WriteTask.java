package com.datagenerator.task.domain;

import com.datagenerator.common.model.BaseEntity;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Lob;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "write_task")
public class WriteTask extends BaseEntity {

    @Column(nullable = false, length = 120)
    private String name;

    @Column(name = "connection_id", nullable = false)
    private Long connectionId;

    @Column(name = "group_id")
    private Long groupId;

    @Column(name = "task_key", length = 120)
    private String taskKey;

    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    @Enumerated(EnumType.STRING)
    @Column(name = "table_mode", nullable = false, length = 30)
    private TableMode tableMode = TableMode.USE_EXISTING;

    @Enumerated(EnumType.STRING)
    @Column(name = "write_mode", nullable = false, length = 30)
    private WriteMode writeMode = WriteMode.APPEND;

    @Column(name = "row_count", nullable = false)
    private Integer rowCount = 100;

    @Column(name = "batch_size", nullable = false)
    private Integer batchSize = 500;

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

    @Column(length = 1000)
    private String description;

    @Lob
    @Column(name = "target_config_json", columnDefinition = "LONGTEXT")
    private String targetConfigJson;

    @Lob
    @Column(name = "payload_schema_json", columnDefinition = "LONGTEXT")
    private String payloadSchemaJson;

    @Lob
    @Column(name = "row_plan_json", columnDefinition = "LONGTEXT")
    private String rowPlanJson;

    @Column(name = "last_triggered_at")
    private Instant lastTriggeredAt;

    @Transient
    private String schedulerState;

    @Transient
    private Instant nextFireAt;

    @Transient
    private Instant previousFireAt;

    @OneToMany(mappedBy = "task", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @OrderBy("sortOrder ASC, id ASC")
    private List<WriteTaskColumn> columns = new ArrayList<>();

    public void replaceColumns(List<WriteTaskColumn> newColumns) {
        columns.clear();
        if (newColumns == null) {
            return;
        }
        for (WriteTaskColumn column : newColumns) {
            column.setTask(this);
            columns.add(column);
        }
    }
}
