ALTER TABLE write_task_group
    ADD COLUMN schedule_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    ADD COLUMN cron_expression VARCHAR(120) NULL,
    ADD COLUMN trigger_at DATETIME NULL,
    ADD COLUMN interval_seconds INT NULL,
    ADD COLUMN max_runs INT NULL,
    ADD COLUMN max_rows_total BIGINT NULL,
    ADD COLUMN last_triggered_at DATETIME NULL;

ALTER TABLE write_task_group_execution
    ADD COLUMN trigger_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    ADD COLUMN inserted_row_count BIGINT NOT NULL DEFAULT 0;

CREATE INDEX idx_write_task_group_schedule_type ON write_task_group(schedule_type);
CREATE INDEX idx_write_task_group_execution_trigger_type ON write_task_group_execution(trigger_type);
