ALTER TABLE write_task
    ADD COLUMN trigger_at TIMESTAMP NULL AFTER cron_expression,
    ADD COLUMN interval_seconds INT NULL AFTER trigger_at,
    ADD COLUMN max_runs INT NULL AFTER interval_seconds,
    ADD COLUMN max_rows_total BIGINT NULL AFTER max_runs;

CREATE INDEX idx_write_task_schedule_type ON write_task (schedule_type);
