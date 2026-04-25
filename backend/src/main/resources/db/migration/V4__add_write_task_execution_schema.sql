CREATE TABLE write_task_execution (
    id BIGINT NOT NULL AUTO_INCREMENT,
    write_task_id BIGINT NOT NULL,
    trigger_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NULL,
    generated_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    error_summary VARCHAR(1000) NULL,
    delivery_details_json LONGTEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_write_task_execution_task FOREIGN KEY (write_task_id) REFERENCES write_task (id)
);

CREATE TABLE write_task_execution_log (
    id BIGINT NOT NULL AUTO_INCREMENT,
    write_task_execution_id BIGINT NOT NULL,
    log_level VARCHAR(10) NOT NULL,
    message VARCHAR(1000) NOT NULL,
    detail_json LONGTEXT NULL,
    logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_write_task_execution_log_execution FOREIGN KEY (write_task_execution_id) REFERENCES write_task_execution (id)
);

CREATE INDEX idx_write_task_execution_task_id ON write_task_execution (write_task_id);
CREATE INDEX idx_write_task_execution_status ON write_task_execution (status);
CREATE INDEX idx_write_task_execution_log_execution_id ON write_task_execution_log (write_task_execution_id);
